#!/usr/bin/env python
import socket
import threading
import json
import rospy
from time import sleep
from std_msgs.msg import String
from waterlinked_a50_ros_driver.msg import DVL
from waterlinked_a50_ros_driver.msg import DVLBeam
from waterlinked_a50_ros_driver.srv import ResetDeadReckoning, CalibrateGyro, GetConfig, SetConfig
from waterlinked_a50_ros_driver.srv import ResetDeadReckoningResponse, CalibrateGyroResponse, GetConfigResponse, SetConfigResponse
import select
from dataclasses import dataclass


@dataclass
class Config:
	success: bool
	speed_of_sound: float
	acoustic_enabled: bool
	dark_mode_enabled: bool
	mounting_rotation_offset: float
	range_mode: str


class Handler:

	def __init__(self):
		self.TCP_IP = rospy.get_param("~ip", "192.168.194.95")
		self.TCP_PORT = rospy.get_param("~port", 16171)
		self.do_log_raw_data = rospy.get_param("~do_log_raw_data", False)
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect()

		self.oldJson = ""

		self.rate = rospy.Rate(10)  # 10hz

		self.pub_raw = rospy.Publisher('dvl/json_data', String, queue_size=10)
		self.pub = rospy.Publisher('dvl/data', DVL, queue_size=10)

		self.srv_rdr = rospy.Service('dvl/reset_dead_reckoning', ResetDeadReckoning, self.handle_rdr)
		self.srv_cg = rospy.Service('dvl/calibrate_gyro', CalibrateGyro, self.handle_cg)
		self.srv_gc = rospy.Service('dvl/get_config', GetConfig, self.handle_gc)
		self.srv_sc = rospy.Service('dvl/set_config', SetConfig, self.handle_sc)

		self.cv_rdr = threading.Condition()
		self.waiting_rdr = False
		self.resp_rdr = None
		self.cv_cg = threading.Condition()
		self.waiting_cg = False
		self.resp_cg = None
		self.cv_gc = threading.Condition()
		self.waiting_gc = False
		self.resp_gc = None
		self.cv_sc = threading.Condition()
		self.waiting_sc = False
		self.resp_sc = None

	def connect(self):
		try:
			self.socket.connect((self.TCP_IP, self.TCP_PORT))
			self.socket.settimeout(1)
		except socket.error as err:
			rospy.logerr("No route to host, DVL might be booting? {}".format(err))
			sleep(1)
			self.connect()

	def get_data(self):
		raw_data = ""

		while not '\n' in raw_data:
			try:
				rec = self.socket.recv(1)  # Add timeout for that
				if len(rec) == 0:
					rospy.logerr("Socket closed by the DVL, reopening")
					self.connect()
					continue
			except socket.timeout as err:
				rospy.logerr("Lost connection with the DVL, reinitiating the connection: {}".format(err))
				self.connect()
				continue
			raw_data = raw_data + rec.decode('utf-8')
		raw_data = self.oldJson + raw_data
		self.oldJson = ""
		raw_data = raw_data.split('\n')
		self.oldJson = raw_data[1]
		raw_data = raw_data[0]
		return raw_data

	def run(self):
		while not rospy.is_shutdown():
			raw_data = self.get_data()
			data = json.loads(raw_data)

			# edit: the logic in the original version can't actually publish the raw data
			# we slightly change the if else statement so now
			# do_log_raw_data is true: publish the raw data to /dvl/json_data topic, fill in theDVL using velocity data and publish to dvl/data topic
			# do_log_raw_data is true: only fill in theDVL using velocity data and publish to dvl/data topic

			if self.do_log_raw_data:
				rospy.loginfo(raw_data)

			if data["type"] == "velocity":
				self.pub_raw.publish(raw_data)
				self.handle_velocity(data)
			elif data["type"] == "response":
				self.handle_responses(data)

			self.rate.sleep()

	def handle_velocity(self, data):
		the_dvl = DVL()
		beam0 = DVLBeam()
		beam1 = DVLBeam()
		beam2 = DVLBeam()
		beam3 = DVLBeam()
		the_dvl.header.stamp = rospy.Time.now()
		the_dvl.header.frame_id = "dvl_link"
		the_dvl.time = data["time"]
		the_dvl.velocity.x = data["vx"]
		the_dvl.velocity.y = data["vy"]
		the_dvl.velocity.z = data["vz"]
		the_dvl.fom = data["fom"]
		the_dvl.altitude = data["altitude"]
		the_dvl.velocity_valid = data["velocity_valid"]
		the_dvl.status = data["status"]
		the_dvl.form = data["format"]

		beam0.id = data["transducers"][0]["id"]
		beam0.velocity = data["transducers"][0]["velocity"]
		beam0.distance = data["transducers"][0]["distance"]
		beam0.rssi = data["transducers"][0]["rssi"]
		beam0.nsd = data["transducers"][0]["nsd"]
		beam0.valid = data["transducers"][0]["beam_valid"]

		beam1.id = data["transducers"][1]["id"]
		beam1.velocity = data["transducers"][1]["velocity"]
		beam1.distance = data["transducers"][1]["distance"]
		beam1.rssi = data["transducers"][1]["rssi"]
		beam1.nsd = data["transducers"][1]["nsd"]
		beam1.valid = data["transducers"][1]["beam_valid"]

		beam2.id = data["transducers"][2]["id"]
		beam2.velocity = data["transducers"][2]["velocity"]
		beam2.distance = data["transducers"][2]["distance"]
		beam2.rssi = data["transducers"][2]["rssi"]
		beam2.nsd = data["transducers"][2]["nsd"]
		beam2.valid = data["transducers"][2]["beam_valid"]

		beam3.id = data["transducers"][3]["id"]
		beam3.velocity = data["transducers"][3]["velocity"]
		beam3.distance = data["transducers"][3]["distance"]
		beam3.rssi = data["transducers"][3]["rssi"]
		beam3.nsd = data["transducers"][3]["nsd"]
		beam3.valid = data["transducers"][3]["beam_valid"]

		the_dvl.beams = [beam0, beam1, beam2, beam3]

		self.pub.publish(the_dvl)

	def handle_responses(self, data):
		response_type = data["response_to"]
		if response_type == "reset_dead_reckoning":
			with self.cv_rdr:
				self.cv_rdr.notifyAll()
		elif response_type == "calibrate_gyro":
			with self.cv_cg:
				self.cv_cg.notifyAll()
		elif response_type == "get_config":
			with self.cv_gc:
				self.cv_gc.notifyAll()
		elif response_type == "set_config":
			with self.cv_sc:
				self.cv_sc.notifyAll()
		else:
			raise ValueError(f'Unknown response type {response_type}')

	def handle_rdr(self, req):
		# Wait for response
		resp = ResetDeadReckoningResponse()
		if self.waiting_rdr:
			resp.success = False
			return resp
		else:
			self.waiting_rdr = True
		with self.cv_rdr:
			resp.success = True
			return resp

	def handle_cg(self, req):
		# Wait for response
		resp = CalibrateGyroResponse()
		resp.success = True
		return resp

	def handle_gc(self, req):
		# Wait for response
		resp = GetConfigResponse()
		resp.success = True
		return resp

	def handle_sc(self, req):
		# Wait for response
		resp = SetConfigResponse()
		resp.success = True
		return resp


if __name__ == '__main__':
	rospy.init_node('a50_pub', anonymous=False)
	handler = Handler()
	handler.run()
