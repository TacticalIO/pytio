import paho.mqtt.client as mqtt
import uuid
import json
import threading
import time
import logging

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('data')
logger.setLevel(logging.DEBUG)

def set_interval(func, sec, args = None):
  def func_wrapper():
      set_interval(func, sec)
      if (args == None):
      	func()
      else:
      	func(args)
  t = threading.Timer(sec, func_wrapper)
  t.start()
  return t

def set_timeout(func, sec, args = None):
	t = threading.Timer(sec, func, args)
	t.start()
	return t

class DataSynchroniser(threading.Thread):
	def __init__(self, options):
		threading.Thread.__init__(self)
		self.ip = options["ip"]
		self.port = options["port"]
		self.syncperiod = options["syncperiod"] 
		self.rpctimeout = options["rpctimeout"]
		self.uuid = str(uuid.uuid4())
		self.data = dict()
		self.mqttClient = mqtt.Client(client_id=self.uuid)
		self.mqttClient.on_connect = self.on_connect
		self.mqttClient.on_message = self.on_message
		self.mqttClient.connect(self.ip, self.port, 60)
		self.observers = dict()
		self.services = dict()
		self.syncheap = { "id": self.uuid, "data": {}}
		self.rpcResultProcessing = dict()
		self.resetTimer = dict()
  
		set_interval(self.sync, self.syncperiod)

	def run(self):
		print("Data synchro started")
		self.mqttClient.loop_forever()

	def on_connect(self, client, userdata, flags, rc):
		print(str(client) + " connected with code " + str(rc))
		self.subscribe("datasync")
		self.subscribe("rpc")
		self.subscribe("subscriptions")
		self.publish('subscriptions', { "id": self.uuid })
		print("Subscribed to 3 topics")

	def on_message(self, client, userdata, msg):
		message = json.loads(msg.payload)
 		if msg.topic == "datasync" and message["id"] != self.uuid:
			data = message["data"]
			for k, v in data.iteritems():
				if v.has_key("value"):
					self.internalSet(k, v["value"], v["t"])
		elif msg.topic == "rpc" and message["id"] != self.uuid:
			if message.has_key("service") and self.services.has_key(message["service"]):
				self.rpcResponse(message["token"], 
					self.services[message["service"]](message["args"], message["id"]))
			else:
				if self.rpcResultProcessing.has_key(message["token"]):
					for fct in self.rpcResultProcessing[message["token"]]:
						result = None
						if (message.has_key("result")):
							result = message["result"]
						fct(result)
					del self.rpcResultProcessing[message["token"]]
		elif msg.topic == "subscriptions":
			self.refresh()
			print("Subscription: " + str(message))

	def registerService(self, method, callback):
		self.services[method] = callback

	def unregisterService(self, method):
		if services[method]:
			del services[method]
		else:
			raise Exception("[RPC] Service does not exist and cannot be unregistered: " + method) 

	def observe(self, name, fct):
		if not self.observers.has_key(name):
			self.observers[name] = [ fct ]
		else:
			self.observers[name].push(fct)

	def observers(self, name = None):
		if name:
			return self.observers[name]
		else:
			return self.observers

	def resetObservers(self, name):
		if self.observers.has_key(name):
			del self.observers[name]

	def set(self, name, value, duration = None):
		if self.data.has_key(name):
			old = self.data[name]["value"]
		else: 
			old = None

		self.data[name] = {
			"value": value,
			"t": time.time()
		}

		if duration and self.resetTimer[name] is None:
			def fct(name, v):
				self.set(name, v)
				del self.resetTimer[name]
			self.resetTimer[name] = set_timeout(fct, duration, (name, old))

		if self.observers.has_key(name):
			for observer in self.observers[name]:
				observer(value, old, self.data[name]["t"])

		if self.observers.has_key("_ALL_"):
			# recommended not more than one single global observer
			self.observers["_ALL_"][0](name, value, old, self.data[name]["t"])
    
		self.syncheap["data"][name] = self.data[name]

	# time conservative setter used internally on message reception
	def internalSet(self, name, value, t):
		if self.data.has_key(name):
			old = self.data[name]["value"]
		else: 
			old = None

		self.data[name] = {
			"value": value,
			"t": t or time.time()
		}
		if self.observers.has_key(name):
			for observer in self.observers[name]:
				observer(value, old, self.data[name]["t"])
    
		if self.observers.has_key("_ALL_"):
			# recommended not more than one single global observer
			self.observers["_ALL_"][0](name, value, old, self.data[name]["t"])

	def get(self, name):
		if self.data.has_key(name):
			return self.data[name]["value"];
		return None

	def timestamp(self, name):
		if self.data.has_key(name):
			return self.data[name].t;
		return None

	def subscribe(self, topic):	
		self.mqttClient.subscribe(topic, 1)
		print("Tried subscription for topic <" + topic + ">")

	def unsubscribe(topic):
		self.mqttClient.unsubscribe(topic)

	def publish(self, topic, message):
		self.mqttClient.publish(topic, json.dumps(message))

	def sync(self):
		if self.syncheap["data"]:
			self.mqttClient.publish('datasync', json.dumps(self.syncheap))
			self.syncheap = { "id": self.uuid, "data": {}}

	def forceSync(self, name):
		toSync = { "id": self.uuid, "data": { "name": self.data[name] }}
		self.mqttClient.publish('datasync', json.dumps(toSync))

	def refresh(self):
		if self.data:
			self.mqttClient.publish('datasync', json.dumps({
				"id": self.uuid,
				"data": self.data
			}))

	def rpc(self, remoteService, args = None, cb = None):
		token = str(uuid.uuid4())
		message = { "id": self.uuid, "token": token, "service": remoteService, "args": args }
		self.mqttClient.publish('rpc', json.dumps(message))
		if cb:
			if not self.rpcResultProcessing.has_key(token):
				self.rpcResultProcessing[token] = [ cb ]
			else:
				self.rpcResultProcessing[token].push(cb)
    
		def fct(atoken):
			if self.rpcResultProcessing.has_key(atoken):
				for processing in self.rpcResultProcessing[atoken]:
					processing()
				del self.rpcResultProcessing[atoken]
		set_timeout(fct, self.rpctimeout, [token])
  
	def rpcResponse(self, token, result):
		self.mqttClient.publish('rpc', json.dumps({
			"id": self.uuid,
			"token": token,
			"result": result
		}))

	def data(self):
		return data

if __name__ == '__main__':
	options = {
		"ip": "192.168.0.13",
		"port": 1883,
		"syncperiod": 0.01,
		"rpctimeout": 0.500
	}
	ds = DataSynchroniser(options)