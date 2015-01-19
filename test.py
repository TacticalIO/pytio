#!/usr/bin/python
#!/usr/bin/python
import time
import sys
from tio import data

if __name__ == '__main__':
	try:
		options = {
			"ip": "192.168.0.13",
			"port": 1883,
			"syncperiod": 0.01,
			"rpctimeout": 0.500
		}
		ds = data.DataSynchroniser(options)
		ds.start()
		ds.set("speed", 43.33)
		ds.set("acc_ctrl", 1)
		time.sleep(1)
		print("Cab: " + str(ds.get("cab")))
	except:
		ds._Thread__stop()
		sys.exit()