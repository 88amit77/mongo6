from datetime import datetime, timedelta
from functools import wraps
import time
class throttle(object):
	"""
	Decorator that prevents a function from being called more than once every
	time period.


	"""
	def __init__(self, seconds=0, minutes=0, hours=0):
		self.throttle_period = timedelta(
			seconds=seconds, minutes=minutes, hours=hours
		)
		self.time_of_last_call = datetime.min

	def __call__(self, fn):
		@wraps(fn)
		def wrapper(*args, **kwargs):
			now = datetime.now()
			time_since_last_call = now - self.time_of_last_call

			if time_since_last_call > self.throttle_period:
				self.time_of_last_call = now
				print([time_since_last_call.total_seconds(),'***'])
				return fn(*args, **kwargs)
			else:
				print([time_since_last_call.total_seconds(),'---'])
				func_should_wait = abs(1-time_since_last_call.total_seconds())
				time.sleep(func_should_wait)
				###print("***wrapper start sleep***")
				self.time_of_last_call = now
				return fn(*args, **kwargs)
		return wrapper
