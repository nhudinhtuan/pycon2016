from collections import deque

class SimpleQueue():
	
	def __init__(self, max_size=None):
		self._q = deque(maxlen=max_size)
		
	def size(self):
		return len(self._q)
	
	def empty(self):
		return len(self._q) <= 0

	def put(self, item):
		self._q.append(item)

	def get(self):
		return self._q.popleft()
	
	def remove(self, value):
		if value in self._q:
			self._q.remove(value)

