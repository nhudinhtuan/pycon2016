import struct

class BufferWriter():
	
	def __init__(self,  endian='<'):
		self._buffer = []
		self._endian = endian
		
	@property
	def buffer(self):
		return ''.join(self._buffer)

	def set_endian(self, endian):
		self._endian = endian

	def clear(self):
		self._buffer = []
		
	def add_int8(self, data):
		self._add_int('b', data)
		
	def add_uint8(self, data):
		self._add_int('B', data)
		
	def add_int16(self, data):
		self._add_int('h', data)
		
	def add_uint16(self, data):
		self._add_int('H', data)
		
	def add_int32(self, data):
		self._add_int('i', data)
		
	def add_uint32(self, data):
		self._add_int('I', data)
		
	def add_int64(self, data):
		self._add_int('q', data)
		
	def add_uint64(self, data):
		self._add_int('Q', data)

	def add_float(self, data):
		self._add_int('f', data)

	def add_double(self, data):
		self._add_int('d', data)

	def add_buffer(self, buf):
		self._buffer.append(buf)

	def add_padding(self, size):
		self._buffer.append('\0' * size)

	def _add_int(self, fmt, data):
		self._buffer.append(struct.pack(self._endian + fmt, data))
