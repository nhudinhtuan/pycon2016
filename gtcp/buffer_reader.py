import struct

class BufferReader():
	
	def __init__(self,  buf, endian='<'):
		self._buffer = buf
		self._length = len(buf)
		self._endian = endian
		self._offset = 0
		self._error = False

	@property
	def error(self):
		return self._error

	def set_endian(self, endian):
		self._endian = endian

	def get_int8(self):
		return self._get_int('b', 1)
		
	def get_uint8(self):
		return self._get_int('B', 1)
		
	def get_int16(self):
		return self._get_int('h', 2)
		
	def get_uint16(self):
		return self._get_int('H', 2)
		
	def get_int32(self):
		return self._get_int('i', 4)
		
	def get_uint32(self):
		return self._get_int('I', 4)
		
	def get_int64(self):
		return self._get_int('q', 8)
		
	def get_uint64(self):
		return self._get_int('Q', 8)

	def get_float(self):
		return self._get_int('f', 4)

	def get_double(self):
		return self._get_int('d', 8)

	def get_buffer(self, size):
		end_offset = self._offset + size
		if end_offset > self._length:
			self._error = True
		v = self._buffer[self._offset:end_offset]
		self._offset = end_offset
		return v

	def get_remain(self):
		v = self._buffer[self._offset:]
		self._offset += len(v)
		return v

	def skip(self, size):
		self._offset += size
		if self._offset > self._length:
			self._error = True

	def _get_int(self, fmt, size):
		if self._offset + size > self._length:
			self._offset += size
			self._error = True
			return 0
		v = struct.unpack_from(self._endian + fmt, self._buffer, self._offset)[0]
		self._offset += size
		return v