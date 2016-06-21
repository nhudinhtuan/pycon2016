import random
import struct
import hashlib
import hmac
from Crypto.Cipher import AES
import platform

if platform.system().lower() == 'darwin':
	from cpplib_osx import xtea_encrypt, xtea_decrypt, garena_xtea_encrypt, garena_xtea_decrypt
else:
	from cpplib import xtea_encrypt, xtea_decrypt, garena_xtea_encrypt, garena_xtea_decrypt

DWORD_BYTES = 4
QDWORD_BYTES = 8
UINT64_MASK = 0xffffffffffffffff

def random_byte():
	return chr(random.randint(0, 255))

def random_bytes(length):
	return ''.join([chr(random.randint(0, 255)) for _ in xrange(length)])

RANDOM_CHARACTER_SET = '1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

def random_string(length, allowed_chars=RANDOM_CHARACTER_SET):
	max_index = len(allowed_chars) - 1
	return ''.join([allowed_chars[random.randint(0, max_index)] for _ in xrange(length)])

def md5(plain_text):
	return hashlib.md5(plain_text).hexdigest()

def sha256(data):
	return hashlib.sha256(data).hexdigest()

def hmac_sha256(plain_text, key):
	return hmac.new(key, plain_text, hashlib.sha256).hexdigest()

"""
Reference:
https://en.wikipedia.org/wiki/Skipjack_(cipher)
https://github.com/boivie/skip32-java
"""
_SKIP32_FTABLE = [
	0xa3, 0xd7, 0x09, 0x83, 0xf8, 0x48,
	0xf6, 0xf4, 0xb3, 0x21, 0x15, 0x78, 0x99, 0xb1, 0xaf, 0xf9, 0xe7,
	0x2d, 0x4d, 0x8a, 0xce, 0x4c, 0xca, 0x2e, 0x52, 0x95, 0xd9, 0x1e,
	0x4e, 0x38, 0x44, 0x28, 0x0a, 0xdf, 0x02, 0xa0, 0x17, 0xf1, 0x60,
	0x68, 0x12, 0xb7, 0x7a, 0xc3, 0xe9, 0xfa, 0x3d, 0x53, 0x96, 0x84,
	0x6b, 0xba, 0xf2, 0x63, 0x9a, 0x19, 0x7c, 0xae, 0xe5, 0xf5, 0xf7,
	0x16, 0x6a, 0xa2, 0x39, 0xb6, 0x7b, 0x0f, 0xc1, 0x93, 0x81, 0x1b,
	0xee, 0xb4, 0x1a, 0xea, 0xd0, 0x91, 0x2f, 0xb8, 0x55, 0xb9, 0xda,
	0x85, 0x3f, 0x41, 0xbf, 0xe0, 0x5a, 0x58, 0x80, 0x5f, 0x66, 0x0b,
	0xd8, 0x90, 0x35, 0xd5, 0xc0, 0xa7, 0x33, 0x06, 0x65, 0x69, 0x45,
	0x00, 0x94, 0x56, 0x6d, 0x98, 0x9b, 0x76, 0x97, 0xfc, 0xb2, 0xc2,
	0xb0, 0xfe, 0xdb, 0x20, 0xe1, 0xeb, 0xd6, 0xe4, 0xdd, 0x47, 0x4a,
	0x1d, 0x42, 0xed, 0x9e, 0x6e, 0x49, 0x3c, 0xcd, 0x43, 0x27, 0xd2,
	0x07, 0xd4, 0xde, 0xc7, 0x67, 0x18, 0x89, 0xcb, 0x30, 0x1f, 0x8d,
	0xc6, 0x8f, 0xaa, 0xc8, 0x74, 0xdc, 0xc9, 0x5d, 0x5c, 0x31, 0xa4,
	0x70, 0x88, 0x61, 0x2c, 0x9f, 0x0d, 0x2b, 0x87, 0x50, 0x82, 0x54,
	0x64, 0x26, 0x7d, 0x03, 0x40, 0x34, 0x4b, 0x1c, 0x73, 0xd1, 0xc4,
	0xfd, 0x3b, 0xcc, 0xfb, 0x7f, 0xab, 0xe6, 0x3e, 0x5b, 0xa5, 0xad,
	0x04, 0x23, 0x9c, 0x14, 0x51, 0x22, 0xf0, 0x29, 0x79, 0x71, 0x7e,
	0xff, 0x8c, 0x0e, 0xe2, 0x0c, 0xef, 0xbc, 0x72, 0x75, 0x6f, 0x37,
	0xa1, 0xec, 0xd3, 0x8e, 0x62, 0x8b, 0x86, 0x10, 0xe8, 0x08, 0x77,
	0x11, 0xbe, 0x92, 0x4f, 0x24, 0xc5, 0x32, 0x36, 0x9d, 0xcf, 0xf3,
	0xa6, 0xbb, 0xac, 0x5e, 0x6c, 0xa9, 0x13, 0x57, 0x25, 0xb5, 0xe3,
	0xbd, 0xa8, 0x3a, 0x01, 0x05, 0x59, 0x2a, 0x46
]

def _skip32_g(key, k, w):
	g1 = w >> 8
	g2 = w & 0xff
	g3 = _SKIP32_FTABLE[g2 ^ (ord(key[(4 * k) % 10]) & 0xFF)] ^ g1
	g4 = _SKIP32_FTABLE[g3 ^ (ord(key[(4 * k + 1) % 10]) & 0xFF)] ^ g2
	g5 = _SKIP32_FTABLE[g4 ^ (ord(key[(4 * k + 2) % 10]) & 0xFF)] ^ g3
	g6 = _SKIP32_FTABLE[g5 ^ (ord(key[(4 * k + 3) % 10]) & 0xFF)] ^ g4

	return (g5 << 8) + g6

def skip32(key, value, encrypt):
	buf = [0]*4
	buf[0] = ((value >> 24) & 0xff)
	buf[1] = ((value >> 16) & 0xff)
	buf[2] = ((value >> 8) & 0xff)
	buf[3] = ((value >> 0) & 0xff)

	if encrypt:
		kstep = 1
		k = 0
	else:
		kstep = -1
		k = 23

	wl = (buf[0] << 8) + buf[1]
	wr = (buf[2] << 8) + buf[3]

	for i in range(12):
		wr ^= _skip32_g(key, k, wl) ^ k
		k += kstep
		wl ^= _skip32_g(key, k, wr) ^ k
		k += kstep

	buf[0] = (wr >> 8)
	buf[1] = (wr & 0xFF)
	buf[2] = (wl >> 8)
	buf[3] = (wl & 0xFF)

	return ((buf[0]) << 24) | ((buf[1]) << 16) | ((buf[2]) << 8) | (buf[3])

def skip32_encrypt(value, key):
	return skip32(key, value, True)

def skip32_decrypt(value, key):
	return skip32(key, value, False)

def xtea_encrypt_native(block, key, n=32):
	MASK = 0xffffffffL
	DELTA = 0x9e3779b9L
	v0 = block & MASK
	v1 = (block>>32) & MASK
	s = 0
	for _ in xrange(n):
		v0 = (v0 + ((((v1<<4) ^ (v1>>5)) + v1) ^ (s + key[s & 3]))) & MASK
		s = (s + DELTA) & MASK
		v1 = (v1 + ((((v0<<4) ^ (v0>>5)) + v0) ^ (s + key[(s>>11) & 3]))) & MASK
	return (v1<<32) | v0

def xtea_decrypt_native(block, key, n=32):
	MASK = 0xffffffffL
	DELTA = 0x9e3779b9L
	v0 = block & MASK
	v1 = (block>>32) & MASK
	s = (DELTA * n) & MASK
	for _ in xrange(n):
		v1 = (v1 - ((((v0<<4) ^ (v0>>5)) + v0) ^ (s + key[(s>>11) & 3]))) & MASK
		s = (s - DELTA) & MASK
		v0 = (v0 - ((((v1<<4) ^ (v1>>5)) + v1) ^ (s + key[s & 3]))) & MASK
	return (v1<<32) | v0

XTEA_BLOCK_SIZE = 8

def xtea_cbc_encrypt(data, key, endian='<'):
	block_fmt = endian + 'Q'
	size = len(data)
	if size % XTEA_BLOCK_SIZE != 0:
		return None
	cipher = []
	block_cipher = 0
	for i in xrange(0, size, XTEA_BLOCK_SIZE):
		block = struct.unpack_from(block_fmt, data, i)[0]
		block_cipher = xtea_encrypt_native(block_cipher ^ block, key)
		cipher.append(struct.pack(block_fmt, block_cipher))
	return ''.join(cipher)

def xtea_cbc_decrypt(data, key, endian='<'):
	block_fmt = endian + 'Q'
	size = len(data)
	if size % XTEA_BLOCK_SIZE != 0:
		return None
	plain = []
	last_block = 0
	for i in xrange(0, size, XTEA_BLOCK_SIZE):
		block = struct.unpack_from(block_fmt, data, i)[0]
		plain_block = xtea_decrypt_native(block, key) ^ last_block
		plain.append(struct.pack(block_fmt, plain_block))
		last_block = block
	return ''.join(plain)

AES_BLOCK_SIZE = 16
AES_CBC_IV = '\0' * AES_BLOCK_SIZE

def aes_cbc_encrypt(data, key):
	return AES.new(key, AES.MODE_CBC, AES_CBC_IV).encrypt(data)

def aes_cbc_decrypt(data, key):
	try:
		return AES.new(key, AES.MODE_CBC, AES_CBC_IV).decrypt(data)
	except:
		return None

def pkcs7_padding(data, block_size):
	pad = block_size - (len(data) % block_size)
	data += chr(pad) * pad
	return data

def pkcs7_unpadding(data, block_size):
	size = len(data)
	if size < block_size or size % block_size != 0:
		return None
	pad = ord(data[-1])
	if pad <= 0 or pad > block_size:
		return None
	for i in xrange(2, pad+1):
		if ord(data[-i]) != pad:
			return None
	return data[:-pad]

GARENA_PADDING_MIN_BLOCK = 3

def garena_xtea_padding(data):
	data = random_bytes(XTEA_BLOCK_SIZE) + data
	data = pkcs7_padding(data, XTEA_BLOCK_SIZE)
	check_sum = 0
	for i in xrange(0, len(data), XTEA_BLOCK_SIZE):
		qword = struct.unpack_from('<Q', data, i)[0]
		check_sum += qword
	data += struct.pack('<Q', check_sum & UINT64_MASK)
	return data

def garena_xtea_unpadding(data):
	data_size = len(data)
	if data_size % XTEA_BLOCK_SIZE != 0:
		return None
	if data_size < XTEA_BLOCK_SIZE * GARENA_PADDING_MIN_BLOCK:
		return None
	check_sum = struct.unpack_from('<Q', data, len(data) - XTEA_BLOCK_SIZE)[0]
	for i in xrange(0, len(data) - XTEA_BLOCK_SIZE, XTEA_BLOCK_SIZE):
		qword = struct.unpack_from('<Q', data, i)[0]
		check_sum -= qword
	if (check_sum & UINT64_MASK) != 0:
		return None
	return pkcs7_unpadding(data[XTEA_BLOCK_SIZE:-XTEA_BLOCK_SIZE], XTEA_BLOCK_SIZE)

def garena_xtea_encrypt_native(data, key):
	padding_data = garena_xtea_padding(data)
	return xtea_cbc_encrypt(padding_data, key)

def garena_xtea_decrypt_native(data, key):
	data_size = len(data)
	if data_size % XTEA_BLOCK_SIZE != 0:
		return None
	if data_size < XTEA_BLOCK_SIZE * GARENA_PADDING_MIN_BLOCK:
		return None
	padding_data = xtea_cbc_decrypt(data, key)
	return garena_xtea_unpadding(padding_data)

def garena_cbc_padding(data, block_size):
	data = random_bytes(block_size) + data
	data = pkcs7_padding(data, block_size)
	check_sum = [0] * block_size
	i = 0
	for ch in data:
		check_sum[i] ^= ord(ch)
		i += 1
		if i >= block_size:
			i = 0
	data = ''.join([data] + [chr(v) for v in check_sum])
	return data

def garena_cbc_unpadding(data, block_size):
	data_size = len(data)
	if data_size % block_size != 0:
		return None
	if data_size < block_size * GARENA_PADDING_MIN_BLOCK:
		return None
	check_sum = [0] * block_size
	i = 0
	for ch in data:
		check_sum[i] ^= ord(ch)
		i += 1
		if i >= block_size:
			i = 0
	for v in check_sum:
		if v != 0:
			return None
	return pkcs7_unpadding(data[block_size:-block_size], block_size)

def garena_aes_encrypt(data, key):
	padding_data = garena_cbc_padding(data, AES_BLOCK_SIZE)
	return aes_cbc_encrypt(padding_data, key)

def garena_aes_decrypt(data, key):
	data_size = len(data)
	if data_size % AES_BLOCK_SIZE != 0:
		return None
	if data_size < AES_BLOCK_SIZE * GARENA_PADDING_MIN_BLOCK:
		return None
	padding_data = aes_cbc_decrypt(data, key)
	return garena_cbc_unpadding(padding_data, AES_BLOCK_SIZE)

def create_django_secret_key():
	return random_string(50, 'abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)')
