import platform

if platform.system().lower() == 'darwin':
	from cpplib_osx import ConHash
else:
	from cpplib import ConHash
