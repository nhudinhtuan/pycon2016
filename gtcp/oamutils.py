def pack_archive(dir_name, filter_re=None, filter_dir_re=None):
	import zipfile
	import os
	import re
	
	def filter_dir(dirpath, filter_dir_re_obj):
		dir_list = dirpath.split(os.path.sep)
		for idx, dirname in enumerate(dir_list):
			if filter_dir_re_obj.match(dirname) is not None:
				if idx == len(dir_list) - 1:
					return False
				else:
					return None
		return True
	
	def filter_file(filename, filter_re_obj):
		return filter_re_obj.match(filename) is None
	
	if filter_re is None:
		filter_re = '.*\.zip$|.*\.pyc$|.*\.log.*|(^\.)(?!gitkeep)'
	filter_re_obj = re.compile(filter_re)

	if filter_dir_re is None:
		filter_dir_re = '\..+'
	filter_dir_re_obj = re.compile(filter_dir_re)
	
	archive_name = dir_name + '.zip'
	print 'Packing', archive_name
	f = zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED)
	for dirpath, dirnames, filenames in os.walk('.'):
		is_filter = filter_dir(dirpath, filter_dir_re_obj)
		if is_filter:
			for filename in filenames: 
				fullpath = os.path.join(dirpath,filename)
				if filter_file(filename, filter_re_obj):
					print 'Include:', fullpath
					f.write(fullpath, os.path.join(dir_name, fullpath))
				else:
					print 'Exclude:', fullpath
		elif is_filter is not None:
			print 'Exclude:', dirpath
	f.close()
	print 'Pack Complete.'
