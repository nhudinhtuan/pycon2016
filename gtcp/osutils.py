import psutil

def kill_process_tree(pid):
	try:
		main_proc = psutil.Process(pid)
	except (psutil.NoSuchProcess, psutil.AccessDenied) as ex:
		return
	processes = main_proc.children(recursive=True)
	processes.append(main_proc)
	for proc in processes:
		proc.terminate()
	psutil.wait_procs(processes)

