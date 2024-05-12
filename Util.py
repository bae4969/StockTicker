from datetime import datetime as DateTime
import inspect
import threading


is_print_debug_log = True
print_log_lock = threading.Lock()


def PrintNormalLog(log:str) -> None:
	if is_print_debug_log == False: return

	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]

	print_log_lock.acquire()
	print(
		DateTime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
		+ " |N| "
		+ log
		+ "\t("
		+ inspect.stack()[1][3]
		+ "|"
		+ filename
		+ ":"
		+ str(inspect.stack()[1][2])
		+")")
	print_log_lock.release()

def PrintErrorLog(log:str) -> None:
	if is_print_debug_log == False: return

	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]
	
	print_log_lock.acquire()
	print(
		DateTime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
		+ " |E| "
		+ log
		+ "\t("
		+ inspect.stack()[1][3]
		+ "|"
		+ filename
		+ ":"
		+ str(inspect.stack()[1][2])
		+")")
	print_log_lock.release()
	
def TogglePrintLog() -> None:
	is_print_debug_log = not is_print_debug_log


