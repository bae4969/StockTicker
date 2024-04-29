from datetime import datetime as DateTime
import inspect


is_print_debug_log = True

def PrintNormalLog(log:str):
	if is_print_debug_log == False: return

	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]
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

def PrintErrorLog(log:str):
	if is_print_debug_log == False: return

	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]
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
	
def IsPrintLog(isPrint:bool):
	is_print_debug_log = isPrint

