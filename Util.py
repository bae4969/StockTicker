from datetime import datetime as DateTime
import inspect


is_print_debug_log = True

def PrintNormalLog(log:str) -> None:
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

def PrintErrorLog(log:str) -> None:
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
	
def TogglePrintLog() -> None:
	is_print_debug_log = not is_print_debug_log


def GetCurrnetMarketOpened() -> str:
	kr_market_from = DateTime.now().replace(hour=8, minute=0, second=0)
	kr_market_to = DateTime.now().replace(hour=15, minute=30, second=0)
	
	if (kr_market_from < DateTime.now() < kr_market_to):
		return "KR"
	else:
		return "EX"

