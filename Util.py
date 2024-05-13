from datetime import datetime as DateTime
import inspect
import logging


def PrintNormalLog(log:str) -> None:
	cur_datetime = DateTime.now()
	log_filename = "./log/" + cur_datetime.strftime("%Y%V") + ".log"
	logging.basicConfig(filename=log_filename, level=logging.INFO)
	
	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]

	logging.info(
		cur_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
		+ " |N| "
		+ log
		+ "\t("
		+ inspect.stack()[1][3]
		+ "|"
		+ filename
		+ ":"
		+ str(inspect.stack()[1][2])
		+")"
	)

def PrintErrorLog(log:str) -> None:
	cur_datetime = DateTime.now()
	log_filename = "./log/" + cur_datetime.strftime("%Y%V") + ".log"
	logging.basicConfig(filename=log_filename, level=logging.INFO)

	filepath = inspect.stack()[1][1]
	filename = filepath[filepath.rfind("/") + 1:]
	
	logging.error(
		cur_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
		+ " |E| "
		+ log
		+ "\t("
		+ inspect.stack()[1][3]
		+ "|"
		+ filename
		+ ":"
		+ str(inspect.stack()[1][2])
		+")")
	

