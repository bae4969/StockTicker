import inspect


# 로그는 stdout 으로만 내보낸다.
#
# 컨테이너의 docker syslog 드라이버가 이 출력을 01.core 의 logsink 로 보내고, logsink 가
# 서비스별·날짜별 파일로 보관한다(웹에서도 조회한다). 시각과 서비스명은 logsink 가 붙이므로
# 여기서 찍지 않는다.
#
# 예전에는 로그 전용 DB(stock_ticker_log)에 적재하면서 stdout 출력을 INSERT 성공 뒤에 두었다.
# 그 구조는 로그 DB 가 아플 때 로그가 통째로 사라져, 정작 장애 상황에서 아무것도 안 남았다.
def InsertLog(name:str, type:str, msg:str) -> None:
    # inspect.stack() 은 전체 스택과 소스 컨텍스트를 만들어 비싸다. 호출 프레임만 직접 본다.
    frame = inspect.currentframe().f_back
    filepath = frame.f_code.co_filename
    filename = filepath[filepath.rfind("/") + 1:]

    # flush 하지 않으면 stdout 이 블록 버퍼링(컨테이너에 TTY 없음)이라 한산할 때 로그가 늦게 나간다.
    print(f"[{name}] |{type}| {msg} ({frame.f_code.co_name}|{filename}:{frame.f_lineno})", flush=True)




def TryGetDictStr(dict, key, default_str="") -> str:
    try:
        return dict[key]
    except:
        return default_str
    

def TryGetDictInt(dict, key, default_value=0) -> int:
    try:
        return int(dict[key])
    except:
        return default_value
    

def TryGetDictFloat(dict, key, default_value=0.0) -> float:
    try:
        return float(dict[key])
    except:
        return default_value
    


def TryParseInt(value, default_value=0) -> int:
    try:
        return int(value)
    except:
        return default_value
    

def TryParseFloat(value, default_value=0.0) -> float:
    try:
        return float(value)
    except:
        return default_value

