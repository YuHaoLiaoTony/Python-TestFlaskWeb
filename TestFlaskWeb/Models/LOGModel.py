class LOGModel:
    __Key__= 'LOG_NO'
    LOG_NO = 0
    LOG_TYPE = 0
    MSG = ''
    USR_ID = ''
    LOG_TIME = ''
    SYS = ''
    def __init__(self,logNo,logType,msg,usrId,logTime,sys):
        self.LOG_NO = logNo
        self.LOG_TYPE = logType
        self.MSG = msg
        self.USR_ID = usrId
        self.LOG_TIME = logTime
        self.SYS = sys
