from io import TextIOWrapper
import os as os
from datetime import datetime as dt

class Chainsaw:
    
    __fpath  : str 
    __format : str
    __module : str = ''
    
    __logfile : TextIOWrapper
    
    __is_open : bool = False
    
    __default_success : str = ''
    __default_failure : str = ''
    
    ____default_format = "{date} :: {time} :: {success} :: {module} :: {message}"
    
    def __init__(self, fdir:str='./logs/', fname:str='log.txt', format:str=____default_format,
                 success_msg:str='Task executed Successfully',
                 failure_msg:str='Task Execution Failed', module:str=''):
        
        self.__format = format
        
        self.__module = module
        
        self.__default_failure = failure_msg
        self.__default_success = success_msg
        
        dir_sep = '/'
        if fdir[-1] == '/' or fdir[-1] == '\\':
            dir_sep = ''
        elif '\\' in fdir:
            dir_sep = '\\'
        elif '/' in fdir:
            dir_sep = '/'
                            
        self.__fpath = fdir + dir_sep + fname

    def __bool__(self) -> bool:
        return self.is_open()
    
    def is_open(self) -> bool:
        return self.__is_open
        
    def open(self):
        if not self.is_open():
            self.__is_open = True
            self.__logfile = open(self.__fpath, 'a')
            self.write(success=True,message=f"Opened {self.__fpath} Successfully", success_str='PROC', module='pylogger')
        else:
            print(f"{self.__fpath} already open!")
        
    def write(self, 
              success:bool=False,
              message:str="",
              module:str=__module,
              success_str:str=""
              ):
        
        if not self.is_open():
            raise RuntimeError(f"LogFile {self.__fpath} not open!!")
        
        if success:
            if not success_str:
                success_str = 'PASS'
            if not message:
                message = self.__default_success 
        elif not success:
            if not success_str:
                success_str = 'FAILED' 
            if not message:
                message = self.__default_failure
            
        if not module:
            module = self.__module

        now = dt.now()
        try:
            self.__logfile.write(self.__format.format(date=now.strftime("%Y-%m-%d"), time=now.strftime("%H:%M"),
                                                      module=module, success=success_str, message=message))
            self.__logfile.write('\n')
        except Exception as e:
            print(e)
        
    def close(self):
        if not self.is_open():
            raise RuntimeError(f"LogFile {self.__fpath} not open!!")
        try:
            self.write(message=f'Closing {self.__fpath}...', success_str='PROC', module='pylogger')
            self.__logfile.close()
            self.__is_open = False
        except (Exception) as e:
            print(e)
            
    def log(self, success:bool=False):
        self.write(success)
        
    def get_filepath(self) -> str:
        return self.__fpath
    
    def update(self, success:str=__default_success, failure:str=__default_failure, module:str=__module):
        self.__default_success = success
        self.__default_failure = failure
        self.__module = module
        
    def __str__(self):
        open_stat = 'OPEN' if self.is_open() else 'CLOSED'
        return f"Logging {self.get_filepath()}\nStatus: {open_stat}"