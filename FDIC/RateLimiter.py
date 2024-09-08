from functools import wraps
from datetime import datetime as dt
import time
import sys

class RateLimiter:
    def __init__(self, max_calls, period_in_seconds, logger=None):
        self.max_calls = max_calls
        self.period = period_in_seconds
        self.calls = []
        self.logger = logger

    @property
    def start_time(self):
        if self.calls:
            return self.calls[0]
        return None

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_time = time.time()
            self.calls = [call for call in self.calls if call > current_time - self.period]
            
            # Extract files_downloaded for internal use
            log_msg = kwargs.pop('log_msg', None)

            if len(self.calls) >= self.max_calls:
                wait_time = self.calls[0] - (current_time - self.period)
                              
                if wait_time > 300:
                    print(f'Rate limit: Paused at {dt.now().strftime("%Y.%m.%d %H:%M:%S")} for {wait_time / 60:.1f} minutes. Resume at {time.strftime("%H:%M:%S",time.localtime(current_time + wait_time))}.')
                    
                    #break_process = input('Stop loader? (y/N): ')
                    #@if break_process.lower() == 'y':
                    #    quit()
                else:
                    print(f'Rate limit: Paused for {wait_time:.0f} seconds')                    
                
                try:
                    time.sleep(wait_time +1)
                except:
                    print('User interrupted application.  Quitting.')   
                    if log_msg and self.logger:
                        self.logger.log_instantly(log_msg)        
                    sys.exit(1)
                current_time = time.time()
                self.calls = [call for call in self.calls if call > current_time - self.period]

            self.calls.append(current_time)
            return func(*args, **kwargs)
        
        return wrapper
    

    def wait(self, period_in_seconds=None):
        
        current_time = time.time()
        wait_time = (period_in_seconds or self.period)
        #print(f'Pausing for {wait_time / 60:.1f} minutes. Resume at {time.strftime("%Y.%m.%d %H:%M:%S",time.localtime(self.calls[0] + 3600))}.')
        print(f'Service paused at {dt.now().strftime("%Y.%m.%d %H:%M:%S")} for {wait_time/60:.1f}min . Service to resume at {dt.fromtimestamp(current_time + wait_time).strftime("%Y.%m.%d %H:%M:%S")}.')

        try:
            time.sleep(wait_time+1)
        except KeyboardInterrupt:
            print('User interrupted application.  Quitting.')
            #LOG HERE?
            sys.exit(1)            