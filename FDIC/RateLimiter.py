from functools import wraps
import datetime
import time

class RateLimiter:
    def __init__(self, max_calls, period_in_seconds):
        self.max_calls = max_calls
        self.period = period_in_seconds
        self.calls = []

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
            
            if len(self.calls) >= self.max_calls:
                wait_time = self.calls[0] - (current_time - self.period)
                if wait_time > 300:
                    print(f'Rate limit: Paused at {time.strftime("%Y.%m.%d %H:%M:%S",current_time)} for {wait_time / 60:.1f} minutes. Resume at {time.strftime("%H:%M:%S",time.localtime(current_time + wait_time))}.')
                    
                    #break_process = input('Stop loader? (y/N): ')
                    #@if break_process.lower() == 'y':
                    #    exit()
                else:
                    print(f'Rate limit: Paused for {wait_time} seconds')                    
                time.sleep(wait_time +1)
                current_time = time.time()
                self.calls = [call for call in self.calls if call > current_time - self.period]

            self.calls.append(current_time)
            return func(*args, **kwargs)
        
        return wrapper
    

    def wait(self, period_in_seconds=None):
        wait_time = time.time() + (period_in_seconds or self.period)
        print(f'Pausing for {wait_time / 60:.1f} minutes. Resume at {time.strftime("%Y.%m.%d %H:%M:%S",time.localtime(self.calls[0] + 3600))}.')