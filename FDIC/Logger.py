import time
import os
from datetime import datetime
#import constants as paths
import FDIC.constants as paths

class Logger:
    def __init__(self, log_file_path=paths.folder_Log + paths.filename_Log, mode='a'):
        """
        Initialize the Logger class with the log file path.
        """
        self.log_file_path = log_file_path
        self.buffer = []
        self.mode = mode


    def log_instantly(self, msg):
        """
        Log the given data instantly to the log file.
        """
        timestamp = self._get_timestamp()
        log_entry = f"{timestamp}: {msg}"

        self._write_to_file(log_entry)

    def log_to_buffer(self, data):
        """
        Add the given data to the buffer.
        """
        timestamp = self._get_timestamp()
        log_entry = f"{timestamp}: {data}"

        self.buffer.append(log_entry)

    def flush_buffer(self):
        """
        Flush the buffer and write all buffered log entries to the log file.
        """
        for entry in self.buffer:
            self._write_to_file(entry)

        self.buffer = []

    def _get_timestamp(self):
        """
        Get the current timestamp.
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write_to_file(self, log_entry):
        """
        Write the log entry to the log file.
        """
        with open(self.log_file_path, self.mode) as log_file:
            log_file.write(f"{log_entry}\n")