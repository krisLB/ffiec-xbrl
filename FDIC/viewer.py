from FDIC import constants as paths

import pandas as pd

class Viewer:
    def __init__(self, filepath=paths.localPath + paths.filename_MasterCall):
        self.filepath = filepath
    

    def query(self, MDRMitem=None, RSSD=None, ReportPeriodEndDate=None):
        