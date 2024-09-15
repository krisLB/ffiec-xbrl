from FDIC import ExEngine
from FDIC import Viewer
from FDIC.ETL import ETL
from FDIC import RateLimiter, Logger
from FDIC import constants as paths
#import matplotlib.pyplot as plt


def main():
    #Initialize base objects
    rate_limiter = RateLimiter(max_calls=2500, period_in_seconds=3660) #~2.5k every 60min
    logger = Logger()
    etl = ETL(paths.WSDL_path, rate_limiter, logger)
    
   
    #Build MDRM_Dict - only keep selected items by filtering on mnemonic
    filters = {'Reporting Form':['FFIEC 031', 'FFIEC 041', 'FFIEC 051']}    
    #filters = {'Mnemonic':['CALL',
    #                            'CENB','IADX','IBFQ','RCCD','RCCF','RCEG',
    #                    'RCFD',
    #                            'RCFA', 'RCFW',
    #                    'RCFN',
    #                    'RCON',
    #                        'RCF0','RCF1','RCF2','RCF3','RCF4','RCF5','RCF6','RCF7','RCF8','RCF9','RCOA','RCOW',
    #                    'RCOS','RIAD','RIAS','RIDM','RIFN','SCHJ',
    #                ]}
    #etl.build_MDRMdict(input_path=paths.localPath + paths.folder_MDRMs + paths.filename_MDRM_src, export_path =paths.folder_Orig + paths.filename_MDRM, filters=filters)
    
    # ExEngine.FillMaster('504713')
    
    #Generate Bank Dimension
    #etl.GenBankDim()
        
    #Download Call Reports for Instns in Bank Dimension
    etl.DownloadCallReports(['XBRL'])
    
    #etl.GenBankMaster()
    #etl.GenCallMaster()

    #etl.loadCSV()
    #viewer = Viewer()
    #viewer.query()

if __name__ == "__main__":
    main()
