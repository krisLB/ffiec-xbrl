from FDIC import ExEngine
from FDIC.ETL import ETL, RateLimiter
from FDIC import constants as paths

def main():
    rate_limiter = RateLimiter(max_calls=2500, period_in_seconds=3660) #~2.5k every 60min
    etl = ETL(paths.WSDL_path, rate_limiter)
    
    # ExEngine.FillMaster('504713')
    #etl.GenBankDim()
    #etl.DownloadCallReports(['XBRL'])
    etl.ParseXBRL()
    #etl.GenBankMaster()
    #etl.GenCallMaster()

if __name__ == "__main__":
    main()
