from FDIC import ETL, ExEngine

def main():
    Etl = ETL()
    
    # ExEngine.FillMaster('504713')
    Etl.GenBankDim()
    Etl.DownloadCallReports(['XBRL'])
    Etl.ParseXBRL()
    Etl.GenBankMaster()
    Etl.GenCallMaster()

if __name__ == "__main__":
    main()
