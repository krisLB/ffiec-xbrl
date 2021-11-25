from FDIC import ETL, ExEngine

def main():
    # ExEngine.FillMaster('504713')
    ETL.GenBankDim()
    ETL.DownloadCallReports(['XBRL'])
    ETL.ParseXBRL()
    ETL.GenBankMaster()
    ETL.GenCallMaster()

if __name__ == "__main__":
    main()
