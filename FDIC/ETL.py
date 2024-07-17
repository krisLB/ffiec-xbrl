from fuzzywuzzy import fuzz, process
#from FDIC.constants import folder_Orig, folder_Dest, path, service_path, filename_BankDim
import FDIC.constants as paths
from FDIC.constants_private import ffiec_un, ffiec_pw

import pandas as pd
from zeep import Client, exceptions #, xsd 
from zeep.wsse.username import UsernameToken
import re
import os.path
import glob
import xml.etree.ElementTree as ET
import datetime
import FDIC.wsio as wsio


class ZeepServiceProxy:
    def __init__(self, service, rate_limiter):
        self._service = service
        self._rate_limiter = rate_limiter
        
    def __getattr__(self, name):
        service_method = getattr(self._service, name)
        
        @self._rate_limiter
        def wrapped_service_method(*args, **kwargs):
            return service_method(*args, **kwargs)
        
        return wrapped_service_method



class ETL:

    def __init__(self, wsdl, rate_limiter):
        self.client = Client(wsdl, wsse=UsernameToken(ffiec_un,ffiec_pw))
        self.service = ZeepServiceProxy(self.client.service, rate_limiter)
        self.rate_limiter = rate_limiter


    def GenBankDim(self):
        """
        
        """
        dtypes = {'RSSD_ID':str,
                'FDIC_Certificate_Number':str,
                'OCC_Charter_Number':str,
                'OTS_Docket_Number':str,
                'Primary_ABA_Routing_Number':str,
                'Financial_Institution_Name': str,
                'Financial_Institution_Address': str,
                'Financial_Institution_City': str,
                'Financial_Institution_State': str,
                'Financial_Institution_Zip_Code': str,
                'Financial_Institution_Filing_Type': str}
        RssdDict = wsio.ReadCSV(paths.folder_Orig + 'RSSD_Dict', dtype = dtypes)

        # Set up Bank Name lookup
        BankDict = []
        cnt = 0
        
        print('Generate Bank Dictionary:')
        for i, r in RssdDict.iterrows():
            fi_zip = str(r['Financial_Institution_Zip_Code']).strip()
            if len(fi_zip) > 5:
                RssdDict.loc[i,['Financial_Institution_Zip_Code']] = int(fi_zip[:5])
            fi_name = r['Financial_Institution_Name']
            #tz = r['Financial_Institution_Zip_Code'].strip()
            fi_state = r['Financial_Institution_State'].strip().upper()
            fi_city = r['Financial_Institution_City'].strip().upper()
            
            choices = RssdDict[RssdDict.Financial_Institution_Zip_Code == fi_zip]
            choiceStat='zip'
            if len(choices) == 0:
                choiceStat = 'CityState'
                # print('BAD ZIP', fi_zip, fi_city, fi_state)
                choices = RssdDict[((RssdDict.Financial_Institution_State == fi_state) &
                                    (RssdDict.Financial_Institution_City == fi_city))]
                if len(choices) == 0:
                    # print('BAD CITY STATE')
                    choiceStat = 'State'
                    choices = RssdDict[RssdDict.Financial_Institution_State == fi_state]

            fuzzr = process.extract(fi_name, list(choices['Financial_Institution_Name']), limit=1)
            if fuzzr[0][1] >= 90:
                try:
                    tdf = RssdDict[((RssdDict.Financial_Institution_Name == fuzzr[0][0]) &
                                (RssdDict.Financial_Institution_City == fi_city) &
                                (RssdDict.Financial_Institution_State == fi_state))]
                except Exception as e:
                    print(f'Exception {e}. \nLength of tdf: {len(tdf)}\n{tdf}')    
                BankDict.append([fi_name,
                                tdf['RSSD_ID'].item(),
                                fi_state,
                                '',
                                tdf['Financial_Institution_Name'].item(),
                                tdf['Financial_Institution_Address'].item(),
                                fi_city,
                                fi_zip])
            else:
                fuzzr_old = fuzzr
                fi_address = r['Address']
                fuzzr = process.extract(fi_address, list(choices['Financial_Institution_Address']), limit=1)
                if fuzzr[0][1] >= 90:

                    tdf = RssdDict[((RssdDict.Financial_Institution_Address == fuzzr[0][0]) &
                            (RssdDict.Financial_Institution_State == fi_state))]
                    # print('ADDRESS MATCH',tdf['Financial_Institution_Name'].item(), dbank, fuzzr[0][1])
                    print('append address match')
                    BankDict.append([fi_name,
                                    tdf['RSSD_ID'].item(),
                                    fi_state,
                                    '',
                                    tdf['Financial_Institution_Name'].item(),
                                    tdf['Financial_Institution_Address'].item(),
                                    fi_city,
                                    fi_zip])

                else:
                    if fuzzr_old[0][1]>fuzzr[0][1]:
                        print('BAD MATCH - FAVORS NAME', fi_name, fuzzr_old, choiceStat)
                    else:
                        print('BAD MATCH - FAVORS ADDRESS', fi_name, fuzzr,tdf['Financial_Institution_Name'].item(), choiceStat)

        wsio.WriteDataFrame(paths.folder_Orig + paths.filename_BankDim, pd.DataFrame(BankDict,columns=['Bank',
                                                'RSSD_ID',
                                                'State',
                                                'Class',
                                                'FDIC_Name',
                                                'Address',
                                                'City',
                                                'Zip']).applymap(str))
        print(f'\t{paths.folder_Orig + paths.filename_BankDim}')
        print(f'\t{i} records added to file')


    # Match Dad Bank Name to FDIC
    def MatchBankNames(self, input_file=paths.folder_Orig + paths.filename_BankDim):
        rssd_dtypes = {'RSSD_ID':str,
                'FDIC_Certificate_Number':str,
                'OCC_Charter_Number':str,
                'OTS_Docket_Number':str,
                'Primary_ABA_Routing_Number':str,
                'Financial_Institution_Name': str,
                'Financial_Institution_Address': str,
                'Financial_Institution_City': str,
                'Financial_Institution_State': str,
                'Financial_Institution_Zip_Code': str,
                'Financial_Institution_Filing_Type': str,
                'Last_Date/Time_Submission_Updated_On': datetime.datetime}
        RssdDict = wsio.ReadCSV(paths.localPath + paths.filename_RSSD, dtype = rssd_dtypes)
        if input_file == '':
            print('Please supply input file')
            return
        bank_dtypes = {'Bank':str,
            'RSSD_ID':str,
            'State': str,
            'Class':str,
            'FDIC_Name': str,
            'Address': str,
            'City': str,
            'Zip': str}
        ib = wsio.ReadCSV(input_file, dtype = bank_dtypes)
        ib = ib[ib.RSSD_ID.isnull()]
        if len(ib) == 0:
            return 'All banks have RSSD_ID Match'
        # Make zip code 5 digits
        for i, r in RssdDict.iterrows():
            fi_zip = str(r['Financial_Institution_Zip_Code'])
            if len(fi_zip) > 5:
                RssdDict.loc[i,['Financial_Institution_Zip_Code']] = int(fi_zip[:5])
        return

    #FIGURE OUT THIS SECTION and whether it's necessary
    '''
        # Set up Bank Name lookup
        BankDict = []
        cnt = 0
        for i, r in ib.iterrows():
            cnt = cnt+1
            fi_name = r['Bank']
            fi_zip = r['Zip'].strip()
            fi_state = r['State'].strip().upper()
            fi_city = r['City'].strip().upper()
            choices = RssdDict[RssdDict.Financial_Institution_Zip_Code == fi_zip]
            choiceStat='zip'
            if len(choices) == 0:
                choiceStat = 'CityState'
                # print('BAD ZIP', fi_zip, fi_city, fi_state)
                choices = RssdDict[((RssdDict.Financial_Institution_State == fi_state) &
                                    (RssdDict.Financial_Institution_City == fi_city))]
                if len(choices) == 0:
                    # print('BAD CITY STATE')
                    choiceStat = 'State'
                    choices = RssdDict[RssdDict.Financial_Institution_State == fi_state]

            fuzzr = process.extract(fi_name, list(choices['Financial_Institution_Name']), limit=1)
            if fuzzr[0][1] >= 90:
                tdf = RssdDict[((RssdDict.Financial_Institution_Name == fuzzr[0][0]) &
                            (RssdDict.Financial_Institution_State == fi_state))]
                BankDict.append([fi_name,
                                tdf['RSSD_ID'].item(),
                                fi_state,
                                r['Class'],
                                tdf['Financial_Institution_Name'].item(),
                                tdf['Financial_Institution_Address'].item(),
                                fi_city,
                                fi_zip])
            else:
                fuzzr_old = fuzzr
                fi_address = r['Address']
                fuzzr = process.extract(fi_address, list(choices['Financial_Institution_Address']), limit=1)
                if fuzzr[0][1] >= 90:

                    tdf = RssdDict[((RssdDict.Financial_Institution_Address == fuzzr[0][0]) &
                            (RssdDict.Financial_Institution_State == fi_state))]
                    # print('ADR MATCH',tdf['Financial_Institution_Name'].item(), fi_name, fuzzr[0][1])
                    print('append address match')
                    BankDict.append([fi_name,
                                    tdf['RSSD_ID'].item(),
                                    fi_state,
                                    r['Class'],
                                    tdf['Financial_Institution_Name'].item(),
                                    tdf['Financial_Institution_Address'].item(),
                                    fi_city,
                                    fi_zip])

                else:
                    if fuzzr_old[0][1]>fuzzr[0][1]:
                        print('BAD MATCH FAVOR NAME', fi_name, fuzzr_old, choiceStat)
                    else:
                        print('BAD MATCH FAVOR ADDR', fi_name, fuzzr,tdf['Financial_Institution_Name'].item(), choiceStat)


        ib = wsio.ReadCSV(paths.localPath + input_file, dtype = dtypes)
        ib = ib[~ib.RSSD_ID.isnull()]
        rdlkp = pd.concat([pd.DataFrame(BankDict,columns=['Bank',
                                                'RSSD_ID',
                                                'State',
                                                'Class',
                                                'FDIC_Name',
                                                'Address',
                                                'City',
                                                'Zip']).applymap(str),ib]).reset_index()
        rssd_lkp = rdlkp['Bank','RSSD_ID','State','Class','FDIC_Name','Address','City','Zip']

        # Check for Missing FDIC Names if RSSD_ID exists
        for i, r in rssd_lkp.iterrows():
            if pd.isnull(r['FDIC_Name']):
                tdf = RssdDict[RssdDict.RSSD_ID == str(r['RSSD_ID'])]
                rssd_lkp.loc[i,'FDIC_Name'] = tdf['Financial_Institution_Name'].item()
        wsio.WriteDataFrame(paths.localPath + paths.filename_MissingName,rssd_lkp)
        return rssd_lkp
    '''
        

    # Initiate FFIEC SOAP Client
    #def InitiateSOAPClient(self)-> Client:
    #    wsdl = paths.service_path
    #    client = Client(wsdl=wsdl, wsse=UsernameToken(ffiec_un,ffiec_pw))
    #    return client


    # Get Filers from FFIEC As Of A Specific Date
    def RetrieveFilersSinceDate(self, finlPeriod: str, lastUpdate_finlPeriod: str) -> list[int]:

        errorList = []
        try:
            response = self.service.RetrieveFilersSinceDate(dataSeries = 'Call',
                                                            reportingPeriodEndDate = finlPeriod,
                                                            lastUpdateDateTime = lastUpdate_finlPeriod)
        except:
            errorList.append([finlPeriod, lastUpdate_finlPeriod])
            print(errorList)

        return(response)


    # Get Filers from FFIEC As Of A Specific Filing Date
    def RetrieveFilersSubmissionDateTime(self, finlPeriod: str, lastUpdate_finlPeriod: str) -> list[int]:

        errorList = []
        try:
            response = self.service.RetrieveFilersSubmissionDateTime(dataSeries = 'Call',
                                                            reportingPeriodEndDate = finlPeriod,
                                                            lastUpdateDateTime = lastUpdate_finlPeriod)
        except:
            errorList.append([finlPeriod, lastUpdate_finlPeriod])
            print(errorList)

        return(response)


    # Download Call Reports from FFIEC Service
    def DownloadCallReports(self, ftypes=[]):
        outboundCount = 0
        if len(ftypes) == 0:
            print('Please Select a file type to Download PDF or XBRL')
            return
        else:
            for ftype in ftypes:
                ftypes[ftypes.index(ftype)] = ftype.upper()
                if ftype not in ['PDF','XBRL']:
                    ftypes.remove(ftype)
                    print(ftype ,' is not a valid response')
        rssd_lkp = wsio.ReadCSV(paths.folder_Orig + paths.filename_RSSD)
        bnk_dim = wsio.ReadCSV(paths.folder_Orig + paths.filename_BankDim)
        
        #client = self.InitiateSOAPClient()
        reporting_periods = self.client.service.RetrieveReportingPeriods(dataSeries='Call')
        errorList = []

        if len(bnk_dim[pd.isnull(bnk_dim.RSSD_ID)]) != 0:
            bnk_dim = self.MatchBankNames(paths.localPath + paths.filename_BankDim)

        bnk_dim_rssds = bnk_dim['RSSD_ID'].drop_duplicates()

        for rssdid in bnk_dim_rssds:
            try:
                bkn = rssd_lkp[rssd_lkp.RSSD_ID == rssdid]['Financial_Institution_Name'].item()
            except:
                print('Invalid RSSD_ID or Multiple references in RSSD_DICT')
            fbkn = re.sub('[.,-/\&]','',bkn)
            fbkn_forPath = fbkn + '_' + str(rssdid)
            located = []
            for ftype in ftypes:
                for f in glob.glob(paths.localPath + paths.folder_BulkReports + fbkn_forPath + '/*.' + ftype):
                    located.append(f)

            #Create empty skipped_df for each RSSD iteration
            skipped_dtypes = {'Date': str,
                            'As_of': str}
            skipped_df = pd.DataFrame(columns=skipped_dtypes.keys())
            skippedFile_path = paths.localPath + paths.folder_BulkReports + fbkn_forPath + '/' + paths.filename_Skipped

            if len(reporting_periods) == len(located):
                print(f'Already downloaded: {fbkn_forPath}')
                continue
            elif os.path.isfile(paths.localPath + paths.folder_BulkReports + fbkn_forPath + '/' + paths.filename_Skipped):
                skipped_df = wsio.ReadCSV(skippedFile_path, dtype=skipped_dtypes)
                skipped_dates = skipped_df['Date'].values
                skipped_len = len(skipped_dates)
                if len(reporting_periods) == len(located) + skipped_len:
                    print(f'Already downloaded: {fbkn_forPath}')
                    continue
            elif not os.path.exists(paths.localPath + paths.folder_BulkReports + fbkn_forPath):
                os.makedirs(paths.localPath + paths.folder_BulkReports + fbkn_forPath)
                ##CREATE BANK_LOG FILE
                

            print(f'\nDownloading: {fbkn_forPath}')

            #Check for Skipped file  -- Consolidate w/ check above
            if os.path.isfile(skippedFile_path):
                skipped_df = wsio.ReadCSV(skippedFile_path, dtype=skipped_dtypes)
                skipped_dates = skipped_df['Date'].values
            else:
                skipped_dates = []

            for dt in [x for x in reporting_periods if x not in skipped_dates]:
                #format dt for path to be YYYYMMDD from MM/DD/YYYY
                fdt = re.sub('[ -//]','',dt)
                fdt= fdt[len(fdt)-4:] + fdt[:len(fdt)-4]
                if len(fdt) < 8:
                    fdt = fdt[:4] + '0' + fdt[4:]

                for ftype in ftypes:
                    fname = paths.localPath + paths.folder_BulkReports  + fbkn_forPath + '/' + fbkn_forPath + '_' + fdt + '.' + ftype

                    if fname in located:
                        print(f'\t{dt}: already downloaded')
                        continue
                    outboundCount +=1
                    try:
                        response = self.service.RetrieveFacsimile(dataSeries = 'Call',
                                                                    reportingPeriodEndDate=dt,
                                                                    fiIDType='ID_RSSD',
                                                                    fiID = rssdid,
                                                                    facsimileFormat = ftype)
                    except exceptions.Fault as fault:
                        errorList.append([fbkn, dt, rssdid])
                        #print(f'\tError Message: {fault.message}')
                        if fault.code == 'Server.FacsimileNotFoundOrUnavailable':
                            #Create skipped_df to track all records (dates) to skip in future without an api call
                            skipped_df = pd.concat([skipped_df, pd.DataFrame({'Date': dt, 'As_of': datetime.datetime.now()}, index=[0])], ignore_index=True)                        
                        elif fault.code == 'q0:Client.UserQuotaExceeded':
                            print(f'\t{dt}: Error_code: {fault.code}\n{outboundCount} records retrieved. Service paused until {datetime.datetime.fromtimestamp(self.rate_limiter.start_time + 3600).strftime("%Y.%m.%d %H:%M:%S")}.\nQuitting...')
                            self.rate_limiter.wait()
                        else:
                            print(f'\t{dt}: Error_code: {fault.code}\n{outboundCount} records retrieved.')
                            break_process = input('Stop loader? (y/N): ')
                            if break_process.lower() == 'y':
                                exit()
                        continue

                    with open(fname, 'wb') as f:
                        f.write(response)
                    print(f'\t{dt}: downloaded')
            #Only write skipped file in RSSD_ID folder if skipped_df >0
            if skipped_df.shape[0] >0:
                wsio.WriteDataFrame(skippedFile_path, skipped_df)
        if errorList: print(f'Error List: {errorList}')
        print(f'{outboundCount} records retrieved.')
        return


    def ParseXBRL(self, filepath):
        """

        """
        # Parse XML
        tree  = ET.parse(filepath)
        root = tree.getroot()

        report_values = []
        for child in root:
            #print(child.tag, child.attrib)
            code = child.tag[child.tag.find('}')+1:]
            if len(code) == 8:
                report_values.append([code, child.text])

        parsed_df = pd.DataFrame(report_values, columns = ['MDRM_Item','Value'])
        MDRM_df = wsio.ReadCSV(paths.folder_Orig + paths.filename_MDRM)
        full_df=parsed_df.set_index('MDRM_Item').join(MDRM_df.set_index('MDRM_Item'))
        #print(len(tot[pd.isnull(tot.Item_Name)]))
        full_df.reset_index(level=0, inplace=True)
        return full_df


    def RenameFolders(self):
        """
            UPDATE THIS        
        """

        bnk_dim = wsio.ReadCSV(paths.folder_Orig + paths.filename_BankDim)

        #Rename folders
        for folder in glob.glob(paths.localPath + paths.folder_BulkReports + '*'):
            bank = folder[len(paths.localPath + paths.folder_BulkReports):]

            if '_' in bank:
                print(bank, 'No Rename')
                tdf = bnk_dim[bnk_dim.FDIC_Name == bank[:bank.rfind('_')]]
            else:
                print(bank, 'Rename')
                tdf = bnk_dim[bnk_dim.FDIC_Name == bank]
            trssd = tdf['RSSD_ID'].item()
            if not str(trssd) in folder:
                os.rename(folder, folder + '_' + str(trssd))
        return


    def GenBankMaster(self, path = paths.localPath + paths.folder_BulkReports):
        """
        Consolidates qtrly call reports into a single bank-level file. File is saved to the bank specific folder as <RSSID>_master.csv 
        """
        instn_count, records_count = 0, 0
        print('Generate Bank Master:')
        for instn_count,folder in enumerate(glob.glob(path + '*[!.]')):
            fdf = pd.DataFrame()
            bank_foldername = folder[len(path):]
            bank_name = bank_foldername[:bank_foldername.rfind('_')]
            rssd = bank_foldername[bank_foldername.rfind('_')+1:]
            filepathname = folder + '/' + rssd + '_master'
            #Only parse folders in path; skip all files
            if os.path.isfile(filepathname) and os.path.isdir(folder):
                continue
            for call_report_name in glob.glob(path + bank_foldername + '/*.XBRL'):
                period_date = call_report_name[call_report_name.rfind('_')+1:len(call_report_name)-5]
                period_date = datetime.datetime.strptime(period_date, '%Y%m%d').date()
                #print(period_date, bank_name, rssd)
                tdf = self.ParseXBRL(call_report_name)
                #print(tdf)
                tdf['ReportPeriodEndDate']=period_date
                tdf['RSSD_ID'] = rssd
                tdf = tdf[['ReportPeriodEndDate','RSSD_ID','Item_Name','MDRM_Item','Confidential','Value']]
                try:
                    fdf = pd.concat([fdf,tdf])
                except NameError:
                    fdf = tdf
                records_count +=1
            wsio.WriteDataFrame(filepathname, fdf)
            print(f'\t{filepathname}')
            del fdf
        print(f'{records_count} records processed across {instn_count} institutions')
        return


    def createDF_from_CSV(self, path_root = paths.localPath + paths.folder_BulkReports) -> pd.DataFrame:
        print('Loading DF from CSVs:')
        for instn_count, folder in enumerate(glob.glob(path_root + '*')):
            rssd = folder[folder.rfind('_')+1:]
            for file in glob.glob(folder + '/*_master.csv'):
                tdf = wsio.ReadCSV(file)
                tdf['RSSD_ID'] = rssd
            try:
                fdf = pd.concat([fdf,tdf])
            except NameError:
                fdf = tdf
        print(f'\tDataframe loaded with {instn_count} institutions processed')
        return fdf


    def GenCallMaster(self, path = paths.localPath + paths.folder_BulkReports):
        print('Generate Call Master:')
        #print(path + '*')
        for instn_count, folder in enumerate(glob.glob(path + '*')):
            rssd = folder[folder.rfind('_')+1:]
            for file in glob.glob(folder + '/*_master.csv'):
                tdf = wsio.ReadCSV(file)
                tdf['RSSD_ID'] = rssd
            try:
                fdf = pd.concat([fdf,tdf])
            except NameError:
                fdf = tdf
        wsio.WriteDataFrame(path + '../' + paths.filename_MasterCall, fdf)
        print(f'\t{path + paths.filename_MasterCall}')
        print(f'\t{instn_count} institutions processed in file')
        return


    def loadCSV(self, path = paths.localPath + paths.filename_MasterCall) -> pd.DataFrame:
        if os.path.isfile(path):
            df = wsio.ReadCSV(path)
        else:
            df = None
        return df

