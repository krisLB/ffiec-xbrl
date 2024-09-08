from fuzzywuzzy import fuzz, process
#from FDIC.constants import folder_Orig, folder_Dest, path, service_path, filename_BankDim
import FDIC.constants as paths
from FDIC.constants_private import ffiec_un, ffiec_pw

import pandas as pd
from zeep import Client, exceptions #, xsd 
from zeep.wsse.username import UsernameToken
from requests.exceptions import ReadTimeout
import re
import os.path
import sys
import glob
import xml.etree.ElementTree as ET
import datetime
import time
import numpy as np
#import FDIC.wsio as wsio
from FDIC import wsio, Logger


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

    def __init__(self, wsdl, rate_limiter, logger=None):
        self.client = Client(wsdl, wsse=UsernameToken(ffiec_un,ffiec_pw))
        self.service = ZeepServiceProxy(self.client.service, rate_limiter)
        self.rate_limiter = rate_limiter
        self.logger = logger


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
        try:
            reporting_periods = self.client.service.RetrieveReportingPeriods(dataSeries='Call')
        except exceptions.Fault as fault:
            if fault.code == 'q0:Client.UserQuotaExceeded':
                print(f'\t{dt}: Error_code: {fault.code}\n')
                try:
                    self.rate_limiter.wait(3600)
                except KeyboardInterrupt: 
                    print(f'User interrupted application.\n{msg}')
                    if self.logger:
                        self.logger.log_instantly(msg)
                    sys.exit(1) 

        errorList = []

        if len(bnk_dim[pd.isnull(bnk_dim.RSSD_ID)]) != 0:
            bnk_dim = self.MatchBankNames(paths.localPath + paths.filename_BankDim)

        bnk_dim_rssds = bnk_dim['RSSD_ID'].drop_duplicates()
        
        skippedInstnCount, outboundInstnCount, outboundRecCount = 0, 0, 0
        incr_skippedInstnCount, incr_outboundInstnCount, incr_outboundRecCount = 0, 0, 0
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
                skippedInstnCount +=1; incr_skippedInstnCount +=1
                continue
            elif os.path.isfile(paths.localPath + paths.folder_BulkReports + fbkn_forPath + '/' + paths.filename_Skipped):
                skipped_df = wsio.ReadCSV(skippedFile_path, dtype=skipped_dtypes)
                skipped_dates = skipped_df['Date'].values
                skipped_len = len(skipped_dates)
                if len(reporting_periods) == len(located) + skipped_len:
                    print(f'Already downloaded: {fbkn_forPath}')
                    skippedInstnCount +=1; incr_skippedInstnCount +=1                   
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

            outboundInstnCount += 1; incr_outboundInstnCount +=1
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
                    outboundRecCount +=1; incr_outboundRecCount +=1
                    msg = f'{skippedInstnCount} institutions reviewed.  {outboundRecCount} records retrieved across {outboundInstnCount} institutions.'
                    #msg_logger = f'{skippedInstnCount} institutions reviewed.  {outboundRecCount} records retrieved across {outboundInstnCount} institutions.'
                    try:
                        response = self.service.RetrieveFacsimile(dataSeries = 'Call',
                                                                    reportingPeriodEndDate=dt,
                                                                    fiIDType='ID_RSSD',
                                                                    fiID = rssdid,
                                                                    facsimileFormat = ftype,
                                                                    log_msg = msg)                    
                    except exceptions.Fault as fault:
                        # Handle SOAP Exceptions
                        errorList.append([fbkn, dt, rssdid])
                        #print(f'\tError Message: {fault.message}')
                        if fault.code == 'Server.FacsimileNotFoundOrUnavailable':
                            #Create skipped_df to track all records (dates) to skip in future without an api call
                            skipped_df = pd.concat([skipped_df, pd.DataFrame({'Date': dt, 'As_of': datetime.datetime.now()}, index=[0])], ignore_index=True)                        
                        elif fault.code == 'q0:Client.UserQuotaExceeded':
                            print(f'\t{dt}: Error_code: {fault.code}\n')
                            #print(f'Service paused at {datetime.datetime.fromtimestamp(self.rate_limiter.start_time).strftime("%Y.%m.%d %H:%M:%S")}. Service to resume at {datetime.datetime.fromtimestamp(self.rate_limiter.start_time + 3600).strftime("%Y.%m.%d %H:%M:%S")}.\n{msg}')
                            try:
                                self.rate_limiter.wait(3600)
                            except KeyboardInterrupt:
                            #except Exception:
                            #except exceptions.Fault as rate_fault:    
                                print(f'User interrupted application.\n{msg}')
                                if self.logger:
                                    self.logger.log_instantly(msg)
                                sys.exit(1) 
                        else:
                            print(f'\t{dt}: Error_code: {fault.code}\n{msg}')
                            if self.logger:
                                self.logger.log_instantly(msg)
                            break_process = input('Stop loader? (y/N): ')
                            if break_process.lower() == 'y':
                                sys.exit(1)
                        continue
                    except exceptions.Error as e:
                        # Handle broader Zeep errors - Request Timeouts
                        if e.__cause__ and isinstance(e.__cause__, ReadTimeout):
                            # Handle the ReadTimeout exception specifically
                            print(f"ReadTimeout occurred: {e.__cause__}")
                            time.sleep(5)
                            #ADD CODE TO HANDLE REPEAT CASES TO STOP INDEFINITE
                            continue
                        else:
                            # Handle other zeep errors
                            print(f"Zeep error occurred: {e}")
                    except Exception as e:
                        # Handle other exceptions
                        print(f"An unexpected error occurred: {e}")

                    with open(fname, 'wb') as f:
                        f.write(response)
                    print(f'\t{dt}: downloaded')
            #Only write skipped file in RSSD_ID folder if skipped_df >0
            if skipped_df.shape[0] >0:
                wsio.WriteDataFrame(skippedFile_path, skipped_df)
        if errorList: print(f'Error List: {errorList}')
        print(msg)
        if self.logger:
            self.logger.log_instantly(msg=msg)
        return


    def ParseXBRL(self, filepath: str, allow_external_references: bool = True):
        """
        Parses Call Reports to extract all MDRM item and value from XBRL file
        """

        def clean_xbrl_tag(text):
            remove_text = [
                            '{http://www.w3.org/1999/xlink}',
                            '{http://www.xbrl.org/2003/linkbase}',
                            '{http://www.xbrl.org/2003/instance}',
                            '{http://www.ffiec.gov/xbrl/call/concepts}'
                        ]
            
            for rt in remove_text:
                text = text.replace(rt, "")
            return text

        xlink = '{http://www.w3.org/1999/xlink}'

        # Parse XML
        tree  = ET.parse(filepath)
        root = tree.getroot()
        reporting_form = None

        report_values = []
        for child in root:
            #print(child.tag, child.attrib)
            #code = child.tag[child.tag.find('}')+1:]
            code = clean_xbrl_tag(child.tag)

            if code == 'schemaRef' and reporting_form is None:
                #get attrib=href
                #parse url for regex='report{0-9}*3'
                attrib = child.attrib.get(xlink+'href')
                if attrib:
                    reporting_form = re.search(pattern=r'report\d{3}', string=attrib)
                    reporting_form_str = reporting_form.group(0) if reporting_form else None

            data_unit = child.attrib.get('unitRef')
            data_decimals = child.attrib.get('decimals')
            
            #Infer datatype based on basic logic and value
            if data_decimals:
                if int(data_decimals) == 0:
                    data_type = 'int'
                elif int(data_decimals) >0:
                    data_type = 'float'
                else:
                    raise TypeError()
            elif (child.text and child.text.strip().lower() in ['true','false']):
                data_type = 'bool'
            else:
                data_type = 'str'

            #NEED SOMETHING MORE ROBUST TO CAPTURE VALUES HERE
            if len(code) == 8:
                report_values.append([code, child.text, data_type, data_unit, data_decimals, reporting_form_str])

        parsed_df = pd.DataFrame(report_values, columns = ['MDRM_Item','Value','Datatype','Unit','Decimals', 'CallReport_Form'])
        
        if allow_external_references:
            MDRM_df = wsio.ReadCSV(paths.folder_Orig + paths.filename_MDRM)
            full_df=parsed_df.set_index('MDRM_Item').join(MDRM_df.set_index('MDRM_Item'))
            #print(len(tot[pd.isnull(tot.Item_Name)]))
            full_df.reset_index(level=0, inplace=True)
            return full_df
        else:
            return parsed_df



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


    def get_latest_mod_date(self, path, file_search_pattern='*'):
        """
        Get the latest modification date of files in a directory.
        """
        latest_date = None
        for file_path in glob.glob(path + file_search_pattern):
            file_date = os.path.getmtime(file_path)
            if latest_date is None or file_date > latest_date:
                latest_date = file_date
        return latest_date


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
            
            #Skip all files, only parse folders in path
            if os.path.isfile(filepathname) and os.path.isdir(folder):
                continue
            
            #Only process folders where one xbrl file has modification date later than _master.csv
            date_master = self.get_latest_mod_date(path=folder, file_search_pattern='/*_master.csv')
            date_xbrl = self.get_latest_mod_date(path=folder, file_search_pattern='/*.XBRL')
            if (date_master and date_xbrl) and (date_master > date_xbrl):
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
        folders = glob.glob(path + '*')
        total_folders = len(folders)

        for instn_count, folder in enumerate(folders):
            print(f"\r\tProcessing record {instn_count} of {total_folders}", end='', flush=True)
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


    def build_bankDim(self, filepath_in = paths.localPath + paths.folder_RSSDs + '* POR *', filepath_out = paths.folder_Orig + paths.filename_RSSD):
        """
        Converts period based POR files from FFIEC into a single cleaned RSSD.csv file
        """
        #NEEDS TESTED!

        dtypes = {'IDRSSD': np.dtype('str'),
          'FDIC Certificate Number': np.dtype('str'),
          'OCC Charter Number': np.dtype('str'),
          'OTS Docket Number': np.dtype('str'),
          'Primary ABA Routing Number': np.dtype('str'),
          'Financial Institution Name': np.dtype('str'),
          'Financial Institution Address': np.dtype('str'),
          'Financial Institution City': np.dtype('str'),
          'Financial Institution State': np.dtype('str'),
          'Financial Institution Zip Code': np.dtype('str'),
          'Financial Institution Filing Type': np.dtype('str'),
          'Last Date/Time Submission Updated On': np.dtype('str')}

        #Create blank dataframe
        df = pd.DataFrame()

        #Open csv files and import to dataframe
        for filename in glob.glob(filepath_in):
            with open(filename, 'r') as file:
                df_new = pd.read_csv(filename, sep='\t', index_col=False, quotechar='"', dtype = dtypes, parse_dates=['Last Date/Time Submission Updated On'])
            df = pd.concat([df, df_new])

        #Clean RSSD_Por File - strip spaces for each field
        df = df.apply(lambda x: x.str.strip() if x.dtype.name == 'object' else x, axis=0)

        #Clean RSSD_Por Format
        df.rename(columns={'IDRSSD': 'RSSD_ID'}, inplace=True)
        df.columns = df.columns.str.replace(' ', '_')

        #Create RSSD_Dict file
        df.to_csv(filepath_out, sep=',', quotechar='"', index= False)
        return


    def get_MDRMdict_from_source(self):
        #BUILD THIS METHOD
        #https://www.federalreserve.gov/apps/mdrm/download_mdrm.htm
        pass


    def build_MDRMdict(self, input_path =paths.localPath+paths.folder_MDRMs+paths.filename_MDRM_src, export_path =paths.folder_Orig + paths.filename_MDRM, filters=None):
        """
        Reads a CSV file, filters the DataFrame based on the given criteria, 
        and exports the result to a new CSV file.

        Parameters:
        - input_path: str, path to the input CSV file.
        - export_path: str, path where the filtered DataFrame should be saved.
        - filters: dict, optional. A dictionary where keys are column names and values 
                are lists of filter criteria.

        Example:
        build_MDRMdict(
            input_path='path/to/MDRM_CSV.csv',
            export_path='path/to/export/MDRM_dict.csv',
            filters={'Reporting Form': ['FFIEC 031', 'FFIEC 041', 'FFIEC 051']})
        """

        def format_str_date(date_string):
            try:
                date_part = date_string.split()[0]
                month, day, year = date_part.split('/')
                return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
            except:
                return date_string


        def build_datatypes_from_bankXBRL(folderpath_callReport):

            bank_df = pd.DataFrame()    
            if folderpath_callReport[-1:] == '/':
                folderpath_callReport = folderpath_callReport[:-1]

            bank_foldername = folderpath_callReport.rsplit('/',1)[-1]
            rssd = bank_foldername[bank_foldername.rfind('_')+1:]
            folders = glob.glob(folderpath_callReport + '/*')[:10]

            for folder in folders:
                for call_report_filename in glob.glob(folder + '/*.XBRL'):
                    period_date = call_report_filename[call_report_filename.rfind('_')+1:len(call_report_filename)-5]
                    period_date = datetime.datetime.strptime((call_report_filename[call_report_filename.rfind('_')+1:len(call_report_filename)-5]), '%Y%m%d').date()
                    #print(period_date, bank_name, rssd)
                    tb_df = self.ParseXBRL(call_report_filename, allow_external_references =False)
                    #print(tdf)
                    tb_df['ReportPeriodEndDate'] =period_date
                    tb_df['RSSD_ID'] =rssd
                    tb_df = tb_df[['MDRM_Item','Datatype','Unit','Decimals', 'ReportPeriodEndDate']] #, 'CallReport_Form']]
                    try:
                        #bank_df = pd.concat([bank_df,tb_df])
                        bank_df = pd.concat([bank_df,tb_df], ignore_index=True).drop_duplicates(['MDRM_Item'], keep='first') #,'CallReport_Form'], keep='first')
                    except NameError:
                        bank_df = tb_df
            
            #Set types of merge item
            bank_df = bank_df.astype(dtype={'MDRM_Item':'string'})

            return bank_df


        # Read the CSV file with headers starting on line 2
        df = pd.read_csv(input_path, header=1, sep=',', quotechar='"') #, parse_dates=['Start Date', 'End Date'])
        df.dropna(axis=1, how='all', inplace=True)

        df[['Start Date', 'End Date']] = df.apply(lambda row: pd.Series([
            format_str_date(row['Start Date']),
            format_str_date(row['End Date'])]),
            axis=1)

        # Apply filters if provided
        if filters:
            for column, values in filters.items():
                df = df[df[column].isin(values)]
        
        #Rename columns and format df to needed format and order
        rename_columns={'Start Date':'Start_Date',
                        'End Date':'End_Date',
                        'Item Name':'Item_Name',
                        'Confidentiality':'Confidential',
                        'ItemType':'Item Type',
                        'Reporting Form':'MDRMReport_Form',
                        'SeriesGlossary':'Series Glossary'}
        df.rename(columns=rename_columns, inplace=True)
        df['MDRM_Item'] = (df['Mnemonic'] + df['Item Code'].astype(str)).astype(str)
        #df = df.astype(dtype={'MDRM_Item':'string'})
        df = df[['MDRM_Item', 'Start_Date', 'End_Date', 'Item_Name', 'Confidential']] #, 'MDRMReport_Form']]

        #Get metadata from XBRL call reports
        bank_df = build_datatypes_from_bankXBRL(folderpath_callReport =paths.localPath + paths.folder_BulkReports)
        #Merge dataframes
        merged_df = pd.merge(df, bank_df, how='right', on='MDRM_Item', left_index=False, right_index=False, suffixes=['_l', '_r']) 

        # Export the filtered DataFrame to the specified export path
        if export_path:
            merged_df.to_csv(export_path, index=False)
        else:
            return merged_df