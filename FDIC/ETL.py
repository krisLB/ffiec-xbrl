from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from FDIC.constants import direcOrig
from FDIC.constants import direcDest
from FDIC.constants import direc
from FDIC.constants import ffiec_un
from FDIC.constants import ffiec_pw
import pandas as pd
from zeep import Client, xsd
from zeep.wsse.username import UsernameToken
import re
import os.path
import glob
import xml.etree.ElementTree as ET
import datetime as dt
import FDIC.wsio as wsio


def GenBankDim():
    datat = {'RSSD_ID':str,
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
    RssdDict = wsio.ReadCSV(direcOrig + 'RSSD_Dict', dtype = datat)

    # Set up Bank Name lookup
    BankDict = []
    cnt = 0
    # Make zip code 5 digits
    for i, r in RssdDict.iterrows():
        zp = str(r['Financial_Institution_Zip_Code'])
        if len(zp) > 5:
            RssdDict.loc[i,['Financial_Institution_Zip_Code']] = int(zp[:5])
        dbank = r['Financial_Institution_Name']
        tz = r['Financial_Institution_Zip_Code'].strip()
        st = r['Financial_Institution_State'].strip().upper()
        ct = r['Financial_Institution_City'].strip().upper()
        choices = RssdDict[RssdDict.Financial_Institution_Zip_Code == tz]
        choiceStat='zip'
        if len(choices) == 0:
            choiceStat = 'CityState'
            # print('BAD ZIP', zp, ct, st)
            choices = RssdDict[((RssdDict.Financial_Institution_State == st) &
                                (RssdDict.Financial_Institution_City == ct))]
            if len(choices) == 0:
                # print('BAD CITY STATE')
                choiceStat = 'State'
                choices = RssdDict[RssdDict.Financial_Institution_State == st]

        fuzzr = process.extract(dbank, list(choices['Financial_Institution_Name']), limit=1)
        if fuzzr[0][1] >= 90:
            tdf = RssdDict[((RssdDict.Financial_Institution_Name == fuzzr[0][0]) &
                          (RssdDict.Financial_Institution_State == st))]
            BankDict.append([dbank,
                             tdf['RSSD_ID'].item(),
                             st,
                             '',
                             tdf['Financial_Institution_Name'].item(),
                             tdf['Financial_Institution_Address'].item(),
                             ct,
                             tz])
        else:
            fuzzr_old = fuzzr
            adr = r['Address']
            fuzzr = process.extract(adr, list(choices['Financial_Institution_Address']), limit=1)
            if fuzzr[0][1] >= 90:

                tdf = RssdDict[((RssdDict.Financial_Institution_Address == fuzzr[0][0]) &
                          (RssdDict.Financial_Institution_State == st))]
                # print('ADR MATCH',tdf['Financial_Institution_Name'].item(), dbank, fuzzr[0][1])
                print('append address match')
                BankDict.append([dbank,
                                 tdf['RSSD_ID'].item(),
                                 st,
                                 '',
                                 tdf['Financial_Institution_Name'].item(),
                                 tdf['Financial_Institution_Address'].item(),
                                 ct,
                                 tz])

            else:
                if fuzzr_old[0][1]>fuzzr[0][1]:
                    print('BAD MATCH FAVOR NAME', dbank, fuzzr_old, choiceStat)
                else:
                    print('BAD MATCH FAVOR ADDR', dbank, fuzzr,tdf['Financial_Institution_Name'].item(), choiceStat)

    wsio.WriteDataFrame(direc + direcDest, pd.DataFrame(BankDict,columns=['Bank',
                                               'RSSD_ID',
                                               'State',
                                               'Class',
                                               'FDIC_Name',
                                               'Address',
                                               'City',
                                               'Zip']).applymap(str))

# Match Dad Bank Name to FDIC
def MatchBankNames(input_file=direc + 'Bank_Dim.csv'):
    datat = {'RSSD_ID':str,
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
             'Last_Date/Time_Submission_Updated_On': dt.datetime}
    RssdDict = wsio.ReadCSV(direc + 'RSSD_Dict', dtype = datat)
    if input_file == '':
        print('Please supply input file')
        return
    datat = {'Bank':str,
          'RSSD_ID':str,
          'State': str,
          'Class':str,
          'FDIC_Name': str,
          'Address': str,
          'City': str,
          'Zip': str}
    ib = wsio.ReadCSV(direc + input_file, dtype = datat)
    ib = ib[ib.RSSD_ID.isnull()]
    if len(ib) == 0:
        return 'All banks have RSSD_ID Match'
    # Make zip code 5 digits
    for i, r in RssdDict.iterrows():
        zp = str(r['Financial_Institution_Zip_Code'])
        if len(zp) > 5:
            RssdDict.loc[i,['Financial_Institution_Zip_Code']] = int(zp[:5])
    return

    # Set up Bank Name lookup
    BankDict = []
    cnt = 0
    for i, r in ib.iterrows():
        cnt = cnt+1
        dbank = r['Bank']
        tz = r['Zip'].strip()
        st = r['State'].strip().upper()
        ct = r['City'].strip().upper()
        choices = RssdDict[RssdDict.Financial_Institution_Zip_Code == tz]
        choiceStat='zip'
        if len(choices) == 0:
            choiceStat = 'CityState'
            # print('BAD ZIP', zp, ct, st)
            choices = RssdDict[((RssdDict.Financial_Institution_State == st) &
                                (RssdDict.Financial_Institution_City == ct))]
            if len(choices) == 0:
                # print('BAD CITY STATE')
                choiceStat = 'State'
                choices = RssdDict[RssdDict.Financial_Institution_State == st]

        fuzzr = process.extract(dbank, list(choices['Financial_Institution_Name']), limit=1)
        if fuzzr[0][1] >= 90:
            tdf = RssdDict[((RssdDict.Financial_Institution_Name == fuzzr[0][0]) &
                          (RssdDict.Financial_Institution_State == st))]
            BankDict.append([dbank,
                             tdf['RSSD_ID'].item(),
                             st,
                             r['Class'],
                             tdf['Financial_Institution_Name'].item(),
                             tdf['Financial_Institution_Address'].item(),
                             ct,
                             tz])
        else:
            fuzzr_old = fuzzr
            adr = r['Address']
            fuzzr = process.extract(adr, list(choices['Financial_Institution_Address']), limit=1)
            if fuzzr[0][1] >= 90:

                tdf = RssdDict[((RssdDict.Financial_Institution_Address == fuzzr[0][0]) &
                          (RssdDict.Financial_Institution_State == st))]
                # print('ADR MATCH',tdf['Financial_Institution_Name'].item(), dbank, fuzzr[0][1])
                print('append address match')
                BankDict.append([dbank,
                                 tdf['RSSD_ID'].item(),
                                 st,
                                 r['Class'],
                                 tdf['Financial_Institution_Name'].item(),
                                 tdf['Financial_Institution_Address'].item(),
                                 ct,
                                 tz])

            else:
                if fuzzr_old[0][1]>fuzzr[0][1]:
                    print('BAD MATCH FAVOR NAME', dbank, fuzzr_old, choiceStat)
                else:
                    print('BAD MATCH FAVOR ADDR', dbank, fuzzr,tdf['Financial_Institution_Name'].item(), choiceStat)


    ib = wsio.ReadCSV(direc + input_file, dtype = datat)
    ib = ib[~ib.RSSD_ID.isnull()]
    rdlkp = pd.concat([pd.DataFrame(BankDict,columns=['Bank',
                                               'RSSD_ID',
                                               'State',
                                               'Class',
                                               'FDIC_Name',
                                               'Address',
                                               'City',
                                               'Zip']).applymap(str),ib]).reset_index()
    rd_lkp = rdlkp['Bank','RSSD_ID','State','Class','FDIC_Name','Address','City','Zip']

    # Check for Missing FDIC Names if RSSD_ID exists
    for i, r in rd_lkp.iterrows():
        if pd.isnull(r['FDIC_Name']):
            tdf = RssdDict[RssdDict.RSSD_ID == str(r['RSSD_ID'])]
            rd_lkp.loc[i,'FDIC_Name'] = tdf['Financial_Institution_Name'].item()
    wsio.WriteDataFrame(direc+'Missing_FDIC_Name',rd_lkp)
    return rd_lkp

# Initiate FFIEC SOAP Client
def DownloadCallReports(ftype=[]):
    if len(ftype) == 0:
        print('Please Select a file type to Download PDF or XBRL')
        return
    else:
        for i in ftype:
            ftype[ftype.index(i)] = i.upper()
            if i not in ['PDF','XBRL']:
                ftype.remove(i)
                print(i ,' is not a valid response')
    rd_lkp = wsio.ReadCSV(direc+'RSSD_Dict')
    bnk_dim = wsio.ReadCSV(direc+'Bank_Dim')
    wsdl = 'https://cdr.ffiec.gov/public/pws/webservices/retrievalservice.asmx?WSDL'
    client = Client(wsdl=wsdl, wsse=UsernameToken(ffiec_un,ffiec_pw))
    dlist = client.service.RetrieveReportingPeriods(dataSeries='Call')
    errorList = []

    ext = 'BulkReports/'

    if len(bnk_dim[pd.isnull(bnk_dim.RSSD_ID)]) != 0:
        bnk_dim = MatchBankNames(direc+'Bank_Dim.csv')

    rssd = bnk_dim['RSSD_ID'].drop_duplicates()

    for rssdid in rssd:
        print(rssdid)
        try:
            bkn = rd_lkp[rd_lkp.RSSD_ID == rssdid]['Financial_Institution_Name'].item()
        except:
            print('Invalid RSSD_ID or Multiple references in RSSD_DICT')
        fbkn = re.sub('[.,-/\&]','',bkn)
        locf = []
        for i in ftype:
            for f in glob.glob(direc + ext + fbkn + '/*.' + i):
                locf.append(f)

        # check for existing folder and popuplation

        if len(dlist) == len(locf):
            print('skipped folder', fbkn)
            continue
        elif not os.path.exists(direc + ext + fbkn + '_' + str(rssdid)):
            os.makedirs(direc + ext + fbkn + '_' + str(rssdid))
        fbkn = fbkn + '_' + str(rssdid)
        for d in dlist:
            fd = re.sub('[ -//]','',d)
            fd=fd[len(fd)-4:]+fd[:len(fd)-4]
            if len(fd) < 8:
                fd = fd[:4] + '0' + fd[4:]
            for ft in ftype:
                fname = direc + ext  + fbkn + '/' + fbkn + '_' + fd + '.' + ft

                if fname in locf:
                     print('skipped date', fbkn, d)
                     continue
                print('try',rssdid, fbkn)


                try:
                    response = client.service.RetrieveFacsimile(dataSeries = 'Call',
                                                                reportingPeriodEndDate=d,
                                                                fiIDType='ID_RSSD',
                                                                fiID = rssdid,
                                                                facsimileFormat = ft)
                except:
                    errorList.append([fbkn, d, rssdid])
                    continue

                with open(fname, 'wb') as f:
                    f.write(response)
                print('DOWNLOAD', fbkn, rssdid,fd)
    print(errorList)
    return


def ParseXBRL(fpath):
    # Parse XML
    tree  = ET.parse(fpath)
    root = tree.getroot()

    rv = []
    for child in root:
        #print(child.tag, child.attrib)
        code = child.tag[child.tag.find('}')+1:]
        if len(code) == 8:
            rv.append([code, child.text])

    rdf = pd.DataFrame(rv, columns = ['MDRM_Item','Value'])
    cd = wsio.ReadCSV(direc+'MDRM_Dict')
    tot=rdf.set_index('MDRM_Item').join(cd.set_index('MDRM_Item'))
    #print(len(tot[pd.isnull(tot.Item_Name)]))
    tot.reset_index(level=0, inplace=True)
    return tot


def RenameFolders():
    ext = 'BulkReports/'
    #Rename folders
    for f in glob.glob(direc+ext+'*'):
        bnk = f[len(direc+ext):]

        if '_' in bnk:
            print(bnk, 'No Rename')
            tdf = bnk_d[bnk_d.FDIC_Name == bnk[:bnk.rfind('_')]]
        else:
            print(bnk, 'Rename')
            tdf = bnk_d[bnk_d.FDIC_Name == bnk]
        trssd = tdf['RSSD_ID'].item()
        if not str(trssd) in f:
            os.rename(f, f + '_' + str(trssd))
    return


def GenBankMaster(direc = direc):
    ext = 'BulkReports/'
    for f in glob.glob(direc+ext+'*'):
        bfn = f[len(direc+ext):]
        bn = bfn[:bfn.rfind('_')]
        rssd = bfn[bfn.rfind('_')+1:]
        fname = f + '/' + rssd + '_master'
        if os.path.isfile(fname):
            continue
        for cr in glob.glob(direc+ext+bfn+'/*.XBRL'):
            d = cr[cr.rfind('_')+1:len(cr)-5]
            d = dt.datetime.strptime(d, '%Y%m%d').date()
            print(d, bn, rssd)
            tdf = ParseXBRL(cr)
            #print(tdf)
            tdf['ReportPeriodEndDate']=d
            tdf['RSSD_ID'] = rssd
            tdf = tdf[['ReportPeriodEndDate','RSSD_ID','Item_Name','MDRM_Item','Confidential','Value']]
            try:
                fdf = pd.concat([fdf,tdf])
            except NameError:
                fdf = tdf
        print(fname)
        wsio.WriteDataFrame(fname, fdf)
        del fdf
    return


def GenCallMaster(direc = direc):
    ext = 'BulkReports/'
    print(direc + ext + '*')
    for fol in glob.glob(direc+ext+'*'):
        print(fol)
        rssd = fol[fol.rfind('_')+1:]
        print(rssd)
        for fi in glob.glob(fol+'/*_master.csv'):
            tdf = wsio.ReadCSV(fi)
            tdf['RSSD_ID'] = rssd
        try:
            fdf = pd.concat([fdf,tdf])
        except NameError:
            fdf = tdf
    wsio.WriteDataFrame(direc+'MasterCall.csv', fdf)
    return

