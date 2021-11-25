import os
import glob
# import FDIC.fake as wsio
import time
import FDIC.wsio as wsio
import FDIC.ETL as ETL
from FDIC.constants import direc
import shutil
import pandas as pd


# Removes a list of line items from Master call for all banks
# Used if a calculation is updated and needs refreshing in MasterCall.csv
def ClearCache(li):
    if len(li) == 0:
        print('No Cache Cleared')
        return

    mc = wsio.ReadCSV(direc + 'MasterCall.csv')
    nmc = mc[~mc.MDRM_Item.isin(li)]
    wsio.WriteDataFrame(direc + 'MasterCall.csv', nmc)
    print('Cache Cleared:', li)
    return


# Fills MasterCall.csv for a specific bank
# Every bank will have all calculations in cluded in ExhibitCalcs.csv
def FillMaster(rssd):
    # Aggregator
    def CalcBreakDown(li, ulist, tlist):
        li = li.split(' ')
        for i in li[::2]:
            if 'LINE' in i:
                nli = Ecalcs[Ecalcs.MDRM_Item == i[i.index('L'):]]['Calculation'].item()
                nli, tlist = CalcBreakDown(nli, ulist, tlist)
            else:
                ulist.append(i)
        return li, ulist

    def FillLine(rssd, d, msli_calc, msli):
        val = 0
        # print('MSLI_CALC: ', msli, msli_calc)

        for i in msli_calc:
            tval = d[d.MDRM_Item == i]

            if len(tval) >= 2:
                # print('duplicate entries taking the top')
                tval = int(tval['Value'].head(1).item())
            elif len(tval) == 1:
                tval = int(tval['Value'].item())
            elif len(tval) == 0:
                # print('No entry assuming 0')
                tval = 0

            if i[:1] == '-':
                tval = -1 * int(tval)
            val = val + tval

        rlist = [d['ReportPeriodEndDate'].head(1).item(),
                 rssd,
                 '',
                 msli,
                 'No',
                 val]
        return rlist

    mc = wsio.ReadCSV(direc + 'MasterCall.csv')

    bmc = mc[mc.RSSD_ID == rssd]
    custrows = []
    Ecalcs = wsio.ReadCSV(direc + 'ExhibitCalcs.csv')

    for i, r in Ecalcs.iterrows():
        msli = r.MDRM_Item
        print(msli)
        t, msli_cal = CalcBreakDown(msli, ulist=[], tlist=[])
        bmcd = bmc.ReportPeriodEndDate.unique()
        for d in bmcd:
            bmcdf = bmc[bmc.ReportPeriodEndDate == d]
            if len(bmcdf[bmcdf.MDRM_Item == msli]) == 1:
                break
            elif len(bmcdf[bmcdf.MDRM_Item == msli]) == 0:
                nlines = FillLine(rssd, bmcdf, msli_cal, msli)
                custrows.append(nlines)

    return custrows


# Updates existing banks in MasterCall and new banks from Bank_Dim
# cc allows for clearing calculations that have been updated in ExhibitCalc
def UpdateCalls(cc=[]):
    mc = wsio.ReadCSV(direc + 'MasterCall.csv')
    bn_dim = wsio.ReadCSV(direc + 'Bank_Dim.csv')

    ClearCache(cc)
    fmast = []

    rssd_ulist = mc['RSSD_ID'].drop_duplicates()
    t_ulist = []
    for i in bn_dim['RSSD_ID'].drop_duplicates():
        if i not in rssd_ulist:
            t_ulist.append(i)
    if len(t_ulist) != 0:
        ETL.DownloadCallReports(['XBRL'])

    for rssd in rssd_ulist:
        print('START: ', rssd)
        fmast = fmast + FillMaster(rssd)
        print('FINISH: ', len(fmast))
    print(len(fmast))
    wsio.WriteDataFrame(direc + 'MasterCall.csv', fmast, append=True)
    return fmast


# Generates Master Exhibit Reference for easy Excel INDEX(MATCH()) Overwrites MasterExhibit every time
def GenMasterExhibitReference():
    mc = wsio.ReadCSV(direc + 'MasterCall.csv')
    bd = wsio.ReadCSV(direc + 'Bank_Dim.csv')
    ndf = mc[mc.MDRM_Item.str.contains('LINE')]
    ndf = pd.merge(ndf, bd, how='inner', on='RSSD_ID')
    ndf = ndf[['ReportPeriodEndDate', 'RSSD_ID', 'FDIC_Name', 'State', 'City', 'MDRM_Item', 'Value']]
    wsio.WriteDataFrame(direc + 'MasterExhibit.csv', ndf)
    return
