import shutil
import os
import os.path
import dateutil
import pandas as pd
import csv
from FDIC.constants import direc


def RemoveTrailingCommas(f=''):
    shutil.copyfile(f, f + '_bkp')
    filein = open(f)
    filedata = filein.read()
    filein.close()
    filedata = filedata.replace(', ', ' ')
    filedata = filedata.replace(' ,', ' ')
    os.remove(f)
    fout = open(f, 'w')
    fout.write(filedata)
    fout.close()
    return


def ReadCSV(f='', dt=False, dt_list=[], trail_comma=False, dtype=None):
    def RemoveDubSpace(s):
        print('Remove DubSpace')
        s = s.replace('  ', ' ')
        s = s.replace(',,', ',')
        if '  ' in s or ',,' in s:
            return RemoveDubSpace(s)
        else:
            return s

    # Check for .csv
    ext = f[len(f) - 4:].lower()
    if ext != '.csv':
        f = f + '.csv'


    if trail_comma is True:
        RemoveTrailingCommas(f)
    try:
        df = pd.read_csv(f, index_col=False, quotechar='"', dtype=dtype)
    except:
        print('possible bad row count, use trail_comma=True to remove all commas with a space on either side.')
        RemoveTrailingCommas(f)
        df = pd.read_csv(f, index_col=False, quotechar='"', dtype=dtype)

    if len(dt_list) > 0:
        for r in df.columns:
            if r in dt_list:
                df[r] = [ParseDate(date) for date in df[r]]

    return df


def WriteDataFrame(f, data, append=False):
    if ".csv" not in f:
        f = f + ".csv"

    # if "e:" not in f:
    #     f = direc + f

    def SepByComma(data):
        if type(data) == pd.DataFrame:
            data = data.values.tolist()
        clist = []
        for i in data:
            tmp_r = ",".join(map(str, i))
            clist.append([tmp_r])

        return clist

    # This append writes each character in an individual cell
    if not os.path.isfile(f):
        append = False
    if append is True:
        write_file = open(f, 'a')
        data = SepByComma(data)
        for i in data:
            # write_file.write(i[0]+'\n')
            write_file.writelines(i[0])
        write_file.close()
    else:
        if type(data) == pd.DataFrame:
            data.to_csv(f, mode='w+', sep=',',
                        index_label=None, index=False, line_terminator='\n')
        # Records a list of variables to a CSV file
        else:
            import csv
            # with open(f, 'w+', newline='\n') as csvfile:
            with open(f, 'w+', newline='\n') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',', newline='\n')
                for i in data:
                    spamwriter.writerow(i)
    return
