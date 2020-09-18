# module for indicator
import indicator

# modules for raw_data_directory function
from tkinter import filedialog
from tkinter import *

# modules for instruments functions
import sys, os
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime

def resource_path(relative_path):
    """ Get the absolute path to the resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def r_metadata():
    with open(resource_path(".metadata.dat"), 'r') as fout:
        all_metadata = pd.read_csv(fout, sep=",", header=0, index_col=0)
        return all_metadata

def w_metadata(all_metadata):
    with open(resource_path(".metadata.dat"), 'w') as fout:
        all_metadata.to_csv(fout,sep=',',encoding='utf-8', header=True)
        return all_metadata

def raw_data_directory():
    root = Tk()
    root.withdraw()
    folder_selected = filedialog.askdirectory()
    return folder_selected

def include_entire_year(full_data_sorted):
    init_year=(pd.DataFrame(full_data_sorted.loc[full_data_sorted.index[0]])).transpose(copy=True)
    init_year.index=[pd.to_datetime("00:00 01-01-18",format='%H:%M %m-%d-%y')]
    init_year.index=init_year.index.rename('Time (UTC)')
    init_year.loc[pd.to_datetime("00:00 01-01-18",format='%H:%M %m-%d-%y'),init_year.columns]=np.NaN

    end_year=(pd.DataFrame(full_data_sorted.loc[full_data_sorted.index[0]])).transpose(copy=True)
    end_year.index=[pd.to_datetime("00:00 12-31-18",format='%H:%M %m-%d-%y')]
    end_year.index=end_year.index.rename('Time (UTC)')
    end_year.loc[pd.to_datetime("00:00 12-31-18",format='%H:%M %m-%d-%y'),end_year.columns]=np.NaN

    return(pd.concat([init_year,full_data_sorted,end_year]))

def CPC(folder_selected):

    # Finding all files with .csv extensions in the selected folder
    PATH = folder_selected
    EXT = "*.csv"
    files = [file for path, subdir, files in os.walk(PATH)
                      for file in glob(os.path.join(path, EXT))]

    # reading and concatenating all data from all files
    full_data = pd.read_csv(files[0], sep=",",usecols=[1,2], names=["Time (UTC)", 'Conc'])
    print('Total files found: ' + str(len(files)))
    for i in range(1, len(files)):
        full_data_i = pd.read_csv(files[i], sep=",",usecols=[1,2], names=["Time (UTC)", 'Conc'])
        full_data=pd.concat([full_data, full_data_i])
        print('File read: ' + str(i+1), end="\r")
    print("F")
    print(" ")

    print("Processing Data... ")
    with indicator.Running():

        # excluding file reading errors
        full_data = full_data.dropna()
        full_data = full_data.drop_duplicates()

        # transforming the Time (UTC) for datetime object
        full_data["Time (UTC)"] = pd.to_datetime(full_data["Time (UTC)"],format='%a %Y/%m/%d %H:%M:%S')
        full_data = full_data.set_index(pd.DatetimeIndex(full_data["Time (UTC)"]))
        full_data=full_data.drop(columns=["Time (UTC)"])

        # time sorting the file
        full_data_sorted = full_data.sort_index()

        # number of lines in the initial data
        data_init = len(full_data_sorted)

        # applying the level 1 filters
        full_data_sorted = full_data_sorted[full_data_sorted.Conc < 10000] #exclui linhas que registram contagem superior a 10mil
        full_data_sorted = full_data_sorted[full_data_sorted.Conc > 50] #exclui linhas que registram contagem inferior a 50

        # number of lines in the processed data
        data_processed = len(full_data_sorted)

        # including the entire year period
        full_data_sorted=include_entire_year(full_data_sorted)

        # Resample to 5min, creating the final dataframe columns
        full_data_5min_mean = full_data_sorted.resample('5min').mean()
        full_data_5min_std = full_data_sorted.resample('5min').std()
        full_data_5min_median = full_data_sorted.resample('5min').median()
        full_data_5min_max = full_data_sorted.resample('5min').max()
        full_data_5min_min = full_data_sorted.resample('5min').min()
        full_data_5min_p25 = full_data_sorted.resample('5min').quantile(q=0.25)
        full_data_5min_p75 = full_data_sorted.resample('5min').quantile(q=0.75)
        full_data_5min_count = full_data_sorted.resample('5min').count()

        # creating the Unix Timestamp column for 5min
        full_data_5min_mean['Timestamp']=full_data_5min_mean.index
        full_data_5min_mean['Timestamp']=(full_data_5min_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating the final data dataframe with 5min resolution
        full_data_5min=pd.concat([full_data_5min_mean.Timestamp, full_data_5min_mean.Conc, full_data_5min_std.Conc, full_data_5min_median.Conc, full_data_5min_p25.Conc, full_data_5min_p75.Conc, full_data_5min_min.Conc, full_data_5min_max.Conc, full_data_5min_count.Conc],axis=1)
        full_data_5min.columns = ['Unix Time (UTC)', 'Total concentration Mean (cm-3)', 'Total concentration std (cm-3)', 'Total concentration Median (cm-3)', 'Total concentration p25 (cm-3)', 'Total concentration p75 (cm-3)',  'Total concentration Min (cm-3)', ' Total concentration Max (cm-3)', '# of samples']


        # Resample to 30min, creating the final dataframe columns
        full_data_30min_mean = full_data_sorted.resample('30min').mean()
        full_data_30min_std = full_data_sorted.resample('30min').std()
        full_data_30min_median = full_data_sorted.resample('30min').median()
        full_data_30min_max = full_data_sorted.resample('30min').max()
        full_data_30min_min = full_data_sorted.resample('30min').min()
        full_data_30min_p25 = full_data_sorted.resample('30min').quantile(q=0.25)
        full_data_30min_p75 = full_data_sorted.resample('30min').quantile(q=0.75)
        full_data_30min_count = full_data_sorted.resample('30min').count()

        # creating the Unix Timestamp column for 30min
        full_data_30min_mean['Timestamp']=full_data_30min_mean.index
        full_data_30min_mean['Timestamp']=(full_data_30min_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating the final data dataframe with 30min resolution
        full_data_30min=pd.concat([full_data_30min_mean.Timestamp, full_data_30min_mean.Conc, full_data_30min_std.Conc, full_data_30min_median.Conc, full_data_30min_min.Conc, full_data_30min_max.Conc],axis=1)
        full_data_30min.columns = ['Unix Time (UTC)', 'Total concentration Mean (cm-3)', 'Total concentration std (cm-3)', 'Total concentration Median (cm-3)', 'Total concentration Min (cm-3)', ' Total concentration Max (cm-3)']

        # creating the time metadata
        time_now = datetime.now()

        # reading the metadata file
        all_metadata=r_metadata()
        instrument='CPC'
        site= str(all_metadata[instrument][0])
        brand= str(all_metadata[instrument][2])
        model= str(all_metadata[instrument][3])
        serial_num= str(all_metadata[instrument][4])
        val_level= str(all_metadata[instrument][5])
        proc_vers= str(all_metadata[instrument][6])

        # generating data removed metadata
        data_removed= str((1-(data_processed/data_init))*100)

        # generating metadata string
        str_metadata = 'File generated on ' + str(time_now) + ', Level:' + val_level + ', Processing Version: ' + proc_vers + ', Amount of raw data removed: ' + data_removed + '%' + ', Site: ' + site + ', Instrument: ' + instrument + ', Model: ' + model + ', S/N: ' + serial_num + ', Data processing details can be found in ftp://ftp.lfa.if.usp.br/LFA_Processed_Data/DataProcessing_GoAmazon_LFA.pdf\n'
        metadata=pd.Series([(str_metadata)])

        # generating the filename
        str_filename_5min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '5min.csv'
        str_filename_30min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '30min.csv'

        # writing the file to 5min
        with open(str_filename_5min, 'w') as fout:
            fout.write(str_metadata)
        full_data_5min.to_csv(str_filename_5min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

        # writing the file to 30min
        with open(str_filename_30min, 'w') as fout:
            fout.write(str_metadata)
        full_data_30min.to_csv(str_filename_30min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

def Termo49i(folder_selected):

    # Finding all files with .csv extensions in the selected folder
    PATH = folder_selected
    EXT = "*.dat"
    files = [file for path, subdir, files in os.walk(PATH)
                      for file in glob(os.path.join(path, EXT))]

    # reading and concatenating all data from all files
    full_data = pd.read_csv(files[0], sep="\s* \s*", header=5, usecols=[0,1,3,5,6,10,11], engine='python')
    print('Total files found: ' + str(len(files)))
    for i in range(1, len(files)):
        full_data_i = pd.read_csv(files[i], sep="\s* \s*", header=5, usecols=[0,1,3,5,6,10,11], engine='python')
        full_data=pd.concat([full_data, full_data_i])
        print('File read: ' + str(i+1), end="\r")
    print("F")
    print(" ")


    print("Processing Data... ")
    with indicator.Running():
        # excluding file reading errors
        full_data = full_data.dropna()
        full_data = full_data.drop_duplicates()

        # transforming the Time (UTC) for datetime object
        full_data["Time (UTC)"]=full_data["Time"]+" "+full_data["Date"]
        full_data["Time (UTC)"] = pd.to_datetime(full_data["Time (UTC)"],format='%H:%M %m-%d-%y')
        full_data = full_data.set_index(pd.DatetimeIndex(full_data['Time (UTC)']))
        full_data=full_data.drop(columns=['Time','Date','Time (UTC)'])

        # time sorting the file
        full_data_sorted = full_data.sort_index()

        # number of lines in the initial data
        data_init = len(full_data_sorted)

        # applying the level 1 filters
        full_data_sorted = full_data_sorted[full_data_sorted.flowa > 0.55]
        full_data_sorted = full_data_sorted[full_data_sorted.flowb > 0.55]
        full_data_sorted = full_data_sorted[full_data_sorted.cellai > 60000]
        full_data_sorted = full_data_sorted[full_data_sorted.cellbi > 60000]
        full_data_sorted = full_data_sorted[full_data_sorted.o3 > -50]
        full_data_sorted = full_data_sorted[full_data_sorted.o3 < 1000]

        # number of lines in the processed data
        data_processed = len(full_data_sorted)
        
        # including the entire year period
        full_data_sorted=include_entire_year(full_data_sorted)

        # Resample to 5min, creating the final dataframe columns
        full_data_5min_mean = full_data_sorted.o3.resample('5min').mean()
        full_data_5min_mean = full_data_5min_mean.to_frame()

        full_data_5min_std = full_data_sorted.o3.resample('5min').std()
        full_data_5min_std = full_data_5min_std.to_frame()

        full_data_5min_median = full_data_sorted.o3.resample('5min').median()
        full_data_5min_median = full_data_5min_median.to_frame()

        full_data_5min_max = full_data_sorted.o3.resample('5min').max()
        full_data_5min_max = full_data_5min_max.to_frame()

        full_data_5min_min = full_data_sorted.o3.resample('5min').min()
        full_data_5min_min = full_data_5min_min.to_frame()

        full_data_5min_p25 = full_data_sorted.o3.resample('5min').quantile(q=0.25)
        full_data_5min_p25 = full_data_5min_p25.to_frame()

        full_data_5min_p75 = full_data_sorted.o3.resample('5min').quantile(q=0.75)
        full_data_5min_p75 = full_data_5min_p75.to_frame()

        full_data_5min_count = full_data_sorted.o3.resample('5min').count()
        full_data_5min_count = full_data_5min_count.to_frame()

        # creating the Unix Timestamp column for 5min
        full_data_5min_mean['Timestamp']=full_data_5min_mean.index
        full_data_5min_mean['Timestamp']=(full_data_5min_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating the final data dataframe with 5min resolution
        full_data_5min=pd.concat([full_data_5min_mean.Timestamp, full_data_5min_mean.o3, full_data_5min_std.o3, full_data_5min_median.o3, full_data_5min_p25.o3, full_data_5min_p75.o3, full_data_5min_min.o3, full_data_5min_max.o3, full_data_5min_count.o3],axis=1)
        full_data_5min.columns = ['Unix Time (UTC)', 'O3 Mean (ppb)', 'O3 std (ppb)', 'O3 Median (ppb)', ' O3 p25 (ppb)', ' O3 p75 (ppb)','O3 Min (ppb)', 'O3 Max (ppb)', '# of samples']


        # Resample to 30min, creating the final dataframe columns
        full_data_30min_mean = full_data_sorted.o3.resample('30min').mean()
        full_data_30min_mean = full_data_30min_mean.to_frame()

        full_data_30min_std = full_data_sorted.o3.resample('30min').std()
        full_data_30min_std = full_data_30min_std.to_frame()

        full_data_30min_median = full_data_sorted.o3.resample('30min').median()
        full_data_30min_median = full_data_30min_median.to_frame()

        full_data_30min_max = full_data_sorted.o3.resample('30min').max()
        full_data_30min_max = full_data_30min_max.to_frame()

        full_data_30min_min = full_data_sorted.o3.resample('30min').min()
        full_data_30min_min = full_data_30min_min.to_frame()

        full_data_30min_p25 = full_data_sorted.o3.resample('30min').quantile(q=0.25)
        full_data_30min_p25 = full_data_30min_p25.to_frame()

        full_data_30min_p75 = full_data_sorted.o3.resample('30min').quantile(q=0.75)
        full_data_30min_p75 = full_data_30min_p75.to_frame()

        full_data_30min_count = full_data_sorted.o3.resample('30min').count()
        full_data_30min_count = full_data_30min_count.to_frame()

        # creating the Unix Timestamp column for 30min
        full_data_30min_mean['Timestamp']=full_data_30min_mean.index
        full_data_30min_mean['Timestamp']=(full_data_30min_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating the final data dataframe with 30min resolution
        full_data_30min=pd.concat([full_data_30min_mean.Timestamp, full_data_30min_mean.o3, full_data_30min_std.o3, full_data_30min_median.o3, full_data_30min_p25.o3,  full_data_30min_p75.o3,full_data_30min_min.o3, full_data_30min_max.o3, full_data_30min_count.o3],axis=1)
        full_data_30min.columns = ['Unix Time (UTC)', 'O3 Mean (ppb)', 'O3 std (ppb)', 'O3 Median (ppb)', ' O3 p25 (ppb)', ' O3 p75 (ppb)','O3 Min (ppb)', 'O3 Max (ppb)', '# of samples']

        # creating the time metadata
        time_now = datetime.now()

        # reading the metadata file
        all_metadata=r_metadata()
        instrument='Termo49i'
        site= str(all_metadata[instrument][0])
        brand= str(all_metadata[instrument][2])
        model= str(all_metadata[instrument][3])
        serial_num= str(all_metadata[instrument][4])
        val_level= str(all_metadata[instrument][5])
        proc_vers= str(all_metadata[instrument][6])

        data_removed= str((1-(data_processed/data_init))*100)

        str_metadata = 'File generated on ' + str(time_now) + ', Level:' + val_level + ', Processing Version: ' + proc_vers + ', Amount of raw data removed: ' + data_removed + '%' + ', Site: ' + site + ', Instrument: ' + instrument + ', Model: ' + model + ', S/N: ' + serial_num + ', Data processing details can be found in ftp://ftp.lfa.if.usp.br/LFA_Processed_Data/DataProcessing_GoAmazon_LFA.pdf\n'

        # generating the filename
        str_filename_5min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '5min.csv'
        str_filename_30min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '30min.csv'

        # writing the file to 5min
        with open(str_filename_5min, 'w') as fout:
            fout.write(str_metadata)
        full_data_5min.to_csv(str_filename_5min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

        # writing the file to 30min
        with open(str_filename_30min, 'w') as fout:
            fout.write(str_metadata)
        full_data_30min.to_csv(str_filename_30min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

def AE33(folder_selected):

    # Finding all files with .csv extensions in the selected folder
    PATH = folder_selected
    EXT = "*.dat"
    files = [file for path, subdir, files in os.walk(PATH)
                      for file in glob(os.path.join(path, EXT))]

    # reading and concatenating all data from all files
    full_data = pd.read_csv(files[0], sep="\s* \s*", skiprows=8, usecols=[0,1,26,32,40,43,46,49,52,55,58], names=["Date","Time","FlowC","Status","BC1","BC2","BC3","BC4","BC5","BC6","BC7"], engine='python')
    print('Total de arquivos: ' + str(len(files)))
    for i in range(1, len(files)):
        full_data_i = pd.read_csv(files[i], sep="\s* \s*", skiprows=8, usecols=[0,1,26,32,40,43,46,49,52,55,58], names=["Date","Time","FlowC","Status","BC1","BC2","BC3","BC4","BC5","BC6","BC7"], engine='python')
        full_data=pd.concat([full_data, full_data_i])
        print('File read: ' + str(i+1), end="\r")
    print("F")
    print(" ")


    print("Processing Data... ")
    with indicator.Running():
        # excluding file reading errors
        full_data = full_data.dropna()
        full_data = full_data.drop_duplicates()

        # transforming the Time (UTC) for datetime object
        full_data["Time (UTC)"]=full_data["Date"]+" "+full_data["Time"]
        full_data["Time (UTC)"] = pd.to_datetime(full_data["Time (UTC)"],format='%Y-%m-%d %H:%M:%S')
        full_data = full_data.set_index(pd.DatetimeIndex(full_data["Time (UTC)"]))
        full_data=full_data.drop(columns=['Date','Time',"Time (UTC)"])

        # time sorting the file
        full_data_sorted = full_data.sort_index()

        # number of lines in the initial data
        data_init = len(full_data_sorted)

        # applying the level 1 filters
        full_data_sorted = full_data_sorted[full_data_sorted.FlowC > 1000]
        full_data_sorted = full_data_sorted[full_data_sorted.FlowC < 8000]
        full_data_sorted = full_data_sorted[full_data_sorted.Status == 0]
        full_data_sorted = full_data_sorted[full_data_sorted.BC6 > -1000]
        full_data_sorted = full_data_sorted[full_data_sorted.BC6 < 100000]

        # number of lines in the processed data
        data_processed = len(full_data_sorted)

        # including the entire year period
        full_data_sorted=include_entire_year(full_data_sorted)

        # Resample to 5min, creating dataframe to BC370 columns
        full_data_5min_BC1_mean = full_data_sorted.BC1.resample('5min').mean()
        full_data_5min_BC1_mean = full_data_5min_BC1_mean.to_frame()

        full_data_5min_BC1_std = full_data_sorted.BC1.resample('5min').std()
        full_data_5min_BC1_std = full_data_5min_BC1_std.to_frame()

        full_data_5min_BC1_median = full_data_sorted.BC1.resample('5min').median()
        full_data_5min_BC1_median = full_data_5min_BC1_median.to_frame()

        full_data_5min_BC1_max = full_data_sorted.BC1.resample('5min').max()
        full_data_5min_BC1_max = full_data_5min_BC1_max.to_frame()

        full_data_5min_BC1_min = full_data_sorted.BC1.resample('5min').min()
        full_data_5min_BC1_min = full_data_5min_BC1_min.to_frame()

        full_data_5min_BC1_p25 = full_data_sorted.BC1.resample('5min').quantile(q=0.25)
        full_data_5min_BC1_p25 = full_data_5min_BC1_p25.to_frame()

        full_data_5min_BC1_p75 = full_data_sorted.BC1.resample('5min').quantile(q=0.75)
        full_data_5min_BC1_p75 = full_data_5min_BC1_p75.to_frame()

        # creating the Unix Timestamp column for 5min
        full_data_5min_BC1_mean['Timestamp']=full_data_5min_BC1_mean.index
        full_data_5min_BC1_mean['Timestamp']=(full_data_5min_BC1_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating BC370 dataframe with 5min resolution
        full_data_5min_BC1=pd.concat([full_data_5min_BC1_mean.Timestamp, full_data_5min_BC1_mean.BC1, full_data_5min_BC1_std.BC1, full_data_5min_BC1_median.BC1, full_data_5min_BC1_p25.BC1, full_data_5min_BC1_p75.BC1, full_data_5min_BC1_min.BC1, full_data_5min_BC1_max.BC1],axis=1)
        full_data_5min_BC1.columns = ['Unix Time (UTC)', 'BC370 Mean (ug.m-3)', 'BC370 std (ug.m-3)', 'BC370 Median (ug.m-3)', 'BC370 p25 (ug.m-3)', 'BC370 p75 (ug.m-3)', 'BC370 Min (ug.m-3)', 'BC370 Max (ug.m-3)']

        # Resample to 5min, creating dataframe to BC470 columns
        full_data_5min_BC2_mean = full_data_sorted.BC2.resample('5min').mean()
        full_data_5min_BC2_mean = full_data_5min_BC2_mean.to_frame()

        full_data_5min_BC2_std = full_data_sorted.BC2.resample('5min').std()
        full_data_5min_BC2_std = full_data_5min_BC2_std.to_frame()

        full_data_5min_BC2_median = full_data_sorted.BC2.resample('5min').median()
        full_data_5min_BC2_median = full_data_5min_BC2_median.to_frame()

        full_data_5min_BC2_max = full_data_sorted.BC2.resample('5min').max()
        full_data_5min_BC2_max = full_data_5min_BC2_max.to_frame()

        full_data_5min_BC2_min = full_data_sorted.BC2.resample('5min').min()
        full_data_5min_BC2_min = full_data_5min_BC2_min.to_frame()

        full_data_5min_BC2_p25 = full_data_sorted.BC2.resample('5min').quantile(q=0.25)
        full_data_5min_BC2_p25 = full_data_5min_BC2_p25.to_frame()

        full_data_5min_BC2_p75 = full_data_sorted.BC2.resample('5min').quantile(q=0.75)
        full_data_5min_BC2_p75 = full_data_5min_BC2_p75.to_frame()

        # generating BC470 dataframe with 5min resolution
        full_data_5min_BC2=pd.concat([full_data_5min_BC2_mean.BC2, full_data_5min_BC2_std.BC2, full_data_5min_BC2_median.BC2, full_data_5min_BC2_p25.BC2, full_data_5min_BC2_p75.BC2, full_data_5min_BC2_min.BC2, full_data_5min_BC2_max.BC2],axis=1)
        full_data_5min_BC2.columns = ['BC470 Mean (ug.m-3)', 'BC470 std (ug.m-3)', 'BC470 Median (ug.m-3)', 'BC470 p25 (ug.m-3)', 'BC470 p75 (ug.m-3)', 'BC470 Min (ug.m-3)', 'BC470 Max (ug.m-3)']


        # Resample to 5min, creating dataframe to BC520 columns
        full_data_5min_BC3_mean = full_data_sorted.BC3.resample('5min').mean()
        full_data_5min_BC3_mean = full_data_5min_BC3_mean.to_frame()

        full_data_5min_BC3_std = full_data_sorted.BC3.resample('5min').std()
        full_data_5min_BC3_std = full_data_5min_BC3_std.to_frame()

        full_data_5min_BC3_median = full_data_sorted.BC3.resample('5min').median()
        full_data_5min_BC3_median = full_data_5min_BC3_median.to_frame()

        full_data_5min_BC3_max = full_data_sorted.BC3.resample('5min').max()
        full_data_5min_BC3_max = full_data_5min_BC3_max.to_frame()

        full_data_5min_BC3_min = full_data_sorted.BC3.resample('5min').min()
        full_data_5min_BC3_min = full_data_5min_BC3_min.to_frame()

        full_data_5min_BC3_p25 = full_data_sorted.BC3.resample('5min').quantile(q=0.25)
        full_data_5min_BC3_p25 = full_data_5min_BC3_p25.to_frame()

        full_data_5min_BC3_p75 = full_data_sorted.BC3.resample('5min').quantile(q=0.75)
        full_data_5min_BC3_p75 = full_data_5min_BC3_p75.to_frame()

        # generating BC520 dataframe with 5min resolution
        full_data_5min_BC3=pd.concat([full_data_5min_BC3_mean.BC3, full_data_5min_BC3_std.BC3, full_data_5min_BC3_median.BC3, full_data_5min_BC3_p25.BC3, full_data_5min_BC3_p75.BC3, full_data_5min_BC3_min.BC3, full_data_5min_BC3_max.BC3],axis=1)
        full_data_5min_BC3.columns = ['BC520 Mean (ug.m-3)', 'BC520 std (ug.m-3)', 'BC520 Median (ug.m-3)', 'BC520 p25 (ug.m-3)', 'BC520 p75 (ug.m-3)', 'BC520 Min (ug.m-3)', 'BC520 Max (ug.m-3)']

        # Resample to 5min, creating dataframe to BC590 columns
        full_data_5min_BC4_mean = full_data_sorted.BC4.resample('5min').mean()
        full_data_5min_BC4_mean = full_data_5min_BC4_mean.to_frame()

        full_data_5min_BC4_std = full_data_sorted.BC4.resample('5min').std()
        full_data_5min_BC4_std = full_data_5min_BC4_std.to_frame()

        full_data_5min_BC4_median = full_data_sorted.BC4.resample('5min').median()
        full_data_5min_BC4_median = full_data_5min_BC4_median.to_frame()

        full_data_5min_BC4_max = full_data_sorted.BC4.resample('5min').max()
        full_data_5min_BC4_max = full_data_5min_BC4_max.to_frame()

        full_data_5min_BC4_min = full_data_sorted.BC4.resample('5min').min()
        full_data_5min_BC4_min = full_data_5min_BC4_min.to_frame()

        full_data_5min_BC4_p25 = full_data_sorted.BC4.resample('5min').quantile(q=0.25)
        full_data_5min_BC4_p25 = full_data_5min_BC4_p25.to_frame()

        full_data_5min_BC4_p75 = full_data_sorted.BC4.resample('5min').quantile(q=0.75)
        full_data_5min_BC4_p75 = full_data_5min_BC4_p75.to_frame()

        # generating BC590 dataframe with 5min resolution
        full_data_5min_BC4=pd.concat([full_data_5min_BC4_mean.BC4, full_data_5min_BC4_std.BC4, full_data_5min_BC4_median.BC4, full_data_5min_BC4_p25.BC4, full_data_5min_BC4_p75.BC4, full_data_5min_BC4_min.BC4, full_data_5min_BC4_max.BC4],axis=1)
        full_data_5min_BC4.columns = ['BC590 Mean (ug.m-3)', 'BC590 std (ug.m-3)', 'BC590 Median (ug.m-3)', 'BC590 p25 (ug.m-3)', 'BC590 p75 (ug.m-3)', 'BC590 Min (ug.m-3)', 'BC590 Max (ug.m-3)']

        # Resample to 5min, creating dataframe to BC660 columns
        full_data_5min_BC5_mean = full_data_sorted.BC5.resample('5min').mean()
        full_data_5min_BC5_mean = full_data_5min_BC5_mean.to_frame()

        full_data_5min_BC5_std = full_data_sorted.BC5.resample('5min').std()
        full_data_5min_BC5_std = full_data_5min_BC5_std.to_frame()

        full_data_5min_BC5_median = full_data_sorted.BC5.resample('5min').median()
        full_data_5min_BC5_median = full_data_5min_BC5_median.to_frame()

        full_data_5min_BC5_max = full_data_sorted.BC5.resample('5min').max()
        full_data_5min_BC5_max = full_data_5min_BC5_max.to_frame()

        full_data_5min_BC5_min = full_data_sorted.BC5.resample('5min').min()
        full_data_5min_BC5_min = full_data_5min_BC5_min.to_frame()

        full_data_5min_BC5_p25 = full_data_sorted.BC5.resample('5min').quantile(q=0.25)
        full_data_5min_BC5_p25 = full_data_5min_BC5_p25.to_frame()

        full_data_5min_BC5_p75 = full_data_sorted.BC5.resample('5min').quantile(q=0.75)
        full_data_5min_BC5_p75 = full_data_5min_BC5_p75.to_frame()

        # generating BC660 dataframe with 5min resolution
        full_data_5min_BC5=pd.concat([full_data_5min_BC5_mean.BC5, full_data_5min_BC5_std.BC5, full_data_5min_BC5_median.BC5, full_data_5min_BC5_p25.BC5, full_data_5min_BC5_p75.BC5, full_data_5min_BC5_min.BC5, full_data_5min_BC5_max.BC5],axis=1)
        full_data_5min_BC5.columns = ['BC660 Mean (ug.m-3)', 'BC660 std (ug.m-3)', 'BC660 Median (ug.m-3)', 'BC660 p25 (ug.m-3)', 'BC660 p75 (ug.m-3)', 'BC660 Min (ug.m-3)', 'BC660 Max (ug.m-3)']

        # Resample to 5min, creating dataframe to BC880 columns
        full_data_5min_BC6_mean = full_data_sorted.BC6.resample('5min').mean()
        full_data_5min_BC6_mean = full_data_5min_BC6_mean.to_frame()

        full_data_5min_BC6_std = full_data_sorted.BC6.resample('5min').std()
        full_data_5min_BC6_std = full_data_5min_BC6_std.to_frame()

        full_data_5min_BC6_median = full_data_sorted.BC6.resample('5min').median()
        full_data_5min_BC6_median = full_data_5min_BC6_median.to_frame()

        full_data_5min_BC6_max = full_data_sorted.BC6.resample('5min').max()
        full_data_5min_BC6_max = full_data_5min_BC6_max.to_frame()

        full_data_5min_BC6_min = full_data_sorted.BC6.resample('5min').min()
        full_data_5min_BC6_min = full_data_5min_BC6_min.to_frame()

        full_data_5min_BC6_p25 = full_data_sorted.BC6.resample('5min').quantile(q=0.25)
        full_data_5min_BC6_p25 = full_data_5min_BC6_p25.to_frame()

        full_data_5min_BC6_p75 = full_data_sorted.BC6.resample('5min').quantile(q=0.75)
        full_data_5min_BC6_p75 = full_data_5min_BC6_p75.to_frame()

        # generating BC880 dataframe with 5min resolution
        full_data_5min_BC6=pd.concat([full_data_5min_BC6_mean.BC6, full_data_5min_BC6_std.BC6, full_data_5min_BC6_median.BC6, full_data_5min_BC6_p25.BC6, full_data_5min_BC6_p75.BC6, full_data_5min_BC6_min.BC6, full_data_5min_BC6_max.BC6],axis=1)
        full_data_5min_BC6.columns = ['BC880 Mean (ug.m-3)', 'BC880 std (ug.m-3)', 'BC880 Median (ug.m-3)', 'BC880 p25 (ug.m-3)', 'BC880 p75 (ug.m-3)', 'BC880 Min (ug.m-3)', 'BC880 Max (ug.m-3)']

        # Resample to 5min, creating dataframe to BC950 columns
        full_data_5min_BC7_mean = full_data_sorted.BC7.resample('5min').mean()
        full_data_5min_BC7_mean = full_data_5min_BC7_mean.to_frame()

        full_data_5min_BC7_std = full_data_sorted.BC7.resample('5min').std()
        full_data_5min_BC7_std = full_data_5min_BC7_std.to_frame()

        full_data_5min_BC7_median = full_data_sorted.BC7.resample('5min').median()
        full_data_5min_BC7_median = full_data_5min_BC7_median.to_frame()

        full_data_5min_BC7_max = full_data_sorted.BC7.resample('5min').max()
        full_data_5min_BC7_max = full_data_5min_BC7_max.to_frame()

        full_data_5min_BC7_min = full_data_sorted.BC7.resample('5min').min()
        full_data_5min_BC7_min = full_data_5min_BC7_min.to_frame()

        full_data_5min_BC7_p25 = full_data_sorted.BC7.resample('5min').quantile(q=0.25)
        full_data_5min_BC7_p25 = full_data_5min_BC7_p25.to_frame()

        full_data_5min_BC7_p75 = full_data_sorted.BC7.resample('5min').quantile(q=0.75)
        full_data_5min_BC7_p75 = full_data_5min_BC7_p75.to_frame()

        full_data_5min_BC7_count = full_data_sorted.BC7.resample('5min').count()
        full_data_5min_BC7_count = full_data_5min_BC7_count.to_frame()

        # generating BC950 dataframe with 5min resolution
        full_data_5min_BC7=pd.concat([full_data_5min_BC7_mean.BC7, full_data_5min_BC7_std.BC7, full_data_5min_BC7_median.BC7, full_data_5min_BC7_p25.BC7, full_data_5min_BC7_p75.BC7, full_data_5min_BC7_min.BC7, full_data_5min_BC7_max.BC7, full_data_5min_BC7_count.BC7],axis=1)
        full_data_5min_BC7.columns = ['BC950 Mean (ug.m-3)', 'BC950 std (ug.m-3)', 'BC950 Median (ug.m-3)', 'BC950 p25 (ug.m-3)', 'BC950 p75 (ug.m-3)', 'BC950 Min (ug.m-3)', 'BC950 Max (ug.m-3)', '# of samples']

        # generating the final columns with all BC dataframes
        full_data_5min=pd.concat([full_data_5min_BC1, full_data_5min_BC2, full_data_5min_BC3, full_data_5min_BC4, full_data_5min_BC5, full_data_5min_BC6, full_data_5min_BC7], axis=1)


        # Resample to 30min, creating dataframe to BC370 columns
        full_data_30min_BC1_mean = full_data_sorted.BC1.resample('30min').mean()
        full_data_30min_BC1_mean = full_data_30min_BC1_mean.to_frame()

        full_data_30min_BC1_std = full_data_sorted.BC1.resample('30min').std()
        full_data_30min_BC1_std = full_data_30min_BC1_std.to_frame()

        full_data_30min_BC1_median = full_data_sorted.BC1.resample('30min').median()
        full_data_30min_BC1_median = full_data_30min_BC1_median.to_frame()

        full_data_30min_BC1_max = full_data_sorted.BC1.resample('30min').max()
        full_data_30min_BC1_max = full_data_30min_BC1_max.to_frame()

        full_data_30min_BC1_min = full_data_sorted.BC1.resample('30min').min()
        full_data_30min_BC1_min = full_data_30min_BC1_min.to_frame()

        full_data_30min_BC1_p25 = full_data_sorted.BC1.resample('30min').quantile(q=0.25)
        full_data_30min_BC1_p25 = full_data_30min_BC1_p25.to_frame()

        full_data_30min_BC1_p75 = full_data_sorted.BC1.resample('30min').quantile(q=0.75)
        full_data_30min_BC1_p75 = full_data_30min_BC1_p75.to_frame()

        # creating the Unix Timestamp column for 30min
        full_data_30min_BC1_mean['Timestamp']=full_data_30min_BC1_mean.index
        full_data_30min_BC1_mean['Timestamp']=(full_data_30min_BC1_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating BC370 dataframe with 30min resolution
        full_data_30min_BC1=pd.concat([full_data_30min_BC1_mean.Timestamp, full_data_30min_BC1_mean.BC1, full_data_30min_BC1_std.BC1, full_data_30min_BC1_median.BC1, full_data_30min_BC1_p25.BC1, full_data_30min_BC1_p75.BC1, full_data_30min_BC1_min.BC1, full_data_30min_BC1_max.BC1],axis=1)
        full_data_30min_BC1.columns = ['Unix Time (UTC)', 'BC370 Mean (ug.m-3)', 'BC370 std (ug.m-3)', 'BC370 Median (ug.m-3)', 'BC370 p25 (ug.m-3)', 'BC370 p75 (ug.m-3)', 'BC370 Min (ug.m-3)', 'BC370 Max (ug.m-3)']

        # Resample to 30min, creating dataframe to BC470 columns
        full_data_30min_BC2_mean = full_data_sorted.BC2.resample('30min').mean()
        full_data_30min_BC2_mean = full_data_30min_BC2_mean.to_frame()

        full_data_30min_BC2_std = full_data_sorted.BC2.resample('30min').std()
        full_data_30min_BC2_std = full_data_30min_BC2_std.to_frame()

        full_data_30min_BC2_median = full_data_sorted.BC2.resample('30min').median()
        full_data_30min_BC2_median = full_data_30min_BC2_median.to_frame()

        full_data_30min_BC2_max = full_data_sorted.BC2.resample('30min').max()
        full_data_30min_BC2_max = full_data_30min_BC2_max.to_frame()

        full_data_30min_BC2_min = full_data_sorted.BC2.resample('30min').min()
        full_data_30min_BC2_min = full_data_30min_BC2_min.to_frame()

        full_data_30min_BC2_p25 = full_data_sorted.BC2.resample('30min').quantile(q=0.25)
        full_data_30min_BC2_p25 = full_data_30min_BC2_p25.to_frame()

        full_data_30min_BC2_p75 = full_data_sorted.BC2.resample('30min').quantile(q=0.75)
        full_data_30min_BC2_p75 = full_data_30min_BC2_p75.to_frame()

        # generating BC470 dataframe with 30min resolution
        full_data_30min_BC2=pd.concat([full_data_30min_BC2_mean.BC2, full_data_30min_BC2_std.BC2, full_data_30min_BC2_median.BC2, full_data_30min_BC2_p25.BC2, full_data_30min_BC2_p75.BC2, full_data_30min_BC2_min.BC2, full_data_30min_BC2_max.BC2],axis=1)
        full_data_30min_BC2.columns = ['BC470 Mean (ug.m-3)', 'BC470 std (ug.m-3)', 'BC470 Median (ug.m-3)', 'BC470 p25 (ug.m-3)', 'BC470 p75 (ug.m-3)', 'BC470 Min (ug.m-3)', 'BC470 Max (ug.m-3)']


        # Resample to 30min, creating dataframe to BC520 columns
        full_data_30min_BC3_mean = full_data_sorted.BC3.resample('30min').mean()
        full_data_30min_BC3_mean = full_data_30min_BC3_mean.to_frame()

        full_data_30min_BC3_std = full_data_sorted.BC3.resample('30min').std()
        full_data_30min_BC3_std = full_data_30min_BC3_std.to_frame()

        full_data_30min_BC3_median = full_data_sorted.BC3.resample('30min').median()
        full_data_30min_BC3_median = full_data_30min_BC3_median.to_frame()

        full_data_30min_BC3_max = full_data_sorted.BC3.resample('30min').max()
        full_data_30min_BC3_max = full_data_30min_BC3_max.to_frame()

        full_data_30min_BC3_min = full_data_sorted.BC3.resample('30min').min()
        full_data_30min_BC3_min = full_data_30min_BC3_min.to_frame()

        full_data_30min_BC3_p25 = full_data_sorted.BC3.resample('30min').quantile(q=0.25)
        full_data_30min_BC3_p25 = full_data_30min_BC3_p25.to_frame()

        full_data_30min_BC3_p75 = full_data_sorted.BC3.resample('30min').quantile(q=0.75)
        full_data_30min_BC3_p75 = full_data_30min_BC3_p75.to_frame()

        # generating BC520 dataframe with 30min resolution
        full_data_30min_BC3=pd.concat([full_data_30min_BC3_mean.BC3, full_data_30min_BC3_std.BC3, full_data_30min_BC3_median.BC3, full_data_30min_BC3_p25.BC3, full_data_30min_BC3_p75.BC3, full_data_30min_BC3_min.BC3, full_data_30min_BC3_max.BC3],axis=1)
        full_data_30min_BC3.columns = ['BC520 Mean (ug.m-3)', 'BC520 std (ug.m-3)', 'BC520 Median (ug.m-3)', 'BC520 p25 (ug.m-3)', 'BC520 p75 (ug.m-3)', 'BC520 Min (ug.m-3)', 'BC520 Max (ug.m-3)']

        # Resample to 30min, creating dataframe to BC590 columns
        full_data_30min_BC4_mean = full_data_sorted.BC4.resample('30min').mean()
        full_data_30min_BC4_mean = full_data_30min_BC4_mean.to_frame()

        full_data_30min_BC4_std = full_data_sorted.BC4.resample('30min').std()
        full_data_30min_BC4_std = full_data_30min_BC4_std.to_frame()

        full_data_30min_BC4_median = full_data_sorted.BC4.resample('30min').median()
        full_data_30min_BC4_median = full_data_30min_BC4_median.to_frame()

        full_data_30min_BC4_max = full_data_sorted.BC4.resample('30min').max()
        full_data_30min_BC4_max = full_data_30min_BC4_max.to_frame()

        full_data_30min_BC4_min = full_data_sorted.BC4.resample('30min').min()
        full_data_30min_BC4_min = full_data_30min_BC4_min.to_frame()

        full_data_30min_BC4_p25 = full_data_sorted.BC4.resample('30min').quantile(q=0.25)
        full_data_30min_BC4_p25 = full_data_30min_BC4_p25.to_frame()

        full_data_30min_BC4_p75 = full_data_sorted.BC4.resample('30min').quantile(q=0.75)
        full_data_30min_BC4_p75 = full_data_30min_BC4_p75.to_frame()

        # generating BC590 dataframe with 30min resolution
        full_data_30min_BC4=pd.concat([full_data_30min_BC4_mean.BC4, full_data_30min_BC4_std.BC4, full_data_30min_BC4_median.BC4, full_data_30min_BC4_p25.BC4, full_data_30min_BC4_p75.BC4, full_data_30min_BC4_min.BC4, full_data_30min_BC4_max.BC4],axis=1)
        full_data_30min_BC4.columns = ['BC590 Mean (ug.m-3)', 'BC590 std (ug.m-3)', 'BC590 Median (ug.m-3)', 'BC590 p25 (ug.m-3)', 'BC590 p75 (ug.m-3)', 'BC590 Min (ug.m-3)', 'BC590 Max (ug.m-3)']

        # Resample to 30min, creating dataframe to BC660 columns
        full_data_30min_BC5_mean = full_data_sorted.BC5.resample('30min').mean()
        full_data_30min_BC5_mean = full_data_30min_BC5_mean.to_frame()

        full_data_30min_BC5_std = full_data_sorted.BC5.resample('30min').std()
        full_data_30min_BC5_std = full_data_30min_BC5_std.to_frame()

        full_data_30min_BC5_median = full_data_sorted.BC5.resample('30min').median()
        full_data_30min_BC5_median = full_data_30min_BC5_median.to_frame()

        full_data_30min_BC5_max = full_data_sorted.BC5.resample('30min').max()
        full_data_30min_BC5_max = full_data_30min_BC5_max.to_frame()

        full_data_30min_BC5_min = full_data_sorted.BC5.resample('30min').min()
        full_data_30min_BC5_min = full_data_30min_BC5_min.to_frame()

        full_data_30min_BC5_p25 = full_data_sorted.BC5.resample('30min').quantile(q=0.25)
        full_data_30min_BC5_p25 = full_data_30min_BC5_p25.to_frame()

        full_data_30min_BC5_p75 = full_data_sorted.BC5.resample('30min').quantile(q=0.75)
        full_data_30min_BC5_p75 = full_data_30min_BC5_p75.to_frame()

        # generating BC660 dataframe with 30min resolution
        full_data_30min_BC5=pd.concat([full_data_30min_BC5_mean.BC5, full_data_30min_BC5_std.BC5, full_data_30min_BC5_median.BC5, full_data_30min_BC5_p25.BC5, full_data_30min_BC5_p75.BC5, full_data_30min_BC5_min.BC5, full_data_30min_BC5_max.BC5],axis=1)
        full_data_30min_BC5.columns = ['BC660 Mean (ug.m-3)', 'BC660 std (ug.m-3)', 'BC660 Median (ug.m-3)', 'BC660 p25 (ug.m-3)', 'BC660 p75 (ug.m-3)', 'BC660 Min (ug.m-3)', 'BC660 Max (ug.m-3)']

        # Resample to 30min, creating dataframe to BC880 columns
        full_data_30min_BC6_mean = full_data_sorted.BC6.resample('30min').mean()
        full_data_30min_BC6_mean = full_data_30min_BC6_mean.to_frame()

        full_data_30min_BC6_std = full_data_sorted.BC6.resample('30min').std()
        full_data_30min_BC6_std = full_data_30min_BC6_std.to_frame()

        full_data_30min_BC6_median = full_data_sorted.BC6.resample('30min').median()
        full_data_30min_BC6_median = full_data_30min_BC6_median.to_frame()

        full_data_30min_BC6_max = full_data_sorted.BC6.resample('30min').max()
        full_data_30min_BC6_max = full_data_30min_BC6_max.to_frame()

        full_data_30min_BC6_min = full_data_sorted.BC6.resample('30min').min()
        full_data_30min_BC6_min = full_data_30min_BC6_min.to_frame()

        full_data_30min_BC6_p25 = full_data_sorted.BC6.resample('30min').quantile(q=0.25)
        full_data_30min_BC6_p25 = full_data_30min_BC6_p25.to_frame()

        full_data_30min_BC6_p75 = full_data_sorted.BC6.resample('30min').quantile(q=0.75)
        full_data_30min_BC6_p75 = full_data_30min_BC6_p75.to_frame()

        # generating BC880 dataframe with 30min resolution
        full_data_30min_BC6=pd.concat([full_data_30min_BC6_mean.BC6, full_data_30min_BC6_std.BC6, full_data_30min_BC6_median.BC6, full_data_30min_BC6_p25.BC6, full_data_30min_BC6_p75.BC6, full_data_30min_BC6_min.BC6, full_data_30min_BC6_max.BC6],axis=1)
        full_data_30min_BC6.columns = ['BC880 Mean (ug.m-3)', 'BC880 std (ug.m-3)', 'BC880 Median (ug.m-3)', 'BC880 p25 (ug.m-3)', 'BC880 p75 (ug.m-3)', 'BC880 Min (ug.m-3)', 'BC880 Max (ug.m-3)']

        # Resample to 30min, creating dataframe to BC950 columns
        full_data_30min_BC7_mean = full_data_sorted.BC7.resample('30min').mean()
        full_data_30min_BC7_mean = full_data_30min_BC7_mean.to_frame()

        full_data_30min_BC7_std = full_data_sorted.BC7.resample('30min').std()
        full_data_30min_BC7_std = full_data_30min_BC7_std.to_frame()

        full_data_30min_BC7_median = full_data_sorted.BC7.resample('30min').median()
        full_data_30min_BC7_median = full_data_30min_BC7_median.to_frame()

        full_data_30min_BC7_max = full_data_sorted.BC7.resample('30min').max()
        full_data_30min_BC7_max = full_data_30min_BC7_max.to_frame()

        full_data_30min_BC7_min = full_data_sorted.BC7.resample('30min').min()
        full_data_30min_BC7_min = full_data_30min_BC7_min.to_frame()

        full_data_30min_BC7_p25 = full_data_sorted.BC7.resample('30min').quantile(q=0.25)
        full_data_30min_BC7_p25 = full_data_30min_BC7_p25.to_frame()

        full_data_30min_BC7_p75 = full_data_sorted.BC7.resample('30min').quantile(q=0.75)
        full_data_30min_BC7_p75 = full_data_30min_BC7_p75.to_frame()

        full_data_30min_BC7_count = full_data_sorted.BC7.resample('30min').count()
        full_data_30min_BC7_count = full_data_30min_BC7_count.to_frame()

        # generating BC950 dataframe with 30min resolution
        full_data_30min_BC7=pd.concat([full_data_30min_BC7_mean.BC7, full_data_30min_BC7_std.BC7, full_data_30min_BC7_median.BC7, full_data_30min_BC7_p25.BC7, full_data_30min_BC7_p75.BC7, full_data_30min_BC7_min.BC7, full_data_30min_BC7_max.BC7, full_data_30min_BC7_count.BC7],axis=1)
        full_data_30min_BC7.columns = ['BC950 Mean (ug.m-3)', 'BC950 std (ug.m-3)', 'BC950 Median (ug.m-3)', 'BC950 p25 (ug.m-3)', 'BC950 p75 (ug.m-3)', 'BC950 Min (ug.m-3)', 'BC950 Max (ug.m-3)', '# of samples']

        # generating the final columns with all BC dataframes
        full_data_30min=pd.concat([full_data_30min_BC1, full_data_30min_BC2, full_data_30min_BC3, full_data_30min_BC4, full_data_30min_BC5, full_data_30min_BC6, full_data_30min_BC7], axis=1)

        # creating the time metadata
        time_now = datetime.now()

        # reading the metadata file
        all_metadata=r_metadata()
        instrument='AE33'
        site= str(all_metadata[instrument][0])
        brand= str(all_metadata[instrument][2])
        model= str(all_metadata[instrument][3])
        serial_num= str(all_metadata[instrument][4])
        val_level= str(all_metadata[instrument][5])
        proc_vers= str(all_metadata[instrument][6])

        data_removed= str((1-(data_processed/data_init))*100)

        str_metadata = 'File generated on ' + str(time_now) + ', Level:' + val_level + ', Processing Version: ' + proc_vers + ', Amount of raw data removed: ' + data_removed + '%' + ', Site: ' + site + ', Instrument: ' + instrument + ', Model: ' + model + ', S/N: ' + serial_num + ', Data processing details can be found in ftp://ftp.lfa.if.usp.br/LFA_Processed_Data/DataProcessing_GoAmazon_LFA.pdf\n'

        metadata=pd.Series([(str_metadata)])

        # generating the filename
        str_filename_5min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '5min.csv'
        str_filename_30min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '30min.csv'

        # writing the file to 5min
        with open(str_filename_5min, 'w') as fout:
            fout.write(str_metadata)
        full_data_5min.to_csv(str_filename_5min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

        # writing the file to 30min
        with open(str_filename_30min, 'w') as fout:
            fout.write(str_metadata)
        full_data_30min.to_csv(str_filename_30min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

def SMPS(folder_selected):

    # Finding all files with .csv extensions in the selected folder
    PATH = folder_selected
    EXT = "*.txt"
    files = [file for path, subdir, files in os.walk(PATH)
                      for file in glob(os.path.join(path, EXT))]

    # reading and concatenating all data from all files
    aux = pd.read_csv(files[0], sep=",", header=25, engine='python', encoding = "ISO-8859-1")
    if(len(aux.columns)==146):
        full_data = pd.read_csv(files[0], sep=",", header=25, usecols = list(range(1,3)) + list(range(9,116+1)) + [120+1,121+1] + [133+1,136+1] + [143+1], engine='python', encoding = "ISO-8859-1")
    else: full_data = pd.read_csv(files[0], sep=",", header=25, usecols = list(range(1,3)) + list(range(9,116)) + [133,136] + [120,121] + [143], engine='python', encoding = "ISO-8859-1")

    print('Total files found: ' + str(len(files)))
    for i in range(1, len(files)):
        aux = pd.read_csv(files[i], sep=",", header=25, engine='python', encoding = "ISO-8859-1")
        if(len(aux.columns)==146):
            full_data_i = pd.read_csv(files[i], sep=",", header=25, usecols = list(range(1,3)) + list(range(9,116+1)) + [120+1,121+1] + [133+1,136+1] + [143+1], engine='python', encoding = "ISO-8859-1")
        else: full_data_i = pd.read_csv(files[i], sep=",", header=25, usecols = list(range(1,3)) + list(range(9,116)) + [120,121] + [133,136] + [143], engine='python', encoding = "ISO-8859-1")
        full_data = pd.concat([full_data, full_data_i])
        print('File read: ' + str(i+1), end="\r")
    print("F")
    print(" ")


    print("Processing Data... ")
    with indicator.Running():

        # transforming the Time (UTC) for datetime object

        full_data["Time (UTC)"]=full_data["Date"]+" "+full_data["Start Time"]
        full_data["Time (UTC)"] = pd.to_datetime(full_data["Time (UTC)"],format='%m/%d/%Y %H:%M:%S')
        full_data = full_data.set_index(pd.DatetimeIndex(full_data["Time (UTC)"]))
        full_data=full_data.drop(columns=['Date','Start Time',"Time (UTC)"])

        # time sorting the file
        full_data_sorted = full_data.sort_index()

        # number of lines in the initial data
        data_init = len(full_data_sorted)

        # applying the level 1 filters
        full_data_sorted = full_data_sorted[full_data_sorted['Total Conc. (#/cm³)'] > -50]
        full_data_sorted = full_data_sorted[full_data_sorted['Total Conc. (#/cm³)'] < 10000]
        full_data_sorted = full_data_sorted[full_data_sorted['Sheath Flow (L/min)'] > 3]
        full_data_sorted = full_data_sorted[full_data_sorted['Sheath Flow (L/min)'] < 12]
        full_data_sorted = full_data_sorted[full_data_sorted['Aerosol Flow (L/min)'] > 0.3]
        full_data_sorted = full_data_sorted[full_data_sorted['Aerosol Flow (L/min)'] < 2]
        full_data_sorted = full_data_sorted[full_data_sorted['Instrument Errors'] == 'Normal Scan']

        # number of lines in the processed data
        data_processed = len(full_data_sorted)

        # calculcating TotalVolume column
        full_data_TotalVol = full_data_sorted['Total Conc. (#/cm³)']
        full_data_TotalVol = pd.DataFrame(full_data_TotalVol)

        for i in range(0,len(full_data_sorted)):
            line_TotalVol=0

            j=0

            diam=float(full_data_sorted.columns[j])
            diam_ant=9
            dlogDp=np.log10(diam)-np.log10(diam_ant)
            dN=full_data_sorted.loc[full_data_sorted.index[i],full_data_sorted.columns[j]]

            aux = (1E-9)*(np.pi/6)*dN*dlogDp*(diam**3)
            line_TotalVol+=aux

            for j in range(1,len(full_data_sorted.columns)-6):
                diam=float(full_data_sorted.columns[j])
                diam_ant=float(full_data_sorted.columns[j-1])
                dlogDp=np.log10(diam)-np.log10(diam_ant)
                dN=full_data_sorted.loc[full_data_sorted.index[i],full_data_sorted.columns[j]]

                aux = (1E-9)*(np.pi/6)*dN*dlogDp*(diam**3)
                line_TotalVol+=aux

            j = len(full_data_sorted.columns)-1
            if((full_data_sorted.loc[full_data_sorted.index[i],full_data_sorted.columns[j]] < 0) or (full_data_sorted.loc[full_data_sorted.index[i],full_data_sorted.columns[j]] > 0)):
                diam=float(full_data_sorted.columns[j])
                diam_ant=429.4
                dlogDp=np.log10(diam)-np.log10(diam_ant)
                dN=full_data_sorted.loc[full_data_sorted.index[i],full_data_sorted.columns[j]]

                aux = (1E-9)*(np.pi/6)*dN*dlogDp*(diam**3)
                line_TotalVol+=aux
                #print("Fora do for",line_TotalVol)

            full_data_TotalVol.loc[full_data_sorted.index[i]] = line_TotalVol

        # including the entire year period
        full_data_sorted=include_entire_year(full_data_sorted)
        full_data_TotalVol=include_entire_year(full_data_TotalVol)

        # Resample to 5min, creating dataframe to TotalConc
        full_data_5min_TotalConc_mean = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').mean()
        full_data_5min_TotalConc_mean = full_data_5min_TotalConc_mean.to_frame()

        full_data_5min_TotalConc_std = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').std()
        full_data_5min_TotalConc_std = full_data_5min_TotalConc_std.to_frame()

        full_data_5min_TotalConc_median = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').median()
        full_data_5min_TotalConc_median = full_data_5min_TotalConc_median.to_frame()

        full_data_5min_TotalConc_max = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').max()
        full_data_5min_TotalConc_max = full_data_5min_TotalConc_max.to_frame()

        full_data_5min_TotalConc_min = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').min()
        full_data_5min_TotalConc_min = full_data_5min_TotalConc_min.to_frame()

        full_data_5min_TotalConc_p25 = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').quantile(q=0.25)
        full_data_5min_TotalConc_p25 = full_data_5min_TotalConc_p25.to_frame()

        full_data_5min_TotalConc_p75 = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').quantile(q=0.75)
        full_data_5min_TotalConc_p75 = full_data_5min_TotalConc_p75.to_frame()

        full_data_5min_TotalConc_count = full_data_sorted['Total Conc. (#/cm³)'].resample('5min').count()
        full_data_5min_TotalConc_count = full_data_5min_TotalConc_count.to_frame()

        # Resample to 5min, crerating dataframe to TotalVolume
        full_data_5min_TotalVol_mean = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').mean()
        full_data_5min_TotalVol_mean = full_data_5min_TotalVol_mean.to_frame()

        full_data_5min_TotalVol_std = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').std()
        full_data_5min_TotalVol_std = full_data_5min_TotalVol_std.to_frame()

        full_data_5min_TotalVol_median = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').median()
        full_data_5min_TotalVol_median = full_data_5min_TotalVol_median.to_frame()

        full_data_5min_TotalVol_max = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').max()
        full_data_5min_TotalVol_max = full_data_5min_TotalVol_max.to_frame()

        full_data_5min_TotalVol_min = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').min()
        full_data_5min_TotalVol_min = full_data_5min_TotalVol_min.to_frame()

        full_data_5min_TotalVol_p25 = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').quantile(q=0.25)
        full_data_5min_TotalVol_p25 = full_data_5min_TotalVol_p25.to_frame()

        full_data_5min_TotalVol_p75 = full_data_TotalVol['Total Conc. (#/cm³)'].resample('5min').quantile(q=0.75)
        full_data_5min_TotalVol_p75 = full_data_5min_TotalVol_p75.to_frame()

        # Resample to 5min, creating dataframe to MeanDiam
        full_data_5min_MeanDiam_mean = full_data_sorted['Geo. Mean (nm)'].resample('5min').mean()
        full_data_5min_MeanDiam_mean = full_data_5min_MeanDiam_mean.to_frame()

        full_data_5min_MeanDiam_std = full_data_sorted['Geo. Mean (nm)'].resample('5min').std()
        full_data_5min_MeanDiam_std = full_data_5min_MeanDiam_std.to_frame()

        full_data_5min_MeanDiam_median = full_data_sorted['Geo. Mean (nm)'].resample('5min').median()
        full_data_5min_MeanDiam_median = full_data_5min_MeanDiam_median.to_frame()

        full_data_5min_MeanDiam_max = full_data_sorted['Geo. Mean (nm)'].resample('5min').max()
        full_data_5min_MeanDiam_max = full_data_5min_MeanDiam_max.to_frame()

        full_data_5min_MeanDiam_min = full_data_sorted['Geo. Mean (nm)'].resample('5min').min()
        full_data_5min_MeanDiam_min = full_data_5min_MeanDiam_min.to_frame()

        full_data_5min_MeanDiam_p25 = full_data_sorted['Geo. Mean (nm)'].resample('5min').quantile(q=0.25)
        full_data_5min_MeanDiam_p25 = full_data_5min_MeanDiam_p25.to_frame()

        full_data_5min_MeanDiam_p75 = full_data_sorted['Geo. Mean (nm)'].resample('5min').quantile(q=0.75)
        full_data_5min_MeanDiam_p75 = full_data_5min_MeanDiam_p75.to_frame()

        # Resample to 5 min, creating dataframe to all columns of concentration by diameter
        full_data_5min_ConcByDiam=full_data_sorted[full_data_sorted.columns[0:107]].resample('5min').mean()
        aux = full_data_sorted[full_data_sorted.columns[len(full_data_sorted.columns)-1]].resample('5min').mean()
        full_data_5min_ConcByDiam=pd.concat([full_data_5min_ConcByDiam,aux], axis=1)

        # creating the Unix Timestamp column for 5min
        full_data_5min_TotalConc_mean['Timestamp']=full_data_5min_TotalConc_mean.index
        full_data_5min_TotalConc_mean['Timestamp']=(full_data_5min_TotalConc_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating the final data dataframe with 5min resolution
        full_data_5min=pd.concat([full_data_5min_TotalConc_mean.Timestamp, full_data_5min_TotalConc_mean['Total Conc. (#/cm³)'], full_data_5min_TotalConc_std['Total Conc. (#/cm³)'], full_data_5min_TotalConc_median['Total Conc. (#/cm³)'], full_data_5min_TotalConc_p25['Total Conc. (#/cm³)'], full_data_5min_TotalConc_p75['Total Conc. (#/cm³)'], full_data_5min_TotalConc_min['Total Conc. (#/cm³)'], full_data_5min_TotalConc_max['Total Conc. (#/cm³)'], full_data_5min_TotalVol_mean['Total Conc. (#/cm³)'], full_data_5min_TotalVol_std['Total Conc. (#/cm³)'], full_data_5min_TotalVol_median['Total Conc. (#/cm³)'], full_data_5min_TotalVol_p25['Total Conc. (#/cm³)'], full_data_5min_TotalVol_p75['Total Conc. (#/cm³)'], full_data_5min_TotalVol_min['Total Conc. (#/cm³)'], full_data_5min_TotalVol_max['Total Conc. (#/cm³)'], full_data_5min_MeanDiam_mean['Geo. Mean (nm)'], full_data_5min_MeanDiam_std['Geo. Mean (nm)'], full_data_5min_MeanDiam_median['Geo. Mean (nm)'], full_data_5min_MeanDiam_p25['Geo. Mean (nm)'], full_data_5min_MeanDiam_p75['Geo. Mean (nm)'], full_data_5min_MeanDiam_min['Geo. Mean (nm)'], full_data_5min_MeanDiam_max['Geo. Mean (nm)'], full_data_5min_ConcByDiam[full_data_5min_ConcByDiam.columns], full_data_5min_TotalConc_count['Total Conc. (#/cm³)']], axis=1)

        full_data_5min.columns = ['Unix Time (UTC)','Total concentration Mean (cm-3)','Total concentration std (cm-3)','Total concentration Median (cm-3)','Total concentration p25 (cm-3)','Total concentration p75 (cm-3)','Total concentration Min (cm-3)','Total concentration Max (cm-3)','Total volume Mean (cm3)','Total volume std (cm3)','Total volume Median (cm3)','Total volume p25 (cm3)','Total volume p75 (cm3)','Total volume Min (cm3)','Total volume Max (cm3)','Mean diameter Mean (nm)','Mean diameter std (nm)','Mean diameter Median (nm)','Mean diameter p25 (nm)','Mean diameter p75 (nm)','Mean diameter Min (nm)','Mean diameter Max (nm)','9.47 nm','9.82 nm','10.2 nm','10.6 nm','10.9 nm','11.3 nm','11.8 nm','12.2 nm','12.6 nm','13.1 nm','13.6 nm','14.1 nm','14.6 nm','15.1 nm','15.7 nm','16.3 nm','16.8 nm','17.5 nm','18.1 nm','18.8 nm','19.5 nm','20.2 nm','20.9 nm','21.7 nm','22.5 nm','23.3 nm','24.1 nm','25 nm','25.9 nm','26.9 nm','27.9 nm','28.9 nm','30 nm','31.1 nm','32.2 nm','33.4 nm','34.6 nm','35.9 nm','37.2 nm','38.5 nm','40 nm','41.4 nm','42.9 nm','44.5 nm','46.1 nm','47.8 nm','49.6 nm','51.4 nm','53.3 nm','55.2 nm','57.3 nm','59.4 nm','61.5 nm','63.8 nm','66.1 nm','68.5 nm','71 nm','73.7 nm','76.4 nm','79.1 nm','82 nm','85.1 nm','88.2 nm','91.4 nm','94.7 nm','98.2 nm','101.8 nm','105.5 nm','109.4 nm','113.4 nm','117.6 nm','121.9 nm','126.3 nm','131 nm','135.8 nm','140.7 nm','145.9 nm','151.2 nm','156.8 nm','162.5 nm','168.5 nm','174.7 nm','181.1 nm','187.7 nm','194.6 nm','201.7 nm','209.1 nm','216.7 nm','224.7 nm','232.9 nm','241.4 nm','250.3 nm','259.5 nm','269 nm','278.8 nm','289 nm','299.6 nm','310.6 nm','322 nm','333.8 nm','346 nm','358.7 nm','371.8 nm','385.4 nm','399.5 nm','414.2 nm','429.4 nm','445.1 nm', '# of samples']


        # Resample to 30min, creating dataframe to TotalConc
        full_data_30min_TotalConc_mean = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').mean()
        full_data_30min_TotalConc_mean = full_data_30min_TotalConc_mean.to_frame()

        full_data_30min_TotalConc_std = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').std()
        full_data_30min_TotalConc_std = full_data_30min_TotalConc_std.to_frame()

        full_data_30min_TotalConc_median = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').median()
        full_data_30min_TotalConc_median = full_data_30min_TotalConc_median.to_frame()

        full_data_30min_TotalConc_max = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').max()
        full_data_30min_TotalConc_max = full_data_30min_TotalConc_max.to_frame()

        full_data_30min_TotalConc_min = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').min()
        full_data_30min_TotalConc_min = full_data_30min_TotalConc_min.to_frame()

        full_data_30min_TotalConc_p25 = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').quantile(q=0.25)
        full_data_30min_TotalConc_p25 = full_data_30min_TotalConc_p25.to_frame()

        full_data_30min_TotalConc_p75 = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').quantile(q=0.75)
        full_data_30min_TotalConc_p75 = full_data_30min_TotalConc_p75.to_frame()

        full_data_30min_TotalConc_count = full_data_sorted['Total Conc. (#/cm³)'].resample('30min').count()
        full_data_30min_TotalConc_count = full_data_30min_TotalConc_count.to_frame()

        # Resample to 30min, creating dataframe to TotalVolume
        full_data_30min_TotalVol_mean = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').mean()
        full_data_30min_TotalVol_mean = full_data_30min_TotalVol_mean.to_frame()

        full_data_30min_TotalVol_std = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').std()
        full_data_30min_TotalVol_std = full_data_30min_TotalVol_std.to_frame()

        full_data_30min_TotalVol_median = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').median()
        full_data_30min_TotalVol_median = full_data_30min_TotalVol_median.to_frame()

        full_data_30min_TotalVol_max = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').max()
        full_data_30min_TotalVol_max = full_data_30min_TotalVol_max.to_frame()

        full_data_30min_TotalVol_min = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').min()
        full_data_30min_TotalVol_min = full_data_30min_TotalVol_min.to_frame()

        full_data_30min_TotalVol_p25 = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').quantile(q=0.25)
        full_data_30min_TotalVol_p25 = full_data_30min_TotalVol_p25.to_frame()

        full_data_30min_TotalVol_p75 = full_data_TotalVol['Total Conc. (#/cm³)'].resample('30min').quantile(q=0.75)
        full_data_30min_TotalVol_p75 = full_data_30min_TotalVol_p75.to_frame()


        # Resample to 30min, creating dataframe to MeanDiam
        full_data_30min_MeanDiam_mean = full_data_sorted['Geo. Mean (nm)'].resample('30min').mean()
        full_data_30min_MeanDiam_mean = full_data_30min_MeanDiam_mean.to_frame()

        full_data_30min_MeanDiam_std = full_data_sorted['Geo. Mean (nm)'].resample('30min').std()
        full_data_30min_MeanDiam_std = full_data_30min_MeanDiam_std.to_frame()

        full_data_30min_MeanDiam_median = full_data_sorted['Geo. Mean (nm)'].resample('30min').median()
        full_data_30min_MeanDiam_median = full_data_30min_MeanDiam_median.to_frame()

        full_data_30min_MeanDiam_max = full_data_sorted['Geo. Mean (nm)'].resample('30min').max()
        full_data_30min_MeanDiam_max = full_data_30min_MeanDiam_max.to_frame()

        full_data_30min_MeanDiam_min = full_data_sorted['Geo. Mean (nm)'].resample('30min').min()
        full_data_30min_MeanDiam_min = full_data_30min_MeanDiam_min.to_frame()

        full_data_30min_MeanDiam_p25 = full_data_sorted['Geo. Mean (nm)'].resample('30min').quantile(q=0.25)
        full_data_30min_MeanDiam_p25 = full_data_30min_MeanDiam_p25.to_frame()

        full_data_30min_MeanDiam_p75 = full_data_sorted['Geo. Mean (nm)'].resample('30min').quantile(q=0.75)
        full_data_30min_MeanDiam_p75 = full_data_30min_MeanDiam_p75.to_frame()

        # Resample to 30min, creating dataframe to all columns of concentration by diameter
        full_data_30min_ConcByDiam=full_data_sorted[full_data_sorted.columns[0:107]].resample('30min').mean()
        aux = full_data_sorted[full_data_sorted.columns[len(full_data_sorted.columns)-1]].resample('30min').mean()
        full_data_30min_ConcByDiam=pd.concat([full_data_30min_ConcByDiam,aux], axis=1)

        # creating the Unix Timestamp column for 30min
        full_data_30min_TotalConc_mean['Timestamp']=full_data_30min_TotalConc_mean.index
        full_data_30min_TotalConc_mean['Timestamp']=(full_data_30min_TotalConc_mean['Timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')

        # generating the final data dataframe with 30min resolution
        full_data_30min=pd.concat([full_data_30min_TotalConc_mean.Timestamp, full_data_30min_TotalConc_mean['Total Conc. (#/cm³)'], full_data_30min_TotalConc_std['Total Conc. (#/cm³)'], full_data_30min_TotalConc_median['Total Conc. (#/cm³)'], full_data_30min_TotalConc_p25['Total Conc. (#/cm³)'], full_data_30min_TotalConc_p75['Total Conc. (#/cm³)'], full_data_30min_TotalConc_min['Total Conc. (#/cm³)'], full_data_30min_TotalConc_max['Total Conc. (#/cm³)'], full_data_30min_TotalVol_mean['Total Conc. (#/cm³)'], full_data_30min_TotalVol_std['Total Conc. (#/cm³)'], full_data_30min_TotalVol_median['Total Conc. (#/cm³)'], full_data_30min_TotalVol_p25['Total Conc. (#/cm³)'], full_data_30min_TotalVol_p75['Total Conc. (#/cm³)'], full_data_30min_TotalVol_min['Total Conc. (#/cm³)'], full_data_30min_TotalVol_max['Total Conc. (#/cm³)'], full_data_30min_MeanDiam_mean['Geo. Mean (nm)'], full_data_30min_MeanDiam_std['Geo. Mean (nm)'], full_data_30min_MeanDiam_median['Geo. Mean (nm)'], full_data_30min_MeanDiam_p25['Geo. Mean (nm)'], full_data_30min_MeanDiam_p75['Geo. Mean (nm)'], full_data_30min_MeanDiam_min['Geo. Mean (nm)'], full_data_30min_MeanDiam_max['Geo. Mean (nm)'], full_data_30min_ConcByDiam[full_data_30min_ConcByDiam.columns], full_data_30min_TotalConc_count['Total Conc. (#/cm³)']], axis=1)

        full_data_30min.columns = ['Unix Time (UTC)','Total concentration Mean (cm-3)','Total concentration std (cm-3)','Total concentration Median (cm-3)','Total concentration p25 (cm-3)','Total concentration p75 (cm-3)','Total concentration Min (cm-3)','Total concentration Max (cm-3)','Total volume Mean (cm3)','Total volume std (cm3)','Total volume Median (cm3)','Total volume p25 (cm3)','Total volume p75 (cm3)','Total volume Min (cm3)','Total volume Max (cm3)','Mean diameter Mean (nm)','Mean diameter std (nm)','Mean diameter Median (nm)','Mean diameter p25 (nm)','Mean diameter p75 (nm)','Mean diameter Min (nm)','Mean diameter Max (nm)','9.47 nm','9.82 nm','10.2 nm','10.6 nm','10.9 nm','11.3 nm','11.8 nm','12.2 nm','12.6 nm','13.1 nm','13.6 nm','14.1 nm','14.6 nm','15.1 nm','15.7 nm','16.3 nm','16.8 nm','17.5 nm','18.1 nm','18.8 nm','19.5 nm','20.2 nm','20.9 nm','21.7 nm','22.5 nm','23.3 nm','24.1 nm','25 nm','25.9 nm','26.9 nm','27.9 nm','28.9 nm','30 nm','31.1 nm','32.2 nm','33.4 nm','34.6 nm','35.9 nm','37.2 nm','38.5 nm','40 nm','41.4 nm','42.9 nm','44.5 nm','46.1 nm','47.8 nm','49.6 nm','51.4 nm','53.3 nm','55.2 nm','57.3 nm','59.4 nm','61.5 nm','63.8 nm','66.1 nm','68.5 nm','71 nm','73.7 nm','76.4 nm','79.1 nm','82 nm','85.1 nm','88.2 nm','91.4 nm','94.7 nm','98.2 nm','101.8 nm','105.5 nm','109.4 nm','113.4 nm','117.6 nm','121.9 nm','126.3 nm','131 nm','135.8 nm','140.7 nm','145.9 nm','151.2 nm','156.8 nm','162.5 nm','168.5 nm','174.7 nm','181.1 nm','187.7 nm','194.6 nm','201.7 nm','209.1 nm','216.7 nm','224.7 nm','232.9 nm','241.4 nm','250.3 nm','259.5 nm','269 nm','278.8 nm','289 nm','299.6 nm','310.6 nm','322 nm','333.8 nm','346 nm','358.7 nm','371.8 nm','385.4 nm','399.5 nm','414.2 nm','429.4 nm','445.1 nm', '# of samples']

        # creating the time metadata
        time_now = datetime.now()

        # reading the metadata file
        all_metadata=r_metadata()
        instrument='SMPS'
        site= str(all_metadata[instrument][0])
        brand= str(all_metadata[instrument][2])
        model= str(all_metadata[instrument][3])
        serial_num= str(all_metadata[instrument][4])
        val_level= str(all_metadata[instrument][5])
        proc_vers= str(all_metadata[instrument][6])

        data_removed= str((1-(data_processed/data_init))*100)

        str_metadata = 'File generated on ' + str(time_now) + ', Level:' + val_level + ', Processing Version: ' + proc_vers + ', Amount of raw data removed: ' + data_removed + '%' + ', Site: ' + site + ', Instrument: ' + instrument + ', Model: ' + model + ', S/N: ' + serial_num + ', Data processing details can be found in ftp://ftp.lfa.if.usp.br/LFA_Processed_Data/DataProcessing_GoAmazon_LFA.pdf\n'

        # generating the filename
        str_filename_5min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '5min.csv'
        str_filename_30min = site + '_' + str(time_now.year) + '_' + instrument + '_' + brand + '_' + model + '_' + 'Level' + val_level + '_' + '30min.csv'

        # writing the file to 5min
        with open(str_filename_5min, 'w') as fout:
            fout.write(str_metadata)
        full_data_5min.to_csv(str_filename_5min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')

        # writing the file to 30min
        # with open(str_filename_30min, 'w') as fout:
        #     fout.write(str_metadata)
        # full_data_30min.to_csv(str_filename_30min, sep=';', encoding='utf-8', na_rep='NaN', header=True, mode='a')
