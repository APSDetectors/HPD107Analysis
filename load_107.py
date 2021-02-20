import sqlite3
import pandas as pd
import numpy as np


'''

Loading .csv files

'''

def load_csv(filepath):
    '''
    Loads relevant columns of a 107 log 

    Parameters
    ----------
    filepath : str
        Filepath of individual, complete 107 log (.csv file)  

    Returns
    -------
    log_107 : DataFrame
        Loaded, reformatted 107 log

    '''
    #Load relevant columns 107 log
    log_filepath = r'{}'.format(filepath)
    log_107 = pd.read_csv(log_filepath, usecols = [0,1,2,3,5,7,8,9,12,13,18], skiprows = [1,2], na_filter=False)
    
    #Reorder and rename columns
    column_order = [0,2,3,4,6,7,5,10,8,9,1]
    column_names = ['Date/Time','Hours','50mK','He-3','3K','MagnetDiode','50K','Setpoint','Current','Voltage','Notes']
    log_107 = log_107[[log_107.columns[i] for i in column_order]]
    log_107.columns = column_names 
    
    #Convert type of "Date/Time" column from string to datetime 
    log_107['Date/Time'] = pd.to_datetime(log_107['Date/Time'], infer_datetime_format=True)
    
    return log_107


def split_csv(log):
    '''
    Splits a reformatted 107 log into separate logs for separate phases (i.e. cooldown, regen, reg, and warmup phases)
    Stores separated logs into 3 dictionaries
    
    Parameters
    ----------
    log : DataFrame
        Entire, reformatted 107 log. Return of load_csv(). 

    Returns
    -------
    coolwarmfils : dict
        Dictionary of cooldown and warmup logs 
        Key names are 'cooldown' and 'warmup'
    regenfiles : dict
        Dictionary of regen logs
        Key names are 'regen1','regen2','regen3',... with numbers based on chronological order
        Logs are reformatted: index and 'Hours from Start' start at 0 for each phase 
        Logs are sorted: if the magnet does not turn on or the magnet cycle is too short/long, it is excluded from this dictionary
    regfiles : dict
        Dictionary of reg logs
        Key names are 'reg1','reg2','reg3',... with numbers based on chronological order
        Logs are reformatted: index and 'Hours from Start' start at 0 for each phase
        Logs are sorted: if the magnet current is too small/large, it is excluded from this dictionary
    '''
    
    
    #Determine ADR cycle start and completion via Notes column
    all_booleans = log['Notes'].map(lambda x:'Start Mag Cycle' in x or 'Mag Cycle complete' in x or 'Mag Cycle Canceled' in x).to_list() 
    all_booleans[0] = True
    all_booleans[-1] = True
    #Create list of indicies where run starts, run completes, ADR cycle starts, and ADR cycle completes
    all_indicies = log.index[all_booleans].to_list() 
    
    
    #Create a dictionary storing cooldown and warmup logs 
    
    coolwarmfiles = {} #Initialize dictionary 
    coolwarmfiles['cooldown']=log.iloc[all_indicies[0]:all_indicies[1],:] 
    coolwarmfiles['warmup']=log.iloc[all_indicies[-2]:all_indicies[-1],:] 
    coolwarmfiles['cooldown']['Hours'] = (coolwarmfiles['cooldown']['Date/Time']-coolwarmfiles['cooldown'].iloc[0,0]).dt.total_seconds()/3600 
    coolwarmfiles['warmup']['Hours'] = (coolwarmfiles['warmup']['Date/Time']-coolwarmfiles['warmup'].iloc[0,0]).dt.total_seconds()/3600 


    #Create a dictionary storing regen logs
    
    #Determine ADR cycle start via Notes column
    regen_booleans = log['Notes'].map(lambda x:'Start Mag Cycle' in x).to_list() 
    #Create list of indicies where ADR cycle starts
    regen_indicies = log.index[regen_booleans].to_list() 
    regenfiles = {} #Initialize dictionary 
    regen_count = 0 #Counter variable for naming dictionary keys 
    for x in range(len(regen_indicies)):
        #Check if magnet turns on (current reaches above 15 A) and if magnet cycle lasts appropriate length of time (between 3 to 5 hours)
        if log.iloc[regen_indicies[x]:all_indicies[all_indicies.index(regen_indicies[x])+1],8].map(lambda x:x>15).any() and \
        3<((log.iloc[all_indicies[all_indicies.index(regen_indicies[x])+1],0]-log.iloc[regen_indicies[x],0]).total_seconds()/3600)<5:
            regen_count += 1 
            #Add regen log to dictionary and reset index 
            regenfiles['regen{}'.format(regen_count)]=log.iloc[regen_indicies[x]:all_indicies[all_indicies.index(regen_indicies[x])+1],:].reset_index(drop=True) 
            #Reset "Hours from Start" column 
            regenfiles['regen{}'.format(regen_count)]["Hours"] = (regenfiles['regen{}'.format(regen_count)]['Date/Time']-regenfiles['regen{}'.format(regen_count)].iloc[0,0]).dt.total_seconds()/3600 
    
    
    #Create a dictionary storing reg logs
    
    #Determine ADR cycle completion via Notes column
    reg_booleans = log['Notes'].map(lambda x:'Mag Cycle complete' in x or 'Mag Cycle Canceled' in x).to_list() 
    #Create list of indicies where ADR cycle completes
    reg_indicies = log.index[reg_booleans].to_list() 
    regfiles = {} #Initialize dictionary 
    reg_count = 0 #Counter variable for naming dictionary keys 
    for x in range(len(reg_indicies)):
        #Check if magnet current is reasonable (above 0.1 A and below 2 A)
        if not log.iloc[reg_indicies[x]:all_indicies[all_indicies.index(reg_indicies[x])+1],8].map(lambda x:x<0.1).all() and \
        not log.iloc[reg_indicies[x]:all_indicies[all_indicies.index(reg_indicies[x])+1],8].map(lambda x:x>2).any():
            reg_count += 1 
            #Add reg log to dictionary and reset index
            regfiles['reg{}'.format(reg_count)]=log.iloc[reg_indicies[x]:all_indicies[all_indicies.index(reg_indicies[x])+1],:].reset_index(drop=True) 
            #Reset "Hours from Start" column 
            regfiles['reg{}'.format(reg_count)]["Hours"] = (regfiles['reg{}'.format(reg_count)]['Date/Time']-regfiles['reg{}'.format(reg_count)].iloc[0,0]).dt.total_seconds()/3600 
            #Replace 0 values in "50 mK FAA" column with NaN 
            regfiles['reg{}'.format(reg_count)]['50mK'].replace(0,np.nan,inplace=True) 
    #Filter warmup data from temperature holds 
    regfiles = temphold_filter(regfiles)
    
    return (coolwarmfiles,regenfiles,regfiles)


def temphold_filter(regfiles):
    '''
    Removes portions of temperature hold logs where the magnet is off (i.e. current is less than 0.085 A)
    E.g. if a temperature hold log includes a warmup, the warmup is removed 

    Parameters
    ----------
    regfiles : dict
        Dictionary of all temperature hold phase logs. 

    Returns
    -------
    regfiles : dict
        Dictionary of revised temperature regulation phase logs.

    '''
    for key,reg in regfiles.items():
        #Remove parts of temperature regulation phase logs where magnet is off
        regfiles[key] = reg.loc[reg['Current']>0.085]
        #Reset index and "Hours after Start" to start at 0 
        regfiles[key].reset_index(drop=True, inplace = True)
        regfiles[key]["Hours"] = (regfiles[key]['Date/Time']-regfiles[key].iloc[0,0]).dt.total_seconds()/3600
    return regfiles


'''

Reading and writing to database

'''


def load_db(filepath):
    
    #Load relevant columns 107 log
    log_filepath = r'{}'.format(filepath)
    log_107 = pd.read_csv(log_filepath, usecols = [0,1,2,3,5,7,8,9,12,13,18], index_col = 0, skiprows = [1,2], na_filter=False)
    column_order = [1,2,3,5,6,4,9,7,8,0] 
    column_names = ['Hours','50mK','He-3','3K','MagnetDiode','50K','Setpoint','Current','Voltage','Notes']
    log_107 = log_107[[log_107.columns[i] for i in column_order]]
    log_107.columns = column_names 
    log_107.rename_axis('Id')
    log_107.insert(10, "Filepath", filepath)
    return log_107


def to_107db(filelist): 
    conn = sqlite3.connect('cryo107.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    for file in filelist: 
        log = load_db(file)
        log.index = log.index.map(lambda x: '{year}-{month}-{day} {hour}:{minute}:{sec}'.format(year = x[6:10], month = x[0:2], day = x[3:5], hour = x[11:13], minute = x[14:16], sec = x[17:]))
        log.to_sql('Cryo107', conn, index_label = 'Id', if_exists = 'append', chunksize = 1000) 
    cur.close()


def read_107db(starttime, endtime): 
    conn = sqlite3.connect('cryo107.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    cur.execute("SELECT * FROM Cryo107 WHERE Id BETWEEN ? AND ? ORDER BY Id", (starttime, endtime))
    data = cur.fetchall() 
    dataDF = pd.DataFrame(data, columns = ['Date/Time','Hours','50mK','He-3','3K','MagnetDiode','50K','Setpoint','Current','Voltage','Notes', 'Filepath'])
    dataDF["Date/Time"] = pd.to_datetime(dataDF["Date/Time"])
    cur.close()
    return dataDF




'''

Test code

'''
conn = sqlite3.connect('cryo107.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS Cryo107')
cur.close()


to_107db(['/Users/cindy/Documents/Argonne 2020/2020_06_18_17;08snout_swissx2_1BM.csv'])
dataDF = read_107db("2020-06-23 17:39:30", "2020-06-23 17:39:60")

testDF = load_csv('/Users/cindy/Documents/Argonne 2020/2020_06_18_17;08snout_swissx2_1BM.csv')
testDict = split_csv(testDF)

filepath = '/Users/cindy/Documents/Argonne 2020/2019_11_01_17;38snout_swissx_M-452x2_1BM.csv'
filepath2 = '/Users/cindy/Documents/Argonne 2020/2020_06_18_17;08snout_swissx2_1BM.csv'
starttime = "2019-11-01 17:39:30"
endtime = "2019-11-01 17:39:60"