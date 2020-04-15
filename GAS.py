# -*- coding: utf-8 -*-
"""


Code written for GAS to combine staffing database containing cost data with 
collections database containing revenue data

Before running the code, 
    1. remve all instances of DR from staffing databse to ensure shift matches 
       to propper case lines
    2. Format Dates in both databases as mm/dd/yy 
    3. Remove row 1 in collections
    4. Delete trailing data in last rows of collections


"""
#%% import modules
import pandas as pd
import numpy as np

#%% Import Data
#import data containing Shifts (cost data)
staff = pd.read_csv('MostRecentData/SHC 12 Mo Staffing.csv', encoding="ISO-8859-1")
#import data containing cases (revenue data)
collections = pd.read_csv('MostRecentData/SHC Collections by DOS_04-14-2020.csv', encoding="ISO-8859-1")

#%%Clean Data

#delete unnecessary columns from shift data
staff = staff[['User','Date','Shift']]
staff = staff.dropna(axis = 'rows')
collections = collections.dropna(axis = 'rows')


#%% Create Join column by combing names and dates

#Seperate provider first and last names
staff[['Last Name', 'First Name']] = staff['User'].str.split(',', expand = True)
staff['Last Name'] = staff['Last Name'].str.upper()
collections[['Last Name', 'First Name']] = collections['Rendering Provider'].str.split(',', expand = True)

#Strip names of extra spaces
collections['First Name'] = collections['First Name'].str.strip()
staff['First Name'] = staff['First Name'].str.strip()

#Change Ryan G so not confused from Rayn C
staff.loc[staff['User'] == 'Ryan, G.', 'Last Name'] = 'RYANG'
collections.loc[collections['Rendering Provider'] == 
                'RYAN, WILLIAM G', 'Last Name'] = 'RYANG'

#Change Ryan C so not confused with Ryan G
staff.loc[staff['User'] == 'Ryan, C.', 'Last Name'] = 'RYANC'
collections.loc[collections['Rendering Provider'] == 
                'RYAN, CRAIG', 'Last Name'] = 'RYANC'

# Create Column to join databases 
collections['LookUp'] = collections['Last Name'] + '_' + collections['Date of Service - Case']
staff['LookUp'] = staff['Last Name'] + '_' + staff['Date']

#%% Format Date Columns

#format Date Columns
staff['Date'] = pd.to_datetime(staff['Date'])
collections['Date of Service - Case'] = pd.to_datetime(collections['Date of Service - Case'])

#add Day of Week Column
staff['Staff_Day'] = staff['Date'].dt.dayofweek
collections['Case_Day'] = collections['Date of Service - Case'].dt.day_name()

#%% Clean shift names with formatted dates
#Change MV Call on weekends to MV WE Call 1
staff.loc[(staff['Shift'] == 'MV Call 1') & (staff['Staff_Day'] == 5) , 'Shift'] = 'MV WE Call 1' 
staff.loc[(staff['Shift'] == 'MV Call 1') & (staff['Staff_Day'] == 6) , 'Shift'] = 'MV WE Call 1'


#%% Filter out Call shifts - Remove Call shifts and add back in Weekend call shifts

#Remove all call shifts from staff
staffShiftFiltered = staff[staff.Shift.str.contains('Call') == False]
#find weekend call shifts
wkndCallShifts = staff[staff.Shift.str.contains('Call WE | WE Call') == True]
#add weekend call shifts back to shifts
staffShiftFiltered = staffShiftFiltered.append(wkndCallShifts)

#store Exluded Call Shifts
staffExcludedShifts = staff[staff.Shift.str.contains('Call') == True]
staffExcludedShifts = staffExcludedShifts.drop(staff[staff.Shift.str.contains(
    'Call WE | WE Call') == True].index)

#%% Find Duplicated Shifts

duplicatedShifts = staffShiftFiltered[staffShiftFiltered.duplicated(['LookUp'], keep = False)]
duplicatedShifts = duplicatedShifts.assign(collections=duplicatedShifts.LookUp.isin(collections.LookUp).astype(str))
duplicatedShifts.rename(columns={'collections':'Provider has Cases on Date'}, inplace = True)
duplicatedShifts.drop(columns = 'LookUp', inplace = True)
#%% Join Databases

#Select staff Columns to join with collections
staffClean = staffShiftFiltered[['User','Date','Shift','LookUp','Staff_Day']]

#Join shift (cost) and cases (revenue) databases
join = pd.merge(collections,staffClean,on='LookUp', how = 'outer')
#join['Date'] = pd.to_datetime(join['Date'])

#%%identify cases without a shift and shifts without cases
nanCount = join.isna().sum()
empty_shifts = join[join['CPT'].isnull()]
empty_cases = join[join['Shift'].isnull()]

#%%remove rows with incmomplete data
joinCleaned = join.dropna(axis ='rows')

#%% Repeat Join for call shfits 

#Join empty shifts and excluded shifts
empty_shifts = empty_shifts.dropna(axis = 'columns', how = 'all')
empty_shifts = empty_shifts.append(staffExcludedShifts)
#drop na from cases
empty_cases = empty_cases.dropna(axis = 'columns', how = 'all')
#Select staff Columns to join with collections
empty_shifts = empty_shifts[['User','Date','Shift','LookUp','Staff_Day']]
empty_join = pd.merge(empty_shifts,empty_cases,on='LookUp', how = 'outer')
empty_join_cleaned = empty_join.dropna(axis ='rows')
#append call cases to join
joinCleaned = joinCleaned.append(empty_join_cleaned)
#identify cases without a shift and shifts without cases
empty_shifts_all = empty_join[empty_join['CPT'].isnull()]
empty_casees_all = empty_join[empty_join['Shift'].isnull()]

#%% Add fill in missing data and add back in empty cases and shifts
#Add facility to empty shifts
#emptyShifts['Facility'] = ''
empty_shifts_all.loc[empty_shifts_all['Shift'].str.contains('MV'), 
                       ['Facility']] = 'MOUNTAIN VISTA-MAIN'
empty_shifts_all.loc[empty_shifts_all['Shift'].str.contains('TSL'), 
                       ['Facility']] = 'TEMPE ST LUKES  MEDICAL CENTER'
empty_shifts_all.loc[empty_shifts_all['Shift'].str.contains('PSL'), 
                       ['Facility']] = 'ST LUKES MEDICAL CENTER LP'
empty_shifts_all.loc[empty_shifts_all['Shift'].str.contains('Florence'), 
                       ['Facility']] = 'FLORENCE'
empty_shifts_all.loc[empty_shifts_all['Shift'].str.contains('Luke\'s'), 
                       ['Facility']] = 'TEMPE ST LUKES  MEDICAL CENTER'
empty_shifts_all.loc[empty_shifts_all['Shift'].str.contains('Physician Call'), 
                       ['Facility']] = 'MOUNTAIN VISTA-MAIN'
#Add Shift to empty cases
empty_cases['Shift'] = 'Unknown Shift'

join_all =joinCleaned.append([empty_shifts_all,empty_casees_all])

#%% Add missings dates to new columns
join_all['Final_Date'] = np.NaN
join_all['Final_Date'] = join_all['Date'].fillna(
    join_all['Date']).fillna(join_all['Date of Service - Case'])

#%% Sum payment on facility and shift
# expectedShift = joinCleaned.groupby(['Facility','Shift'])['Expected'].sum()
# paymentsShift = joinCleaned.groupby(['Facility','Shift'])['Payments - All'].sum()
# balanceShift = joinCleaned.groupby(['Facility','Shift'])['Balance'].sum()
# shiftCount = joinCleaned.groupby(['Facility','Shift'])['Shift'].count()
#

#Group by year, month, facility. Aggreate cases and financials
output = join_all.groupby([join_all['Final_Date'].dt.year.rename('year'), 
                              join_all['Final_Date'].dt.month.rename('month'), 
                              'Facility', 
                              'Shift']
                             ).agg({
                                  'LookUp' : lambda x: x.nunique(),
                                  'Expected':sum,
                                  'Payments - All':sum,
                                  'Balance':sum })
# #%%
# #Export Data
# emptyShifts.loc[:,'User':'Shift'].to_csv('Output/Empty_Shifts.csv',index = False)
# emptyCases.loc[:,'Case ID':'Balance'].to_csv('Output/Cases_WithOut_Shifts.csv', index = False)
# duplicatedShifts.to_csv('Output/Duplicated_Shifts.csv', index = False)
# output.to_csv('Output/Sum_By_Shift_By_Facility.csv')

# # expectedShift.to_csv('Excpected.csv')
# # paymentsShift.to_csv('Payment.csv')
# # balanceShift.to_csv('Balance.csv')
