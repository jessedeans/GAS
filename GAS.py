# -*- coding: utf-8 -*-
"""


Code written for medical client to combine staffing database with collections
database to ultimately determine optimum number of shifts. The companys largest 
cost is staffing a hospital shift, and the Client wants to know how 
profitable each shift is. The collections database contains the of cases and 
revenue of each provider. By combining these two databases the client will have 
a better understanding of how many shifts are profitable. 

Before running the code, 
    1. Remove all instances of 'DR' from staffing database to ensure shift matches 
       to proper case lines
    2. Format dates in both databases as mm/dd/yy 
    3. Remove row 1 in collections
    4. Delete export metadata in last rows of collections



"""
#%% import modules
import pandas as pd
import numpy as np

#%% Import Raw Data
#import data containing Shifts (cost data)
staff = pd.read_csv('MostRecentData/SHC 12 Mo Staffing.csv', 
                    encoding="ISO-8859-1")
#import data containing cases (revenue data)
collections = pd.read_csv('MostRecentData/SHC Collections by DOS_04-14-2020.csv',
                          encoding="ISO-8859-1")

#%%Clean Data

#delete unnecessary columns from shift data
staff = staff[['User','Date','Shift']]
#delete unnecessary columns from collections data
collections = collections.drop(columns = 'ASA')
#Delete empty rows 
staff = staff.dropna(axis = 'rows', how = 'all')
collections = collections.dropna(axis = 'rows', how = 'all')

#%% Create join column by combining names and date strings 

#Each Database contains a provider and a date, albeit in different formats
#Databses will be joined on a unique provider date combination

#Separate provider first and last names and standardize case
staff[['Last Name', 'First Name']] = staff['User'].str.split(',', expand = True)
staff['Last Name'] = staff['Last Name'].str.upper()
collections[['Last Name', 'First Name']
            ] = collections['Rendering Provider'].str.split(',', expand = True)

#Strip names of extra spaces
collections['First Name'] = collections['First Name'].str.strip()
staff['First Name'] = staff['First Name'].str.strip()

#Two providers with last name Ryan, add first initial to create unique ID
# Ryan G
staff.loc[staff['User'] == 'Ryan, G.', 'Last Name'] = 'RYANG'
collections.loc[collections['Rendering Provider'] == 
                'RYAN, WILLIAM G', 'Last Name'] = 'RYANG'
# Ryan C
staff.loc[staff['User'] == 'Ryan, C.', 'Last Name'] = 'RYANC'
collections.loc[collections['Rendering Provider'] == 
                'RYAN, CRAIG', 'Last Name'] = 'RYANC'

# Create column to join databases 
collections['LookUp'] = collections['Last Name'
                                    ] + '_' + collections['Date of Service - Case']
staff['LookUp'] = staff['Last Name'] + '_' + staff['Date']

#%% Format Date Columns and add day of week

#format Date Columns
staff['Date'] = pd.to_datetime(staff['Date'])
collections['Date of Service - Case'] = pd.to_datetime(
    collections['Date of Service - Case'])

#add Day of Week Column
staff['Staff_Day'] = staff['Date'].dt.dayofweek
collections['Case_Day'] = collections['Date of Service - Case'].dt.day_name()

#%% Clean shift names with formatted dates

#Weekend call shift not standardized in databses sometimes Call sometimes WE CALL
#Weekday calls shifts are not assigned revenue so need to differentiate


#Change MV Call on weekends to MV WE Call 1
staff.loc[(staff['Shift'] == 'MV Call 1') & (staff['Staff_Day'] == 5) , 
          'Shift'] = 'MV WE Call 1' 
staff.loc[(staff['Shift'] == 'MV Call 1') & (staff['Staff_Day'] == 6) , 
          'Shift'] = 'MV WE Call 1'


#%% Filter out Call shifts - Remove Call shifts and add back in Weekend call shifts

#Providers have a shift for business hour cases and a call shift of after hours work.
#Revenue should be assigned to the normal shift, not the call shift. 
#On weekends there are only call shifts. 
#Revenue should be assigned to weekend call shifts 

#Remove all call shifts from staff
staff_shift_Filtered = staff[staff.Shift.str.contains('Call') == False]
#find weekend call shifts
wknd_call_shifts = staff[staff.Shift.str.contains('Call WE | WE Call') == True]
#add weekend call shifts back to shifts
staff_shift_Filtered = staff_shift_Filtered.append(wknd_call_shifts)
#store excluded Call Shifts
staff_excluded_shifts = staff[staff.Shift.str.contains('Call') == True]
staff_excluded_shifts = staff_excluded_shifts.drop(staff[staff.Shift.str.contains(
    'Call WE | WE Call') == True].index)

#%% Find Duplicate Shifts

duplicatedShifts = staff_shift_Filtered[
    staff_shift_Filtered.duplicated(['LookUp'], keep = False)]
duplicatedShifts = duplicatedShifts.assign(
    collections=duplicatedShifts.LookUp.isin(collections.LookUp).astype(str))
duplicatedShifts.rename(
    columns={'collections':'Provider has Cases on Date'}, inplace = True)
duplicatedShifts.drop(columns = 'LookUp', inplace = True)
print('There are ' + str(len(duplicatedShifts)/2) + ' duplicate shifts in first pass')

#%% Join Databases

#Select staff Columns to join with collections
staffClean = staff_shift_Filtered[['User','Date','Shift','LookUp','Staff_Day']]
#Join shift (cost) and cases (revenue) databases
join = pd.merge(collections,staffClean,on='LookUp', how = 'outer')

#%%identify cases without a shift and shifts without cases
#nanCount = join.isna().sum()
empty_shifts = join[join['CPT'].isnull()]
empty_cases = join[join['Shift'].isnull()]
print ('There are ' + str(len(empty_shifts)) + ' empty shifts in first pass')
print ('There are ' + str(len(empty_cases)) + ' empty cases in first pass')

#%%remove rows with incomplete data
join_cleaned = join.dropna(axis ='rows')

#%% Repeat Join for call shifts

#There may be instances where a provider only had a call shift on a day
#Revenue should be assigned to these call shifts

#Join empty shifts and excluded shifts
empty_shifts = empty_shifts.dropna(axis = 'columns', how = 'all')
empty_shifts = empty_shifts.append(staff_excluded_shifts)
#drop na from cases for cleaner join
empty_cases = empty_cases.dropna(axis = 'columns', how = 'all')
#Select staff Columns to join with collections
empty_shifts = empty_shifts[['User','Date','Shift','LookUp','Staff_Day']]
empty_join = pd.merge(empty_shifts,empty_cases,on='LookUp', how = 'outer')
empty_join_cleaned = empty_join.dropna(axis ='rows')
#append call cases to join
join_cleaned = join_cleaned.append(empty_join_cleaned)
#identify cases without a shift and shifts without cases
empty_shifts_all = empty_join[empty_join['CPT'].isnull()]
empty_casees_all = empty_join[empty_join['Shift'].isnull()]
print ('There are ' + str(len(empty_shifts_all)) + ' empty shifts in second pass')
print ('There are ' + str(len(empty_casees_all)) + ' empty cases in second pass')


#%% Add unmatched data back

#Facility can be assumed from the shift name.
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
#Create unknown shift for cases without a shift
empty_cases['Shift'] = 'Unknown Shift'
#Add unmatched data back
join_all =join_cleaned.append([empty_shifts_all,empty_casees_all])

#%% Create new date column with date for every row
join_all['Final_Date'] = np.NaN
join_all['Final_Date'] = join_all['Date'].fillna(
    join_all['Date']).fillna(join_all['Date of Service - Case'])

#%% Create Outputs

#Create monthly totals dataframe. 
#Group by year month and facility. Aggregate shifts and financials
facility_group = join_all.groupby([join_all['Final_Date'].dt.year.rename('year'), 
                              join_all['Final_Date'].dt.month.rename('month'), 
                              'Facility'] 
                             ).agg({
                                  'LookUp' : lambda x: x.nunique(),
                                  'Expected':sum,
                                  'Payments - All':sum,
                                  'Balance':sum })

facility_group.index = [facility_group.index.get_level_values(0), 
                        facility_group.index.get_level_values(1),
                        facility_group.index.get_level_values(2),
                        ['Total'] * len(facility_group)]
                                                                                               
#Create shift level dataframe
#Group by year, month, facility, and shift. Aggregate shifts and financials
shift_group = join_all.groupby([join_all['Final_Date'].dt.year.rename('year'), 
                              join_all['Final_Date'].dt.month.rename('month'), 
                              'Facility', 
                              'Shift']
                             ).agg({
                                  'LookUp' : lambda x: x.nunique(),
                                  'Expected':sum,
                                  'Payments - All':sum,
                                  'Balance':sum })

#concatenate dataframes to have shift level detail with monthly totals 
output = pd.concat([facility_group, shift_group]).sort_index(level=[0,1,2])

#rename columns for client
output = output.rename(columns = {'Index': 'Shift', 'LookUp':'Shift_Count'})
#%% Export Data

#output.to_csv('Output/output.csv')
#join_all.to_csv('Output/joined_databases.csv')


