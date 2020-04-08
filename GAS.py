# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd

staff = pd.read_csv('SHC 12 Mo Staffing 2.csv', encoding="ISO-8859-1")
collections = pd.read_csv('SHC 12 Mo Collections.csv', encoding="ISO-8859-1")

#delete empty columns
staff = staff[['User','Date','Shift']]

#Seperate Names
staff[['Last Name', 'First Name']] = staff['User'].str.split(',', expand = True)
staff['Last Name'] = staff['Last Name'].str.upper()
collections[['Last Name', 'First Name']] = collections['Rendering Provider'].str.split(',', expand = True)

#Clean Names
collections['First Name'] = collections['First Name'].str.strip()
staff['First Name'] = staff['First Name'].str.strip()

#Ryan G
staff.loc[staff['User'] == 'Ryan, G.', 'Last Name'] = 'RYANG'
collections.loc[collections['Rendering Provider'] == 'RYAN, WILLIAM G', 'Last Name'] = 'RYANG'

#Ryan C
staff.loc[staff['User'] == 'Ryan, C.', 'Last Name'] = 'RYANC'
collections.loc[collections['Rendering Provider'] == 'RYAN, CRAIG', 'Last Name'] = 'RYANC'

# Create Lookup Column
collections['LookUp'] = collections['Last Name'] + '_' + collections['Date of Service - Case']
staff['LookUp'] = staff['Last Name'] + '_' + staff['Date']

#Remove Call shifts
noCallShifts = staff[staff.Shift.str.contains('Call') == False]
#callShifts = staff[staff.Shift.str.contains('Call') == True]
weCallShifts = staff[staff.Shift.str.contains('Call WE | WE Call') == True]
staffShiftCleaned = noCallShifts.append(weCallShifts)

#Remove unneded columns from  staff
staffClean = staffShiftCleaned[['User','Date','Shift','LookUp']]

#Find all duplicated shifts
duplicatedShifts = staffClean[staffClean.duplicated(['LookUp'], keep = False)]
duplicatedShifts = duplicatedShifts.assign(collections=duplicatedShifts.LookUp.isin(collections.LookUp).astype(str))
duplicatedShifts.rename(columns={'collections':'Provider has Cases on Date'}, inplace = True)
duplicatedShifts.drop(columns = 'LookUp', inplace = True)

#Join spreadsheets
join = pd.merge(collections,staffClean,on='LookUp', how = 'outer')

#identify missing data
nanCount = join.isna().sum()
emptyShifts = join[join['Case ID'].isnull()]
emptyCases = join[join['Shift'].isnull()]

#remove nan cells
joinCleaned = join.dropna(axis ='rows')

#Sum payment on facility and shift
expectedShift = joinCleaned.groupby(['Facility','Shift'])['Expected'].sum()
paymentsShift = joinCleaned.groupby(['Facility','Shift'])['Payments - All'].sum()
balanceShift = joinCleaned.groupby(['Facility','Shift'])['Balance'].sum()

output = joinCleaned.groupby(['Facility','Shift'])['Expected','Payments - All','Balance'].sum()

#Export Data
emptyShifts.loc[:,'User':'Shift'].to_csv('Empty_Shifts.csv',index = False)
emptyCases.loc[:,'Case ID':'Balance'].to_csv('Cases_WithOut_Shifts.csv', index = False)
duplicatedShifts.to_csv('Duplicated_Shifts.csv', index = False)
output.to_csv('Sum_By_Shift_By_Facility.csv')

# expectedShift.to_csv('Excpected.csv')
# paymentsShift.to_csv('Payment.csv')
# balanceShift.to_csv('Balance.csv')
