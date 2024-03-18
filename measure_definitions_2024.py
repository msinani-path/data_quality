import sqlite3
from datetime import datetime,timedelta
import numpy as np
from statistics import mean, median
import csv
import numbers

####### program_id, department, region, and program type must be entered as lists. #######

### Client/Enrollment/Houshold Counts ###

def active_clients(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        # Creates a query parameter list ([?,?,?]) with the length of the number of program_ids passed in.
        placeholders = ','.join(['?' for _ in program_id])
        # Adds to the above query that checks if MergedProgramID is in the list of placeholders.
            # This will filter to include only results found in placeholders.
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        # Adds the new placeholders to the filter_params variable. 
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def new_clients(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate >= ? 
        AND Enrollment.EntryDate <= ?
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def active_enrollment(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT count(distinct Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    filter_params = [end_date, start_date]

    if program_id is not None:
        # Creates a query parameter list ([?,?,?]) with the length of the number of program_ids passed in.
        placeholders = ','.join(['?' for _ in program_id])
        # Adds to the above query that checks if MergedProgramID is in the list of placeholders.
            # This will filter to include only results found in placeholders.
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        # Adds the new placeholders to the filter_params variable. 
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def new_enrollment(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate >= ? 
        AND Enrollment.EntryDate <= ?
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def active_household(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        # Creates a query parameter list ([?,?,?]) with the length of the number of program_ids passed in.
        placeholders = ','.join(['?' for _ in program_id])
        # Adds to the above query that checks if MergedProgramID is in the list of placeholders.
            # This will filter to include only results found in placeholders.
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        # Adds the new placeholders to the filter_params variable. 
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def new_household(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate >= ? 
        AND Enrollment.EntryDate <= ?
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

### Demographics, Disabilities, and Other Vulnerable Populations ###

def race_ethnicity(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Enrollment.PersonalID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    race_sql = f'''
    SELECT
        SUM(CASE WHEN Client.AmIndAKNative = 1 THEN 1 ELSE 0 END) AS AmIndAKNativeCount,
        SUM(CASE WHEN Client.Asian = 1 THEN 1 ELSE 0 END) AS AsianCount,
        SUM(CASE WHEN Client.BlackAfAmerican = 1 THEN 1 ELSE 0 END) AS BlackAfAmericanCount,
        SUM(CASE WHEN Client.HispanicLatinaeo = 1 THEN 1 ELSE 0 END) AS HispanicLatinaeoCount,
        SUM(CASE WHEN Client.MidEastNAfrican = 1 THEN 1 ELSE 0 END) AS MidEastNAfricanCount,
        SUM(CASE WHEN Client.NativeHIPacific = 1 THEN 1 ELSE 0 END) AS NativeHIPacificCount,
        SUM(CASE WHEN Client.White = 1 THEN 1 ELSE 0 END) AS WhiteCount,
        SUM(CASE WHEN Client.AdditionalRaceEthnicity IS NOT NULL THEN 1 ELSE 0 END) AS AdditionalRaceEthnicityCount,
        SUM(CASE WHEN Client.RaceNone = 8 THEN 1 ELSE 0 END) AS ClientDoesNotKnowCount,
        SUM(CASE WHEN Client.RaceNone = 9 THEN 1 ELSE 0 END) AS ClientRefusedCount,
        SUM(CASE WHEN Client.RaceNone = 99 THEN 1 ELSE 0 END) AS DataNotCollectedCount
    FROM (
        {sql}
    ) AS subquery
    LEFT JOIN Client ON subquery.PersonalID = Client.PersonalID
    '''

    c.execute(race_sql, filter_params)
    
    race_counts = {}
    
    for row in c.fetchall():
        race_counts['American Indian, Alaska Native, or Indigenous']=row[0]
        race_counts['Asian or Asian American']= row[1]
        race_counts['Black, African American, or African']=row[2]
        race_counts['Hispanic/Latina/e/o']=row[3]
        race_counts['Middle Eastern or North African']=row[4]
        race_counts['Native Hawaiian or Pacific Islander']=row[5]
        race_counts['White']=row[6]
        race_counts['Other Race/Ethnicity']=row[7]
        race_counts['Client Does Not Know']=row[8]
        race_counts['Client Refused']=row[9]
        race_counts['Data Not Collected']=row[10]
    conn.close()

    race_counts_master = race_counts.copy()
    

    race_counts_unknown = race_counts_master.copy()
    race_counts_unknown['Unknown'] = race_counts['Client Does Not Know'] + race_counts['Client Refused'] + race_counts['Data Not Collected']

    [race_counts_unknown.pop(key) for key in ['Client Does Not Know','Client Refused','Data Not Collected']]


    return race_counts,race_counts_unknown

def gender_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Enrollment.PersonalID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    c.execute(sql, (end_date, start_date))
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sub = f'''
    SELECT
        SUM(CASE WHEN Client.Woman = 1 THEN 1 ELSE 0 END) AS WomanCount,
        SUM(CASE WHEN Client.Man = 1 THEN 1 ELSE 0 END) AS ManCount,
        SUM(CASE WHEN Client.NonBinary = 1 THEN 1 ELSE 0 END) AS NonBinary,
        SUM(CASE WHEN Client.CulturallySpecific = 1 THEN 1 ELSE 0 END) AS CulturallySpecific,        
        SUM(CASE WHEN Client.Transgender = 1 THEN 1 ELSE 0 END) AS Transgender,
        SUM(CASE WHEN Client.Questioning = 1 THEN 1 ELSE 0 END) AS Questioning,
        SUM(CASE WHEN Client.DifferentIdentity = 1 THEN 1 ELSE 0 END) AS DifferentIdentity,                
        SUM(CASE WHEN Client.GenderNone=8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,
        SUM(CASE WHEN Client.GenderNone=9 THEN 1 ELSE 0 END) AS ClientRefused,
        SUM( CASE WHEN Client.GenderNone=99 THEN 1 ELSE 0 END) AS DataNotCollected
    FROM (
        {sql}
    ) AS subquery
    LEFT JOIN Client ON subquery.PersonalID = Client.PersonalID
    '''

    c.execute(sub, filter_params)

    gender_counts = {}
    
    for row in c.fetchall():
        gender_counts['Woman']=row[0]
        gender_counts['Man']=row[1]
        gender_counts['Non-Binary']=row[2]
        gender_counts['Culturally Specific']=row[3]
        gender_counts['Transgender']=row[4]
        gender_counts['Questioning']=row[5]
        gender_counts['Different Identity']=row[6]
        gender_counts['Client Does Not Know']=row[7]
        gender_counts['Client Refused']=row[8]
        gender_counts['Data Not Collected']=row[9]
    conn.close()

    gender_counts_master = gender_counts.copy()
    

    gender_counts_unknown = gender_counts_master.copy()
    gender_counts_unknown['Unknown'] = gender_counts['Client Does Not Know'] + gender_counts['Client Refused'] + gender_counts['Data Not Collected']

    [gender_counts_unknown.pop(key) for key in ['Client Does Not Know','Client Refused','Data Not Collected']]


    return gender_counts, gender_counts_unknown

def age_bins_5y(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT Enrollment.PersonalID, MIN(Enrollment.EntryDate), Client.DOB, Client.DOBDataQuality,
            strftime('%Y', MIN(Enrollment.EntryDate)) - strftime('%Y', Client.DOB) - (strftime('%m-%d', MIN(Enrollment.EntryDate)) < strftime('%m-%d', Client.DOB))
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += ' GROUP BY Enrollment.PersonalID'  # Grouping by PersonalID to select only the first entry for each ID

    c.execute(sql, filter_params)
    rows = c.fetchall()

    age_categories = {
        "0-4": (0, 4),
        "5-9": (5, 9),
        "10-14": (10, 14),
        "15-19": (15, 19),
        "20-24": (20, 24),
        "25-29": (25, 29),
        "30-34": (30, 34),
        "35-39": (35, 39),
        "40-44": (40, 44),
        "45-49": (45, 49),
        "50-54": (50, 54),
        "55-59": (55, 59),
        "60-64": (60, 64),
        "65-69": (65, 69),
        "70-74": (70, 74),
        "75-79": (75, 79),
        "80-84": (80, 84),
        "85-90": (85, 90),
        "90+": (91, float('inf')),
    }

    age_counts = {category: 0 for category in age_categories}

    for row in rows:
        age = row[4]
        if age is not None:
            for category, (min_age, max_age) in age_categories.items():
                if min_age <= age <= max_age:
                    age_counts[category] += 1
    
    sub = f'''
    SELECT
        SUM(CASE WHEN Client.DOBDataQuality = 8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,
        SUM(CASE WHEN Client.DOBDataQuality = 9 THEN 1 ELSE 0 END) AS ClientRefused,
        SUM(CASE WHEN Client.DOBDataQuality = 99 THEN 1 ELSE 0 END) AS DataNotCollected   
    FROM Client
    INNER JOIN (
        {sql}
    ) AS subquery
    ON Client.PersonalID = subquery.PersonalID
'''
    c.execute(sub, filter_params)

    result = {}
    
    for row in c.fetchall():
        result['ClientDoesNotKnow'] = row[0]
        result['ClientRefused'] = row[1]
        result['DataNotCollected'] = row[2]

    conn.close()

    age_counts_master = age_counts.copy()
    age_counts.update(result)

    unknown = {}


    
    unknown['Unknown'] = result['ClientDoesNotKnow'] + result['ClientRefused'] + result['DataNotCollected']
    age_count_unknown = age_counts_master.copy()
    age_count_unknown.update(unknown)



    return age_counts, age_count_unknown

def age_list(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT strftime('%Y', MIN(Enrollment.EntryDate)) - strftime('%Y', Client.DOB) - (strftime('%m-%d', MIN(Enrollment.EntryDate)) < strftime('%m-%d', Client.DOB))
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += ' GROUP BY Enrollment.PersonalID'  # Grouping by PersonalID to select only the first entry for each ID

    c.execute(sql, filter_params)
    rows = c.fetchall()
    conn.close()

    ages = [row[0] for row in rows]

    return ages

def number_of_children(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(*) 
        FROM (
            SELECT strftime('%Y', MIN(Enrollment.EntryDate)) - strftime('%Y', Client.DOB) - (strftime('%m-%d', MIN(Enrollment.EntryDate)) < strftime('%m-%d', Client.DOB)) as age
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
            GROUP BY Enrollment.PersonalID
            HAVING age < 18
        ) AS subquery
    '''

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()

    return count

def transitional_aged_youth(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(*) 
        FROM (
            SELECT strftime('%Y', MIN(Enrollment.EntryDate)) - strftime('%Y', Client.DOB) - (strftime('%m-%d', MIN(Enrollment.EntryDate)) < strftime('%m-%d', Client.DOB)) AS age
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
            GROUP BY Enrollment.PersonalID
            HAVING age BETWEEN 18 and 24
        ) AS subquery
    '''

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()

    return count

def adult_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(*) 
        FROM (
            SELECT strftime('%Y', MIN(Enrollment.EntryDate)) - strftime('%Y', Client.DOB) - (strftime('%m-%d', MIN(Enrollment.EntryDate)) < strftime('%m-%d', Client.DOB)) AS age
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
            GROUP BY Enrollment.PersonalID
            HAVING age BETWEEN 25 and 65
        ) AS subquery
    '''

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()

    return count

def senior_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(*) 
        FROM (
            SELECT strftime('%Y', MIN(Enrollment.EntryDate)) - strftime('%Y', Client.DOB) - (strftime('%m-%d', MIN(Enrollment.EntryDate)) < strftime('%m-%d', Client.DOB)) AS age
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
            GROUP BY Enrollment.PersonalID
            HAVING age >= 65
        ) AS subquery
    '''

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()

    return count

def disability_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    # Function is mishandling multiple enrollments when responses vary between enrollments. 
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = f'''
    SELECT DISTINCT Enrollment.EnrollmentID, Enrollment.PersonalID, MAX(Enrollment.EntryDate) AS MaxEntry
    FROM Enrollment
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    WHERE Enrollment.EntryDate <= ? 
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]
    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
        SUM(CASE WHEN Disabilities.DisabilityType = 5 AND Disabilities.DisabilityResponse = 1 THEN 1 ELSE 0 END) AS PhysicalDisabilityCount,
        SUM(CASE WHEN Disabilities.DisabilityType = 5 AND Disabilities.IndefiniteAndImpairs = 1 THEN 1 ELSE 0 END) AS PhysicalDisabilityIndefiniteAndImpairsCount,
        
        SUM(CASE WHEN Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse = 1 THEN 1 ELSE 0 END) AS DevelopmentalDisabilityCount,
        
        SUM(CASE WHEN Disabilities.DisabilityType = 7 AND Disabilities.DisabilityResponse = 1 THEN 1 ELSE 0 END) AS ChronicHealthConditionCount,
        SUM(CASE WHEN Disabilities.DisabilityType = 7 AND Disabilities.IndefiniteAndImpairs = 1 THEN 1 ELSE 0 END) AS ChronicHealthConditionIndefiniteAndImpairsCount,     
    
        SUM(CASE WHEN Disabilities.DisabilityType = 8 AND Disabilities.DisabilityResponse = 1 THEN 1 ELSE 0 END) AS HIVAIDSCount,
        
        SUM(CASE WHEN Disabilities.DisabilityType = 9 AND Disabilities.DisabilityResponse = 1 THEN 1 ELSE 0 END) AS MentalHealthDisorderCount,
        SUM(CASE WHEN Disabilities.DisabilityType = 9 AND Disabilities.IndefiniteAndImpairs = 1 THEN 1 ELSE 0 END) AS MentalHealthDisorderIndefiniteAndImpairsCount,
        
        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse IN (1, 2, 3) THEN 1 ELSE 0 END) AS AnyDrugOrAlcoholUseDisorderCount,

        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse = 1 THEN 1 ELSE 0 END) AS AlcoholUseDisorderCount,
        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse = 1 AND Disabilities.IndefiniteAndImpairs = 1 THEN 1 ELSE 0 END) AS AlcoholUseIndefiniteAndImpairsCount,     

        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse = 2 THEN 1 ELSE 0 END) AS DrugUseDisorderCount,
        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse = 2 AND Disabilities.IndefiniteAndImpairs = 1 THEN 1 ELSE 0 END) AS DrugUseIndefiniteAndImpairsCount,     

        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse = 3 THEN 1 ELSE 0 END) AS BothAlcoholAndDrugUseDisorderCount,
        SUM(CASE WHEN Disabilities.DisabilityType = 10 AND Disabilities.DisabilityResponse = 3 AND Disabilities.IndefiniteAndImpairs = 1 THEN 1 ELSE 0 END) AS BothAlcoholAndDrugIndefiniteAndImpairsCount

    FROM Disabilities
    INNER JOIN (
        {sql} 
    ) AS subquery
    ON Disabilities.EnrollmentID = subquery.EnrollmentID 
    WHERE Disabilities.DataCollectionStage = 1

    '''

    c.execute(sub, filter_params)

    result = {}
    for row in c.fetchall():
        result['Physical Disability'] = row[0]
        result['Physical Disability, Severe'] = row[1]
        result['Developmental Disability'] = row[2]
        result['Chronic Health Condition'] = row[3]
        result['Chronic Health Condition, Severe'] = row[4]
        result['HIV/AIDS'] = row[5]
        result['Mental Health Disorder'] = row[6]
        result['Mental Health Disorder, Severe'] = row[7]
        result['Any Alcohol And Drug Use Disorder'] = row[8]
        result['Alcohol Use Disorder'] = row[9]
        result['Alcohol Use, Severe'] = row[10]
        result['Drug Use Disorder'] = row[11]
        result['Drug Use, Severe'] = row[12]
        result['Both Alcohol And Drug Use Disorder'] = row[13]
        result['Both Alcohol And Drug, Severe'] = row[14]

    conn.close()
    return result

def veteran_status(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT DISTINCT Enrollment.PersonalID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sub = f'''
            SELECT 
                SUM(CASE WHEN Client.VeteranStatus=0 THEN 1 ELSE 0 END) AS NotaVeteran,
                SUM(CASE WHEN Client.VeteranStatus=1 THEN 1 ELSE 0 END) AS Veteran,
                SUM(CASE WHEN Client.VeteranStatus=8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,
                SUM(CASE WHEN Client.VeteranStatus=9 THEN 1 ELSE 0 END) AS ClientRefused,
                SUM(CASE WHEN Client.VeteranStatus=99 THEN 1 ELSE 0 END) AS DataNotCollected
            FROM ({sql}) AS subquery
            LEFT JOIN Client ON subquery.PersonalID = Client.PersonalID
        '''    
    c.execute(sub, filter_params)
    
    result = {}
    
    for row in c.fetchall():
        result['Not a Veteran']=row[0]
        result['Veteran']=row[1]
        result['Client Does Not Know']=row[2]
        result['Client Refused']=row[3]
        result['Data Not Collected']=row[4]

    conn.close()
    return result

def dv_status(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    # Function is mishandling multiple enrollments when responses vary between enrollments. 
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = f'''
    SELECT DISTINCT Enrollment.EnrollmentID, Enrollment.PersonalID, MAX(Enrollment.EntryDate) AS MaxEntry
    FROM Enrollment
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    WHERE Enrollment.EntryDate <= ? 
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 0 THEN 1 ELSE 0 END) AS NotaDVSurvivor,
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 1 THEN 1 ELSE 0 END) AS DVSurvivor,
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 9 THEN 1 ELSE 0 END) AS ClientRefused,
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 99 THEN 1 ELSE 0 END) AS DataNotCollected,
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 1 AND HealthAndDV.CurrentlyFleeing = 1 THEN 1 ELSE 0 END) AS DVFleeing,
        SUM(CASE WHEN HealthAndDV.DomesticViolenceSurvivor = 1 AND HealthAndDV.CurrentlyFleeing = 0 THEN 1 ELSE 0 END) AS DVNotFleeing         
    FROM HealthAndDV
    INNER JOIN (
        {sql} 
    ) AS subquery
    ON HealthAndDV.EnrollmentID = subquery.EnrollmentID
    WHERE HealthAndDV.DataCollectionStage = 1
    '''

    c.execute(sub, filter_params)

    result = {}
    
    for row in c.fetchall():
        result['Not a Domestic Violence Survivor']=row[0]
        result['Domestic Violence Survivor']=row[1]
        result['Currently Fleeing Domestic Violence']=row[5]
        result['Not Currently Fleeing Domestic Violence']=row[6]
        result['Client Does Not Know']=row[2]
        result['Client Refused']=row[3]
        result['Data Not Collected']=row[4]

    conn.close()
    return result

def insurance_status(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    # Function is mishandling multiple enrollments when responses vary between enrollments. 
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = f'''
    SELECT DISTINCT Enrollment.EnrollmentID, Enrollment.PersonalID, MAX(Enrollment.EntryDate) AS MaxEntry
    FROM Enrollment
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    WHERE Enrollment.EntryDate <= ? 
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 THEN 1 ELSE 0 END) AS AnyInsurance,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.Medicaid=1 THEN 1 ELSE 0 END) AS Medicaid,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.Medicare=1 THEN 1 ELSE 0 END) AS Medicare,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.SCHIP=1 THEN 1 ELSE 0 END) AS SCHIP,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.VHAServices =1 THEN 1 ELSE 0 END) AS VHAServices,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.EmployerProvided=1 THEN 1 ELSE 0 END) AS EmployerProvided,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.COBRA=1 THEN 1 ELSE 0 END) AS COBRA,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.PrivatePay=1 THEN 1 ELSE 0 END) AS PrivatePay,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.StateHealthIns=1 THEN 1 ELSE 0 END) AS StateHealthIns,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.IndianHealthServices=1 THEN 1 ELSE 0 END) AS IndianHealthServices,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 1 AND  IncomeBenefits.OtherInsurance=1 THEN 1 ELSE 0 END) AS OtherInsurance,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource = 0 THEN 1 ELSE 0 END) AS NoInsurance,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource =8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource=9 THEN 1 ELSE 0 END) AS ClientRefused,
        SUM(CASE WHEN IncomeBenefits.InsuranceFromAnySource=99 THEN 1 ELSE 0 END) AS DataNotCollected  
    FROM IncomeBenefits
    INNER JOIN (
        {sql} 
    ) AS subquery
    ON IncomeBenefits.EnrollmentID = subquery.EnrollmentID
    WHERE IncomeBenefits.DataCollectionStage = 1
    '''

    c.execute(sub,filter_params)
    
    result={}
    
    for row in c.fetchall():
        result['Any Insurance']=row[0]     
        result['Medicaid']=row[1]
        result['Medicare']=row[2]
        result['SCHIP']=row[3]
        result['VHA Services']=row[4]
        result['Employer Provided']=row[5]
        result['COBRA']=row[6]
        result['Private Pay']=row[7]
        result['State Health Insurance']=row[8]
        result['Indian Health Services']=row[9]
        result['Other Insurance']=row[10]
        result['No Insurance']=row[11]
        result['Client Does Not Know']=row[12]
        result['Client Refused']=row[13]
        result['Data Not Collected']=row[14]
    conn.close()

    return result

def chronically_homeless_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN AdditionalInformation ON Enrollment.EnrollmentID = AdditionalInformation.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ChronicallyHomeless = 1
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    
    conn.close()
    return result

### Agency Indicators ###

def active_clients(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        # Creates a query parameter list ([?,?,?]) with the length of the number of program_ids passed in.
        placeholders = ','.join(['?' for _ in program_id])
        # Adds to the above query that checks if MergedProgramID is in the list of placeholders.
            # This will filter to include only results found in placeholders.
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        # Adds the new placeholders to the filter_params variable. 
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def new_clients(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate >= ? 
        AND Enrollment.EntryDate <= ?
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def php_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    move_in_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return {"Total PHP Count": move_in_count + exit_to_perm_dest_count,
            "Move-in Count": move_in_count,
            "Exit to Permanent Destination Count": exit_to_perm_dest_count} 

def total_php_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    move_in_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return move_in_count + exit_to_perm_dest_count


def movein_php_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    move_in_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return move_in_count

def exit_to_perm_php_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    move_in_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return exit_to_perm_dest_count

def served_within_x_days(start_date, end_date, days, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):   
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
    SELECT 
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN UpdateQuery.FirstServiceDate IS NOT NULL THEN Enrollment.PersonalID END) = 0 THEN 0
            ELSE COUNT(DISTINCT CASE WHEN (julianday(UpdateQuery.FirstServiceDate) - julianday(Enrollment.EntryDate)) <= ? THEN Enrollment.PersonalID END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN UpdateQuery.FirstServiceDate IS NOT NULL THEN Enrollment.PersonalID END), 0)
        END AS PercentageServedWithinXDays
    FROM Enrollment
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT *
        FROM (
        SELECT CustomClientServices.ClientProgramId as UpdateID, CustomClientServices.StartDate as FirstServiceDate,
        RANK() OVER (PARTITION BY CustomClientServices.ClientProgramId ORDER BY CustomClientServices.StartDate ASC) DateRank
        FROM CustomClientServices
        ORDER BY CustomClientServices.ClientProgramId, DateRank
        )
        WHERE DateRank = 1
        GROUP BY UpdateID
        ) as UpdateQuery ON Enrollment.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)    
    '''

    filter_params = [days, end_date, start_date] 

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]

    conn.close()

    return result

def any_income_increase_counts(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
    SELECT COUNT(DISTINCT IncomeBenefits.PersonalID)
    FROM IncomeBenefits
    LEFT JOIN Enrollment ON IncomeBenefits.EnrollmentID = Enrollment.EnrollmentID
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT * 
            FROM (
            SELECT IncomeBenefits.EnrollmentID AS UpdateID, IncomeBenefits.InformationDate AS UpdateDate, IncomeBenefits.TotalMonthlyIncome AS UpdateIncome, IncomeBenefits.DataCollectionStage AS UpdateStage,
            RANK() OVER (PARTITION BY IncomeBenefits.EnrollmentID ORDER BY IncomeBenefits.InformationDate DESC) DateRank
            FROM IncomeBenefits
            WHERE UpdateDate <= ?
            AND UpdateStage != 1)
        WHERE DateRank = 1
        GROUP BY UpdateID	
            )as UpdateQuery ON IncomeBenefits.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    AND IncomeBenefits.DataCollectionStage = 1
    AND UpdateQuery.UpdateDate BETWEEN ? AND ?
    AND UpdateQuery.UpdateIncome > IncomeBenefits.TotalMonthlyIncome 
    '''
    
    filter_params = [end_date, end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    
    return result

def earned_income_increase_counts(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
    SELECT COUNT(DISTINCT IncomeBenefits.PersonalID)
    FROM IncomeBenefits
    LEFT JOIN Enrollment ON IncomeBenefits.EnrollmentID = Enrollment.EnrollmentID
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT * 
            FROM (
            SELECT IncomeBenefits.EnrollmentID AS UpdateID, IncomeBenefits.InformationDate AS UpdateDate, IncomeBenefits.EarnedAmount AS UpdateEarnedIncome, IncomeBenefits.DataCollectionStage AS UpdateStage,
            RANK() OVER (PARTITION BY IncomeBenefits.EnrollmentID ORDER BY IncomeBenefits.InformationDate DESC) DateRank
            FROM IncomeBenefits
            WHERE UpdateDate <= ?
            AND UpdateStage != 1)
        WHERE DateRank = 1
        GROUP BY UpdateID	
            )as UpdateQuery ON IncomeBenefits.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    AND IncomeBenefits.DataCollectionStage = 1
    AND UpdateQuery.UpdateDate BETWEEN ? AND ?
    AND UpdateQuery.UpdateEarnedIncome > IncomeBenefits.EarnedAmount  
    '''
    
    filter_params = [end_date, end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    
    return result

def benefit_income_increase_counts(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    total_sql = '''
    SELECT COUNT(DISTINCT IncomeBenefits.PersonalID)
    FROM IncomeBenefits
    LEFT JOIN Enrollment ON IncomeBenefits.EnrollmentID = Enrollment.EnrollmentID
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT * 
            FROM (
            SELECT IncomeBenefits.EnrollmentID AS UpdateID, IncomeBenefits.InformationDate AS UpdateDate, IncomeBenefits.TotalMonthlyIncome AS UpdateIncome, IncomeBenefits.DataCollectionStage AS UpdateStage,
            RANK() OVER (PARTITION BY IncomeBenefits.EnrollmentID ORDER BY IncomeBenefits.InformationDate DESC) DateRank
            FROM IncomeBenefits
            WHERE UpdateDate <= ?
            AND UpdateStage != 1)
        WHERE DateRank = 1
        GROUP BY UpdateID	
            )as UpdateQuery ON IncomeBenefits.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    AND IncomeBenefits.DataCollectionStage = 1
    AND UpdateQuery.UpdateDate BETWEEN ? AND ?
    AND UpdateQuery.UpdateIncome > IncomeBenefits.TotalMonthlyIncome 
    '''

    earned_sql = '''
    SELECT COUNT(DISTINCT IncomeBenefits.PersonalID)
    FROM IncomeBenefits
    LEFT JOIN Enrollment ON IncomeBenefits.EnrollmentID = Enrollment.EnrollmentID
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT * 
            FROM (
            SELECT IncomeBenefits.EnrollmentID AS UpdateID, IncomeBenefits.InformationDate AS UpdateDate, IncomeBenefits.EarnedAmount AS UpdateEarnedIncome, IncomeBenefits.DataCollectionStage AS UpdateStage,
            RANK() OVER (PARTITION BY IncomeBenefits.EnrollmentID ORDER BY IncomeBenefits.InformationDate DESC) DateRank
            FROM IncomeBenefits
            WHERE UpdateDate <= ?
            AND UpdateStage != 1)
        WHERE DateRank = 1
        GROUP BY UpdateID	
            )as UpdateQuery ON IncomeBenefits.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    AND IncomeBenefits.DataCollectionStage = 1
    AND UpdateQuery.UpdateDate BETWEEN ? AND ?
    AND UpdateQuery.UpdateEarnedIncome > IncomeBenefits.EarnedAmount  
    '''

    filter_params = [end_date, end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        total_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        earned_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        total_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        earned_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        total_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        earned_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        total_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        earned_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(total_sql, filter_params)
    total_result = c.fetchone()[0]

    c.execute(earned_sql, filter_params)
    earned_result = c.fetchone()[0]
    
    benefit_income = total_result - earned_result

    return benefit_income

def personal_data_quality(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    # NEEDS FURTHER VALIDATION
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID) 
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND (Client.NameDataQuality > 1 OR 
                        (Client.SSNDataQuality > 2 OR (LENGTH(Client.SSN) <> 9) OR (LENGTH(Client.SSN) = 9 AND (
                                            Client.SSN LIKE '000000000'
                                            OR Client.SSN LIKE '111111111'
                                            OR Client.SSN LIKE '222222222'
                                            OR Client.SSN LIKE '333333333'
                                            OR Client.SSN LIKE '444444444'
                                            OR Client.SSN LIKE '555555555'
                                            OR Client.SSN LIKE '666666666'
                                            OR Client.SSN LIKE '777777777'
                                            OR Client.SSN LIKE '888888888'
                                            OR Client.SSN LIKE '999999999'
                                            OR Client.SSN LIKE '123456789'
                                            OR Client.SSN LIKE '234567890'
                                            OR Client.SSN LIKE '987654321')
                                        OR (SUBSTR(Client.SSN, 1, 3) IN ('000', '666', '900')
                                            OR SUBSTR(Client.SSN, 4, 6) = '00')
                            ) OR
                            (
                                (Client.DOBDataQuality IN (8, 9, 99))
                                OR Client.DOBDataQuality = 2
                                OR (
                                    Client.DOB < '1915-01-01' -- Prior to 1/1/1915
                                    OR Client.DOB > Enrollment.EntryDate -- After the record creation date
                                )
                            )
                            ) OR
                        (Client.RaceNone >= 8) OR
                        (Client.GenderNone >= 8)
            )
        '''

    sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
    """

    sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND Enrollment.DateOfEngagement <= ?
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    """

    filter_params = [end_date, start_date]
    outreach_params = [end_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
        outreach_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)
        outreach_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)
        outreach_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)
        outreach_params.extend(program_type)
        
    c.execute(sql, filter_params)
    total_score = c.fetchone()[0]
    
    c.execute(sql_active_outreach, outreach_params)
    total_outreach = c.fetchone()[0]
    
    c.execute(sql_active_non_outreach, filter_params)
    total_non_outreach = c.fetchone()[0]
    
    conn.close()

    total_enrollments = total_outreach + total_non_outreach

    if isinstance(total_score, numbers.Number):                       
        if total_enrollments > 0:
            if 1-(total_score/total_enrollments) <= 0:
                return 0
            else:
                return 1-(total_score/total_enrollments)
        else:
            return 0
    else:
        return None
        
def universal_data_quality(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    base_non_outreach_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID        
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
    '''   

    base_outreach_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID        
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND Enrollment.DateOfEngagement <= ?
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    '''   

    filter_params = [end_date, start_date]
    outreach_params = [end_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        base_non_outreach_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        base_outreach_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
        outreach_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        base_non_outreach_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        base_outreach_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)
        outreach_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        base_non_outreach_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        base_outreach_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)
        outreach_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        base_non_outreach_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        base_outreach_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)
        outreach_params.extend(program_type)

    c.execute(base_non_outreach_sql, filter_params)
    total_non_outreach_enrollments = c.fetchone()[0]

    c.execute(base_outreach_sql, outreach_params)
    total_outreach_enrollments = c.fetchone()[0]
        
    additional_query = '''
            AND (((Client.VeteranStatus = 8 OR Client.VeteranStatus = 9 ) AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18) OR
                ((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18) OR
                (Client.VeteranStatus = 1 AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 <18 ) OR
                (Enrollment.EntryDate > Exit.ExitDate) OR
                (Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL) OR
                (Enrollment.RelationshipToHoH NOT BETWEEN 1 AND 5) OR
                (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
                        SELECT 1 
                        FROM Enrollment 
                        WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                        AND Enrollment.RelationshipToHoH = 1 
                    )) 
                OR (Enrollment.RelationshipToHoH = 1 
                    AND EXISTS (
                        SELECT 1 
                        FROM Enrollment
                        WHERE Enrollment.HouseholdID = Enrollment.HouseholdID 
                        AND Enrollment.RelationshipToHoH = 1 
                        AND Enrollment.PersonalID != Enrollment.PersonalID
                    )) OR
                (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NULL) OR
                (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NOT NULL AND Enrollment.EnrollmentCoC NOT IN 
                    ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-606', 'CA-612', 'CA-614', 'CA-607')) OR
                (Enrollment.DisablingCondition=8 OR Enrollment.DisablingCondition=9) OR
                (Enrollment.DisablingCondition=99 OR Enrollment.DisablingCondition IS NULL)
                )    
        '''
    
    sql_non_outreach = base_non_outreach_sql + additional_query
    sql_outreach = base_outreach_sql + additional_query

    c.execute(sql_non_outreach, filter_params)
    total_non_outreach_score = c.fetchone()[0]    

    c.execute(sql_outreach, outreach_params)
    total_outreach_score = c.fetchone()[0]

    conn.close()

    total_enrollments = total_outreach_enrollments + total_non_outreach_enrollments
    total_score = total_outreach_score + total_non_outreach_score


    if isinstance(total_score, numbers.Number):                       
        if total_enrollments > 0:
            return 1-(total_score/total_enrollments)
        else:
            return 0
    else:
        return None

### Program Type Indicators ###

def percent_engaged(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    engaged_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement IS NOT NULL
        AND Enrollment.DateOfEngagement <= ?
        AND Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    '''
    
    engaged_filter_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        engaged_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        engaged_filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        engaged_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        engaged_filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        engaged_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        engaged_filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        engaged_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        engaged_filter_params.extend(program_type)
    
    c.execute(engaged_sql, engaged_filter_params)
    
    engaged_count = c.fetchone()[0]

    total_enrolled_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'

    '''
    
    total_enrolled_filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        total_enrolled_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        total_enrolled_filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        total_enrolled_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        total_enrolled_filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        total_enrolled_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        total_enrolled_filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        total_enrolled_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        total_enrolled_filter_params.extend(program_type)
    
    c.execute(total_enrolled_sql, total_enrolled_filter_params)
    
    enrolled_count = c.fetchone()[0]

    conn.close()
    
    if enrolled_count == 0:
        return None

    return engaged_count / enrolled_count

def percent_exits_to_positive_destination_outreach(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    positive_destinations = (101, 118, 215, 204, 205, 225, 314, 312, 313, 302, 327, 332, 426, 411, 421, 410, 435, 422, 423)

    sql = '''
        SELECT
            (SUM(CASE WHEN Exit.Destination IN ({}) THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS PercentagePositiveExits
        FROM Exit
        LEFT JOIN Enrollment ON Exit.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 329, 24)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    '''.format(','.join(map(str, positive_destinations)))
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)
    
    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)


    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    
    conn.close()

    return result
   
def percent_exits_to_permanent_destination_outreach(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT
            (SUM(CASE WHEN Exit.Destination > 400 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS PercentagePermExits
        FROM Exit
        LEFT JOIN Enrollment ON Exit.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 329, 24)
        AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)


    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    
    conn.close()

    return result

def percent_exits_to_homelessness_habitation(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT
            (SUM(CASE WHEN Exit.Destination = 116 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS PercentageHomelessExits
        FROM Exit
        LEFT JOIN Enrollment ON Exit.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 329, 24)
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    
    conn.close()

    return result

def utilization_rate(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return
 
def length_of_stay(start_date, end_date, result_type='both', program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT julianday(COALESCE(Exit.ExitDate,DATE('now'))) - julianday(Enrollment.EntryDate) AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(sql, filter_params)
    rows = c.fetchall()
    conn.close()

    length_of_stays = [row[0] for row in rows]

    if not length_of_stays:
        return
    elif result_type == 'mean':
        return mean(length_of_stays)
    elif result_type == 'median':
        return median(length_of_stays)
    elif result_type == 'both':
        return {"Average": mean(length_of_stays), "Median": median(length_of_stays)}
    elif result_type == 'list':
        return length_of_stays
    else:
        raise ValueError("Invalid result_type parameter. Use 'mean', 'median', 'both', or 'list'.")

def days_to_permanent_destination(start_date, end_date, result_type='both', program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    exit_to_perm_dest_sql = '''
        SELECT 
            CASE
                WHEN julianday(Exit.ExitDate) - julianday(Enrollment.EntryDate) < 0 THEN 0
                ELSE julianday(Exit.ExitDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    exit_to_perm_dest_sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchall()
    
    days_to_exit_to_perm_dest_list = [row[0] for row in exit_to_perm_dest_count if row[0] is not None]
    
    conn.close()

    if not days_to_exit_to_perm_dest_list:
        return
    elif result_type == 'mean':
        return mean(days_to_exit_to_perm_dest_list)
    elif result_type == 'median':
        return median(days_to_exit_to_perm_dest_list)
    elif result_type == 'both':
        return {"Average": mean(days_to_exit_to_perm_dest_list), "Median": median(days_to_exit_to_perm_dest_list)}
    elif result_type == 'list':
        return days_to_exit_to_perm_dest_list
    else:
        raise ValueError("Invalid result_type parameter. Use 'mean', 'median', 'both', or 'list'.")

def percent_exits_to_permanent_destination(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT
            (SUM(CASE WHEN Exit.Destination > 400 THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS PercentagePermExits
        FROM Exit
        LEFT JOIN Enrollment ON Exit.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 215, 225, 24)
        AND PATHProgramMasterList.PATHProgramType IS NOT 'Outreach Services'
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)


    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    
    conn.close()

    return result

def percent_document_ready(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 

def days_to_php(start_date, end_date, result_type='both', program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    move_in_sql = '''
        SELECT 
            CASE
                WHEN julianday(Enrollment.MoveInDate) - julianday(Enrollment.EntryDate) < 0 THEN 0
                ELSE julianday(Enrollment.MoveInDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    exit_to_perm_dest_sql = '''
        SELECT 
            CASE
                WHEN julianday(Exit.ExitDate) - julianday(Enrollment.EntryDate) < 0 THEN 0
                ELSE julianday(Exit.ExitDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    move_in_sql += ' GROUP BY Enrollment.PersonalID'
    exit_to_perm_dest_sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchall()
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchall()
    
    days_to_move_in_list = [row[0] for row in move_in_count if row[0] is not None]
    days_to_exit_to_perm_dest_list = [row[0] for row in exit_to_perm_dest_count if row[0] is not None]

    days_to_php_list = days_to_move_in_list + days_to_exit_to_perm_dest_list

    conn.close()

    if not days_to_php_list:
        return 
    elif result_type == 'mean':
        return mean(days_to_php_list)
    elif result_type == 'median':
        return median(days_to_php_list)
    elif result_type == 'both':
        return {"Average": mean(days_to_php_list), "Median": median(days_to_php_list)}
    elif result_type == 'list':
        return days_to_php_list
    else:
        raise ValueError("Invalid result_type parameter. Use 'mean', 'median', 'both', or 'list'.")

def days_to_move_in(start_date, end_date, result_type='both', program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    move_in_sql = '''
        SELECT 
            CASE
                WHEN julianday(Enrollment.MoveInDate) - julianday(Enrollment.EntryDate) < 0 THEN 0
                ELSE julianday(Enrollment.MoveInDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    move_in_sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchall()
    
    days_to_move_in_list = [row[0] for row in move_in_count if row[0] is not None]

    conn.close()

    if not days_to_move_in_list:
        return
    elif result_type == 'mean':
        return mean(days_to_move_in_list)
    elif result_type == 'median':
        return median(days_to_move_in_list)
    elif result_type == 'both':
        return {"Average": mean(days_to_move_in_list), "Median": median(days_to_move_in_list)}
    elif result_type == 'list':
        return days_to_move_in_list
    else:
        raise ValueError("Invalid result_type parameter. Use 'mean', 'median', 'both', or 'list'.")

def percent_housed_within_x_days(start_date, end_date, days, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    move_in_sql = '''
        SELECT 
            CASE
                WHEN julianday(Enrollment.MoveInDate) - julianday(Enrollment.EntryDate) < 0 THEN 0
                ELSE julianday(Enrollment.MoveInDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        move_in_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        move_in_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        move_in_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    move_in_sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchall()
    
    days_to_move_in_list = [row[0] for row in move_in_count if row[0] is not None]

    conn.close()

    total_move_ins = len(days_to_move_in_list)
    count_housed_within_x_days = sum(1 for numdays in days_to_move_in_list if numdays <= days)
    
    if total_move_ins > 0:
        return (count_housed_within_x_days / total_move_ins)
    else:
        return None

def percent_exit_to_nonpermanent_destinations(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    nonperm_destinations = (101, 118, 215, 204, 205, 225, 314, 312, 313, 302, 327, 332)

    sql = '''
        SELECT
            (SUM(CASE WHEN Exit.Destination IN ({}) THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS PercentageNonpermExits
        FROM Exit
        LEFT JOIN Enrollment ON Exit.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 215, 225, 24)
    '''.format(','.join(map(str, nonperm_destinations)))    

    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)


    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    
    conn.close()

    return result

def percent_exits_to_homelessness(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT
            (SUM(CASE WHEN Exit.Destination IN (101, 116, 118) THEN 1 ELSE 0 END) * 1.0) / COUNT(*) AS PercentageHomelessExits
        FROM Exit
        LEFT JOIN Enrollment ON Exit.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 215, 225, 24)
        AND PATHProgramMasterList.PATHProgramType IS NOT 'Outreach Services'
        '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
        
    conn.close()

    return result

def percent_with_hsp_within_x_days(start_date, end_date, days, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 

def percent_income_increase(start_date, end_date, income_type='all', program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    any_income_sql = '''
    SELECT 
        COUNT(DISTINCT CASE WHEN UpdateQuery.UpdateIncome > IncomeBenefits.TotalMonthlyIncome THEN IncomeBenefits.PersonalID END) * 1.0 / COUNT(DISTINCT Enrollment.PersonalID) AS PercentageWithIncomeIncrease
    FROM IncomeBenefits
    LEFT JOIN Enrollment ON IncomeBenefits.EnrollmentID = Enrollment.EnrollmentID
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT * 
        FROM (
        SELECT IncomeBenefits.EnrollmentID AS UpdateID, IncomeBenefits.InformationDate AS UpdateDate, IncomeBenefits.TotalMonthlyIncome AS UpdateIncome, IncomeBenefits.DataCollectionStage AS UpdateStage,
        RANK() OVER (PARTITION BY IncomeBenefits.EnrollmentID ORDER BY IncomeBenefits.InformationDate DESC) DateRank
        FROM IncomeBenefits
        WHERE UpdateDate <= ?
        AND UpdateStage != 1)
        WHERE DateRank = 1
        GROUP BY UpdateID	
        ) as UpdateQuery ON IncomeBenefits.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    AND IncomeBenefits.DataCollectionStage = 1
    '''

    earned_income_sql = '''
    SELECT 
        COUNT(DISTINCT CASE WHEN UpdateQuery.UpdateEarnedIncome > IncomeBenefits.EarnedAmount THEN IncomeBenefits.PersonalID END) * 1.0 / COUNT(DISTINCT Enrollment.PersonalID) AS DecimalPercentageWithEarnedIncomeIncrease
    FROM IncomeBenefits
    LEFT JOIN Enrollment ON IncomeBenefits.EnrollmentID = Enrollment.EnrollmentID
    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
    LEFT JOIN 
        (SELECT * 
        FROM (
        SELECT IncomeBenefits.EnrollmentID AS UpdateID, IncomeBenefits.InformationDate AS UpdateDate, IncomeBenefits.EarnedAmount AS UpdateEarnedIncome, IncomeBenefits.DataCollectionStage AS UpdateStage,
        RANK() OVER (PARTITION BY IncomeBenefits.EnrollmentID ORDER BY IncomeBenefits.InformationDate DESC) DateRank
        FROM IncomeBenefits
        WHERE UpdateDate <= ?
        AND UpdateStage != 1)
        WHERE DateRank = 1
        GROUP BY UpdateID	
        ) as UpdateQuery ON IncomeBenefits.EnrollmentID = UpdateQuery.UpdateID
    WHERE Enrollment.EntryDate <= ?
    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    AND IncomeBenefits.DataCollectionStage = 1
    '''
    
    filter_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        any_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        any_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        any_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        any_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(any_income_sql, filter_params)
    any_income_percentage = c.fetchone()[0]
    
    c.execute(earned_income_sql, filter_params)
    earned_income_percentage= c.fetchone()[0]
    

    conn.close()

    if any_income_percentage is None or earned_income_percentage is None:
        return None
    
    benefits_income_percentage = any_income_percentage - earned_income_percentage
    
    if any_income_percentage == 0:
        return 0
    elif income_type=="any":
        return any_income_percentage
    elif income_type=="earned":
        return earned_income_percentage
    elif income_type=="benefit":
        return benefits_income_percentage
    elif income_type=="all":
        return (any_income_percentage, earned_income_percentage, benefits_income_percentage)
    else:
        raise ValueError("Invalid result_type parameter. Use 'any', 'earned', 'benefit', or 'all'.")

def retention(start_date, end_date, retention_period, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    retention_date = f'DATE(Enrollment.MoveInDate, "+{retention_period} days")'

    numerator_sql = f'''
        SELECT COUNT (DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services') 
        AND Enrollment.MoveInDate <= ?     
        AND {retention_date} <= ?
        AND (
            (Exit.ExitDate IS NULL) OR
            (Exit.ExitDate >= {retention_date}) OR
            (Exit.ExitDate < {retention_date} AND Exit.Destination >= 400)
        )
        AND (
            Exit.Destination NOT IN ('206', '215', '225', '24') OR
            Exit.ExitDate IS NULL
        )
    '''

    numerator_filter_params = [end_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        numerator_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        numerator_filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        numerator_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        numerator_filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        numerator_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        numerator_filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        numerator_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        numerator_filter_params.extend(program_type)

    c.execute(numerator_sql, numerator_filter_params)
    numerator = c.fetchone()[0]

    denominator_sql = f'''
        SELECT COUNT (DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services') 
        AND Enrollment.MoveInDate <= ?     
        AND {retention_date} <= ?
        AND (
            Exit.Destination NOT IN ('206', '215', '225', '24') OR
            Exit.ExitDate IS NULL
        )
    '''

    denominator_filter_params = [end_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        denominator_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        denominator_filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        denominator_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        denominator_filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        denominator_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        denominator_filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        denominator_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        denominator_filter_params.extend(program_type)


    c.execute(denominator_sql, denominator_filter_params)
    denominator = c.fetchone()[0]

    if denominator > 0:
        retention_rate = numerator/denominator
        return retention_rate
    else:
        return None

def cc_indicators(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 

def es_indicators(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 

def hpp_indicators(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 

def bh_indicators(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 

def ac_indicators(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
    return 


### Currently Unused ###   

def exit_destinations(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT Enrollment.EnrollmentID, Enrollment.PersonalID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)
        
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination IN (116,101,118) THEN 1 ELSE 0 END) AS ReturnedToHomelessness,
        SUM(CASE WHEN Exit.Destination IN (215,206,207,225,204,205) THEN 1 ELSE 0 END) AS Institutional,
        SUM(CASE WHEN Exit.Destination IN (302,329,314,332,312,313,327,336,335) THEN 1 ELSE 0 END) AS Temporary,       
        SUM(CASE WHEN Exit.Destination IN (422,423,426,410,435,421,411) THEN 1 ELSE 0 END) AS Permanent,
        SUM(CASE WHEN Exit.Destination IN (24,30,17,37) THEN 1 ELSE 0 END) AS Other,         
        SUM(CASE WHEN Exit.Destination = 8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,        
        SUM(CASE WHEN Exit.Destination = 9 THEN 1 ELSE 0 END) AS ClientRefused,
        SUM(CASE WHEN Exit.Destination = 99 THEN 1 ELSE 0 END) AS DatNotCollected
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.EnrollmentID = subquery.EnrollmentID
    '''

    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['Total Exits']=row[0]+row[1]+row[2]+row[3]+row[4]+row[5]+row[6]+row[7]
        result['Homeless Situations'] = row[0], row[0]/result['Total Exits']
        result['Institutional Situations'] = row[1], row[1]/result['Total Exits']
        result['Temporary Housing Situations'] = row[2], row[2]/result['Total Exits']
        result['Permanent Housing Situations'] = row[3], row[3]/result['Total Exits']
        result['Other'] = row[4], row[4]/result['Total Exits']
        result['ClientDoesNotKnow'] = row[5], row[5]/result['Total Exits']
        result['ClientRefused'] = row[6], row[6]/result['Total Exits']
        result['DataNotCollected'] = row[7], row[7]/result['Total Exits']

    conn.close()
    return result

def return_to_homelessness_habitation_only(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    sql = '''
        SELECT Enrollment.PersonalID, Enrollment.EnrollmentID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate BETWEEN ? AND ?
        AND Exit.Destination NOT IN (206, 329, 24)
        AND PATHProgramMasterList.PATHProgramType IS NOT 'Outreach Services'
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)
    
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.DestinationIN (116, 101, 118) THEN 1 ELSE 0 END) AS Homeless,
        SUM(CASE WHEN Exit.Destination IN (215,204,205,225,314,312,313,302,327,332,426,411,421,410,435,422,423,207,8,9,99,30,17) THEN 1 ELSE 0 END) AS Other,
        Exit.PersonalID
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.EnrollmentID = subquery.EnrollmentID
    '''

    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['Total Exits']=row[0]+row[1]
        result['Returns to Homelessness'] = row[0], row[0]/result['Total Exits']

    conn.close()
    return result

### Helper Functions ###

def shorten_and_format(input_list):
    name_replacements = {
        "Los Angeles County": "LA",
        "San Diego County": "SD",
        "Orange County": "OC",
        "Santa Clara County": "SCC",
        "Santa Barbara County": "SB",
        "Los Angeles": "LA",
        "San Diego": "SD",
        "Orange County": "OC",
        "Santa Clara": "SCC",
        "Santa Barbara": "SB",
        "Families": "FAM",
        "Metro LA": "MET",
        "Permanent Supportive Services": "PSS",
        "South County": "SC",
        "Veterans": "VET",
        "West LA": "WLA"
    }
    
    for i in range(len(input_list)):
        for j in range(len(input_list[i])):
            original_value = input_list[i][j]
            replacement = name_replacements.get(original_value, original_value)
            input_list[i][j] = replacement
    return input_list

def all_programs_dict(db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Region, Department, PATHProgramType, ProgramName, MergedProgramID, 
        DataSystemProgramName, PrimaryDataSystem, SecondaryDataSystem, DataSystemID, GrantCode, ContractTerm
        FROM PATHProgramMasterList
    '''
    c.execute(sql)

    desc = c.description
    column_names = [col[0] for col in desc]
    raw_dicts = [dict(zip(column_names, row))  
            for row in c.fetchall()]
    
    conn.close()


    program_dict = {}
    for row in raw_dicts:
        Region = row['Region']
        Department = row['Department']
        PATHProgramType = row['PATHProgramType']
        ProgramName = row['ProgramName']
        MergedProgramID = row['MergedProgramID']
        DataSystemProgramName = row['DataSystemProgramName']
        PrimaryDataSystem = row['PrimaryDataSystem']
        SecondaryDataSystem = row['SecondaryDataSystem']
        DataSystemID = row['DataSystemID']
        GrantCode = row['GrantCode']
        ContractTerm = row['ContractTerm']

        if Region not in program_dict:
            program_dict[Region] = {}

        if Department not in program_dict[Region]:
            program_dict[Region][Department] = {}

        if PATHProgramType not in program_dict[Region][Department]:
            program_dict[Region][Department][PATHProgramType] = {}

        if ProgramName not in program_dict[Region][Department][PATHProgramType]:
            program_dict[Region][Department][PATHProgramType][ProgramName] = {}

        if MergedProgramID not in program_dict[Region][Department][PATHProgramType][ProgramName]:
            program_dict[Region][Department][PATHProgramType][ProgramName][MergedProgramID] = {}

        # only works if each MergedProgramID contains one version of the dictionary below 
        program_dict[Region][Department][PATHProgramType][ProgramName][MergedProgramID].update({
            'DataSystemProgramName': DataSystemProgramName,
            'PrimaryDataSystem': PrimaryDataSystem,
            'SecondaryDataSystem': SecondaryDataSystem,
            'DataSystemID': DataSystemID,
            'GrantCode': GrantCode,
            'ContractTerm': dict(contract.split(':') for contract in ContractTerm.split(","))
            
        })

    return program_dict
    
def total_exits(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Exit.ExitDate >= ? 
        AND Exit.ExitDate <= ?
    '''
    
    filter_params = [start_date, end_date]

    if program_id is not None:
        # Creates a query parameter list ([?,?,?]) with the length of the number of program_ids passed in.
        placeholders = ','.join(['?' for _ in program_id])
        # Adds to the above query that checks if MergedProgramID is in the list of placeholders.
            # This will filter to include only results found in placeholders.
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        # Adds the new placeholders to the filter_params variable. 
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(sql, filter_params)

    result = c.fetchone()[0]
    conn.close()
    return result

def doc_ready(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    ssn = '''
        SELECT DISTINCT DocReady.EnrollmentID
        FROM DocReady
        LEFT JOIN Enrollment ON DocReady.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Exit ON DocReady.EnrollmentID =Exit.EnrollmentID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing ServiceS','Interim Housing Services', 'Housing Navigation Services') 
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND DocReady.SSCard=1
        '''
    dl= '''
        SELECT DISTINCT DocReady.EnrollmentID
        FROM DocReady
        LEFT JOIN Enrollment ON DocReady.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Exit ON DocReady.EnrollmentID =Exit.EnrollmentID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing ServiceS','Interim Housing Services', 'Housing Navigation Services') 
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND DocReady.DriverLicense=1
        '''
    
    total= '''
        SELECT COUNT( DISTINCT DocReady.EnrollmentID)
        FROM DocReady
        LEFT JOIN Enrollment ON DocReady.EnrollmentID = Enrollment.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Exit ON DocReady.EnrollmentID =Exit.EnrollmentID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing ServiceS','Interim Housing Services', 'Housing Navigation Services') 
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        '''
    
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        ssn += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        dl += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        total += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        ssn += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        dl += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        total += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        ssn += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        dl += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        total += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        ssn += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        dl += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        total += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(ssn, filter_params)
    ssn_result = set(enrollment_id[0] for enrollment_id in c.fetchall())

    c.execute(dl, filter_params)
    dl_result = set(enrollment_id[0] for enrollment_id in c.fetchall())

    # Find common EnrollmentIDs using set intersection
    common_enrollment_ids = list(ssn_result.intersection(dl_result))
    c.execute(total,filter_params)
    total=c.fetchone()[0]

    conn.close()

    return len(common_enrollment_ids)/total

def hsp_plan_within_x_days(start_date, end_date,days, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT 
            CASE
                WHEN julianday(CustomClientServices.StartDate) - julianday(Enrollment.EntryDate)< 0 THEN 0
                ELSE julianday(CustomClientServices.StartDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN CustomClientServices ON Enrollment.PersonalID = CustomClientServices.ClientID
        INNER JOIN HSPPlan ON Enrollment.PersonalID=HSPPlan.PersonalID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (CustomClientServices.ServiceItemName='Intake and Assessment')
        AND (HSPPlan.HSPPlan is not NULL)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(sql, filter_params)
    hsp_days = c.fetchall()
    
    days_to_hsp_list = [row[0] for row in hsp_days if row[0] is not None]

    conn.close()

    total_hsp = len(days_to_hsp_list)
    count_hsp_within_x_days = sum(1 for numdays in days_to_hsp_list if numdays <= days)
    
    if total_hsp > 0:
        return (count_hsp_within_x_days / total_hsp)
    else:
        return 0

def hsp_LA_plan_within_x_days(start_date, end_date,days, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT 
            CASE
                WHEN julianday(CustomClientServices.StartDate) - julianday(Enrollment.EntryDate)< 0 THEN 0
                ELSE julianday(CustomClientServices.StartDate) - julianday(Enrollment.EntryDate)
            END AS DaysBetween
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.PersonalID = Exit.PersonalID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN CustomClientServices ON Enrollment.PersonalID = CustomClientServices.ClientID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND CustomClientServices.ServiceItemName IN ('Housing and Services Plan','Housing and Service Plan')
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    sql += ' GROUP BY Enrollment.PersonalID'

    c.execute(sql, filter_params)
    hsp_days = c.fetchall()
    
    days_to_hsp_list = [row[0] for row in hsp_days if row[0] is not None]

    conn.close()

    total_hsp = len(days_to_hsp_list)
    count_hsp_within_x_days = sum(1 for numdays in days_to_hsp_list if numdays <= days)
    
    if total_hsp > 0:
        return (count_hsp_within_x_days / total_hsp)
    else:
        return 0