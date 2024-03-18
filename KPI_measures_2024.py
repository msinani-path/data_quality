import sqlite3
from datetime import datetime,timedelta
import numpy as np
from datetime import datetime


### We don't want these variables to accidentally be called by one of these functions 
#start_date = '2022-07-01'
#end_date = '2023-06-30'


#KPI-0094 & PTI#170201, #155001, #160114, #170101 #853002
def contracted_to_serve(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#558001
def contracted_to_serve_specific_location(start_date, location,end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    if location == ['Antelope Valley']:
        zipcodes=[92397, 93510, 93523, 93534, 93535, 93536, 93543, 93544, 93550, 93551, 93552, 93553, 93591]
    elif location == ['Central SPA']:
        zipcodes=[92626,92627,92628,92707,92704,92704,92708,92728,92840,92841,92842,92843,92844,92845,92846,92646,92647,92648,92649,92657,92658,92659,92660,92661,92662,92663,92701,92702,92703,92704,92705,92706,92707,92708,92799,92711,92712,90740,92780,92781,92782,92683,92684,92685]
    elif location == ['South SPA']:
        zipcodes=[92653,92656,92624,92629,92602,92603,92604,92606,92612,92616,92617,92618,92619,92620,92621,92623,92651,92652,92653,92654,92655,92656,92677,92637,92653,92656,92677,92637,92653,92653,92656,92677,92637,92653,92610,92630,92676,92675,92691,92692,92694,92688,92675,92672,92673,92674,92691,92693,92692]
    elif location == ['North SPA']:
        zipcodes=[92801,92802,92803,92804,92805,92806,92807,92808,92809,92812,92821,92822,92823,90620,90621,92822,90623,92624,90720,90630,92831,92832,92833,92834,92835,92836,92837,92838,90631,90632,90633,90623,90720,90721,92861,92862,92863,92864,92865,92866,92867,92868,92869,92870,92871,90680,92861,92867,92885,92886,92887]
    else:
        zipcodes=[]
    

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN ProjectCoC ON Enrollment.ProjectID = ProjectCoC.ProjectID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ProjectCoC.ZIP IN ({})
    '''.format(','.join('?' for _ in zipcodes))
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

def contracted_to_serve_antelope_valley(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID), Enrollment.ProjectID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Enrollment.ProjectID =ProjectCoC.ProjectID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ProjectCoC.ZIP IN (92397, 93510, 93523, 93534, 93535, 93536, 93543, 93544, 93550, 93551, 93552, 93553, 93591)
        
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)

    result = c.fetchone()[0]

    conn.close()
    return result

def contracted_to_serve_central_spa(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    central_zip=[]
    
    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID), Enrollment.ProjectID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Enrollment.ProjectID =ProjectCoC.ProjectID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ProjectCoC.ZIP IN (92397, 93510, 93523, 93534, 93535, 93536, 93543, 93544, 93550, 93551, 93552, 93553, 93591)
        
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#KPI-0055 & Performance Target ID#74007, PTI#256001 #805006 #664006
def exit_to_permanent_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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
        
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT    
        SUM(CASE WHEN Exit.Destination IN (422,423,426,410,435,421,411) THEN 1 ELSE 0 END) AS Permanent,
        SUM(CASE WHEN Exit.Destination IN (116,101,118,215,206,207,225,204,205,302,329,314,332,312,313,327,336,335,24,30,17,37,8,9,99) THEN 1 ELSE 0 END) AS Other       
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.EnrollmentID = subquery.EnrollmentID
    '''

    c.execute(sub, filter_params)
    result = {}
    for row in c.fetchall():
        result['Total Exits']=row[0]+row[1]
        result['Permanent Housing Situations'] = row[3], row[3]/result['Total Exits']
        result['Other'] = row[4], row[4]/result['Total Exits']
    conn.close()
    return result

#60007
def specific_exit(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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
        
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT    
        SUM(CASE WHEN Exit.Destination IN (215,206,207,225,204,205,302,329,314,332,312,313,327,336,335,422,423,426,410,435,421,411) THEN 1 ELSE 0 END) AS Specific,
        SUM(CASE WHEN Exit.Destination IN (116,101,118,24,30,17,37,8,9,99) THEN 1 ELSE 0 END) AS Other       
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.EnrollmentID = subquery.EnrollmentID
    '''

    c.execute(sub, filter_params) 
    result = {}
    for row in c.fetchall():
        result['Total Exits']=row[0]+row[1]
        result['Specific Situations'] = row[3], row[3]/result['Total Exits']
        result['Other'] = row[4], row[4]/result['Total Exits']
    conn.close()
    return result

#KPI-0066
def get_program_type(program_id, db_name='merged_hmis.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT PATHProgramType FROM PATHProgramMasterList WHERE MergedProgramID = ?", (program_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

#806011
def movein_120_days(start_date, end_date, program_id=None, department=None, region=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    main_sql = '''
        SELECT DISTINCT Enrollment.PersonalID, Exit.Destination, Enrollment.EntryDate, Exit.ExitDate, Enrollment.MoveInDate, PATHProgramMasterList.PATHProgramType
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        main_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(main_sql, filter_params)
    rows = c.fetchall() 

    # Count exit destinations within 120 business days and program types within 120 business days
    count_exit_destinations = 0
    count_program_types = 0

    for row in rows:
        entry_date_str = row[2]
        exit_date_str = row[3]
        movein_date_str = row[4]
        destination_code = row[1]

        # Convert entry_date string to datetime object
        entry_date = datetime.strptime(entry_date_str, '%Y-%m-%d')

        # Convert exit_date and movein_date strings to datetime objects if they are not None
        if exit_date_str:
            exit_date = datetime.strptime(exit_date_str, '%Y-%m-%d')
        else:
            exit_date = None

        if movein_date_str:
            movein_date = datetime.strptime(movein_date_str, '%Y-%m-%d')
        else:
            movein_date = None

        # Get program type based on program ID
        program_id = row[5]
        program_type = get_program_type(program_id)

        # Filter based on specific destination codes and within 120 business days
        if entry_date and destination_code in [426, 411, 421, 410, 435, 422, 423]:
            business_days_exit_destination = sum(1 for single_date in range((exit_date - entry_date).days + 1) if (entry_date + timedelta(days=single_date)).weekday() < 5)
            if business_days_exit_destination <= 120:
                count_exit_destinations += 1

        if entry_date and movein_date and program_type in ['Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services']:
            business_days_program_type = sum(1 for single_date in range((movein_date - entry_date).days + 1) if (entry_date + timedelta(days=single_date)).weekday() < 5)
            if business_days_program_type <= 120:
                count_program_types += 1

    conn.close()
    overall_count = count_exit_destinations + count_program_types
    return overall_count
#print(movein_120_days(start_date, end_date, program_id=['LA|1983']))

#KPI-0057
def exit_to_successful_housing_destination(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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

        
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination IN (313,312,101,426, 411, 421, 410, 435, 422, 423) THEN 1 ELSE 0 END) AS Successful,
        SUM(CASE WHEN Exit.Destination IN (24,30,17,37,116,118,206,215,207,204,205,225,329,314,302,327,332,8,9,99) THEN 1 ELSE 0 END) AS Other        
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.EnrollmentID = subquery.EnrollmentID
    '''

    c.execute(sub, filter_params)
    result = {}
    for row in c.fetchall():
        result['Total Exits']=row[0]+row[1]
        result['Successful']= row[0], row[0]/result['Total Exits']
        result['Other'] = row[4], row[4]/result['Total Exits']
    conn.close()
    return result

#664005
def exit_to_successful_destination_outreach(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    
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
       
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination IN (101,118,215,204,205,225,314,313,312,302,327,332,426, 411, 421, 410, 435, 422, 423) THEN 1 ELSE 0 END) AS Successful,
        SUM(CASE WHEN Exit.Destination IN (24,30,17,37,116,329,8,9,99) THEN 1 ELSE 0 END) AS Other        
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.EnrollmentID = subquery.EnrollmentID
    '''
    
    c.execute(sub, filter_params)
    result = {}
    for row in c.fetchall():
        result['Total Exits']=row[0]+row[1]
        result['Successful']= row[0], row[0]/result['Total Exits']
        result['Other'] = row[4], row[4]/result['Total Exits']
    conn.close()
    return result

#KPI-0058 #740008 #805008
def any_income_increase_counts(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    any_income_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND HMISSupplement.AnyIncomeIncrease = 1
    '''

    earned_income_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND HMISSupplement.EarnedIncomeIncrease = 1
    '''
    benefits_income_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND HMISSupplement.MainstreamBenefitIncomeIncrease = 1
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        any_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        any_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        any_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        any_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(any_income_sql, filter_params)
    any_income = c.fetchone()[0]
    
    c.execute(earned_income_sql, filter_params)
    earned_income= c.fetchone()[0]
    
    c.execute(benefits_income_sql, filter_params)
    benefits_income = c.fetchone()[0]
    
    conn.close()
    return any_income
#600013
def earned_income_increase_counts(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    any_income_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND HMISSupplement.AnyIncomeIncrease = 1
    '''

    earned_income_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND HMISSupplement.EarnedIncomeIncrease = 1
    '''
    benefits_income_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND HMISSupplement.MainstreamBenefitIncomeIncrease = 1
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        any_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    if department is not None:
        placeholders = ','.join(['?' for _ in department])
        any_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
        filter_params.extend(department)

    if region is not None:
        placeholders = ','.join(['?' for _ in region])
        any_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
        filter_params.extend(region)

    if program_type is not None:
        placeholders = ','.join(['?' for _ in program_type])
        any_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        earned_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        benefits_income_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
        filter_params.extend(program_type)

    c.execute(any_income_sql, filter_params)
    any_income = c.fetchone()[0]
    
    c.execute(earned_income_sql, filter_params)
    earned_income= c.fetchone()[0]
    
    c.execute(benefits_income_sql, filter_params)
    benefits_income = c.fetchone()[0]
    
    conn.close()
    return earned_income


#KPI-0081 & KPI-0082 & Performance Target ID#170103 & Performance Target ID#733001 #243001
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

    c.execute(denominator_sql, denominator_filter_params)
    denominator = c.fetchone()[0]

    if denominator > 0:
        retention_rate = numerator/denominator
    else:
        retention_rate = 0

    return retention_rate

#KPI-0087 & PTI#243003 
def personal_data_quality(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT DISTINCT Enrollment.EnrollmentID, Enrollment.PersonalID, PATHProgramMasterList.PATHProgramType, Enrollment.DateOfEngagement
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

    non_outreach_query = f'''
    SELECT COUNT(DISTINCT PersonalID)
    FROM ({sql}) AS subquery
    WHERE PATHProgramType <> "Outreach Services"
    '''

    c.execute(non_outreach_query, filter_params)
    non_outreach_total_enrollments = c.fetchone()[0]  

    outreach_query = f'''
    SELECT COUNT(DISTINCT PersonalID)
    FROM ({sql}) AS subquery
    WHERE PATHProgramType = "Outreach Services"
    AND DateOfEngagement IS NOT NULL 
    AND DateOfEngagement <= ?
    '''
    
    outreach_params =  filter_params + [end_date]
    c.execute(outreach_query, outreach_params)
    outreach_total_enrollments = c.fetchone()[0]  

    sub = f'''
            SELECT
                SUM(CASE WHEN Client.NameDataQuality > 1 OR 
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
                (Client.GenderNone >= 8) THEN 1 ELSE 0 END)
            FROM ({sql}) AS subquery
            LEFT JOIN Client ON subquery.PersonalID = Client.PersonalID
            LEFT JOIN Enrollment ON subquery.EnrollmentID = Enrollment.EnrollmentID
            '''
    c.execute(sub, filter_params)
    
    total_score = c.fetchone()[0]
        
    conn.close()
    
    total_enrollments = non_outreach_total_enrollments + outreach_total_enrollments

    conn.close()

    if total_enrollments > 0:
        data_quality_score = 1-(total_score/total_enrollments)
        return data_quality_score
    else:
        return None

def universal_data_quality(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    # NEEDS FURTHER VALIDATION
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT DISTINCT Enrollment.EnrollmentID, Enrollment.PersonalID, PATHProgramMasterList.PATHProgramType, Enrollment.DateOfEngagement
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


    total_served_query = f'''
    SELECT COUNT(DISTINCT PersonalID)
    FROM ({sql}) AS subquery
    '''

    c.execute(total_served_query, filter_params)
    total_served = c.fetchone()[0]  

    sub = f'''
            SELECT 
                SUM(CASE WHEN Client.VeteranStatus = 8 OR Client.VeteranStatus = 9 OR
                (Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) OR
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
                (Enrollment.DisablingCondition=99 OR Enrollment.DisablingCondition IS NULL) THEN 1 ELSE 0 END)
            FROM ({sql}) AS subquery
            LEFT JOIN Client ON subquery.PersonalID = Client.PersonalID
            LEFT JOIN Enrollment ON subquery.EnrollmentID=Enrollment.EnrollmentID
            LEFT JOIN Exit ON subquery.EnrollmentID= Exit.EnrollmentID

            '''
    c.execute(sub, filter_params)
    total_score = c.fetchone()[0]
    
    conn.close()


    if total_served > 0:
        data_quality_score = 1-(total_score/total_served)
        return data_quality_score
    else:
        return None


#KPI-0087
#notes: same personalIDS enrolled in same program with different exit date and exit destination, currently calculated to take the most recent exit date
def destination_data_quality(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT Enrollment.EnrollmentID, Enrollment.PersonalID, MAX(Exit.ExitDate) AS MaxExitDate
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

    sql += '''
        GROUP BY Enrollment.PersonalID
    '''
    count_query = f'''
        SELECT COUNT(DISTINCT PersonalID)
        FROM ({sql}) AS subquery
    '''
    # Execute the count query
    c.execute(count_query, filter_params)
    total = c.fetchone()[0]

    sub = f'''
        SELECT
        SUM(CASE WHEN (Exit.Destination = 8 OR Exit.Destination = 9) THEN 1 ELSE 0 END) AS ClientDoesNotKnow_RefusedDestination,
        SUM(CASE WHEN (Exit.Destination = 99 OR Exit.Destination = 30 OR Exit.Destination IS NULL) THEN 1 ELSE 0 END) AS DestinationInformationMissing  
        FROM ({sql}) AS subquery
        LEFT JOIN Exit ON subquery.EnrollmentID = Exit.EnrollmentID AND subquery.MaxExitDate = Exit.ExitDate
    '''

    c.execute(sub, filter_params)
    result = {}

    for row in c.fetchall():
        result['ClientDoesNotKnow_RefusedDestination'] = row[0]
        result['DestinationInformationMissing'] = row[1]
        

    client_doesnt_know_percentage=result['ClientDoesNotKnow_RefusedDestination']/total*100
    information_missing_percentage=result['DestinationInformationMissing']/total*100


    conn.close()
    return result,total,client_doesnt_know_percentage,information_missing_percentage

#KPI-0087
def income_data_quality(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT Enrollment.EnrollmentID, Enrollment.PersonalID, MAX(Exit.ExitDate) AS MaxExitDate
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

    sql += '''
        GROUP BY Enrollment.PersonalID
    '''

    count_query = f'''
        SELECT COUNT(DISTINCT PersonalID)
        FROM ({sql}) AS subquery
    '''
    # Execute the count query
    c.execute(count_query, filter_params)
    total = c.fetchone()[0]
    
    sub = f'''
        SELECT
            SUM(CASE WHEN ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
            AND Enrollment.EntryDate = IncomeBenefits.InformationDate
            AND IncomeBenefits.IncomeFromAnySource IN (8, 9) THEN 1 ELSE 0 END) AS ClientDoesNotKnow_RefusedStartIncome,
            SUM(CASE WHEN ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
            AND Enrollment.EntryDate = IncomeBenefits.InformationDate
            AND (IncomeBenefits.IncomeFromAnySource IS NULL OR IncomeBenefits.IncomeFromAnySource = 99
            OR (IncomeBenefits.DataCollectionStage = 1 AND IncomeBenefits.IncomeFromAnySource IS NULL)) THEN 1 ELSE 0 END) AS StartIncomeInformationMissing,
            SUM(CASE WHEN ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
            AND Enrollment.EntryDate = IncomeBenefits.InformationDate
            AND IncomeBenefits.DataCollectionStage = 1 AND IncomeBenefits.IncomeFromAnySource = 0 
            AND ((IncomeBenefits.Earned = 1 OR IncomeBenefits.Unemployment = 1 OR IncomeBenefits.SSI = 1
            OR IncomeBenefits.SSDI = 1 OR IncomeBenefits.VADisabilityService = 1 OR IncomeBenefits.VADisabilityNonService = 1
            OR IncomeBenefits.PrivateDisability = 1 OR IncomeBenefits.WorkersComp = 1 OR IncomeBenefits.TANF = 1
            OR IncomeBenefits.WorkersComp = 1 OR IncomeBenefits.GA = 1 OR IncomeBenefits.SocSecRetirement = 1
            OR IncomeBenefits.Pension = 1 OR IncomeBenefits.ChildSupport = 1 OR IncomeBenefits.Alimony = 1 OR IncomeBenefits.OtherIncomeSource = 1)
            OR (IncomeBenefits.DataCollectionStage = 1 AND IncomeBenefits.IncomeFromAnySource = 1 
            AND (IncomeBenefits.Earned = 0 OR IncomeBenefits.Unemployment = 0 OR IncomeBenefits.SSI = 0
            OR IncomeBenefits.SSDI = 0 OR IncomeBenefits.VADisabilityService = 0 OR IncomeBenefits.VADisabilityNonService = 0
            OR IncomeBenefits.PrivateDisability = 0 OR IncomeBenefits.WorkersComp = 0 OR IncomeBenefits.TANF = 0
            OR IncomeBenefits.WorkersComp = 0 OR IncomeBenefits.GA = 0 OR IncomeBenefits.SocSecRetirement = 0
            OR IncomeBenefits.Pension = 0 OR IncomeBenefits.ChildSupport = 0 OR IncomeBenefits.Alimony = 0 OR IncomeBenefits.OtherIncomeSource = 0))) THEN 1 ELSE 0 END) AS StartIncomeDataIssue,             
        SUM(CASE WHEN ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1) 
            AND Exit.ExitDate = IncomeBenefits.InformationDate
            AND IncomeBenefits.IncomeFromAnySource IN (8, 9) THEN 1 ELSE 0 END) AS ClientDoesNotKnow_RefusedExitIncome,
        SUM(CASE WHEN ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
            AND Exit.ExitDate = IncomeBenefits.InformationDate
            AND (IncomeBenefits.DataCollectionStage = 3 AND IncomeBenefits.IncomeFromAnySource IS NULL) THEN 1 ELSE 0 END) AS ExitIncomeInformationMissing,
        SUM(CASE WHEN ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
            AND Exit.ExitDate = IncomeBenefits.InformationDate
            AND IncomeBenefits.IncomeFromAnySource = 0 
            AND ((IncomeBenefits.Earned = 1 OR IncomeBenefits.Unemployment = 1 OR IncomeBenefits.SSI = 1
            OR IncomeBenefits.SSDI = 1 OR IncomeBenefits.VADisabilityService = 1 OR IncomeBenefits.VADisabilityNonService = 1
            OR IncomeBenefits.PrivateDisability = 1 OR IncomeBenefits.WorkersComp = 1 OR IncomeBenefits.TANF = 1
            OR IncomeBenefits.WorkersComp = 1 OR IncomeBenefits.GA = 1 OR IncomeBenefits.SocSecRetirement = 1
            OR IncomeBenefits.Pension = 1 OR IncomeBenefits.ChildSupport = 1 OR IncomeBenefits.Alimony = 1 OR IncomeBenefits.OtherIncomeSource = 1)
            OR (IncomeBenefits.IncomeFromAnySource = 1 
            AND (IncomeBenefits.Earned = 0 OR IncomeBenefits.Unemployment = 0 OR IncomeBenefits.SSI = 0
            OR IncomeBenefits.SSDI = 0 OR IncomeBenefits.VADisabilityService = 0 OR IncomeBenefits.VADisabilityNonService = 0
            OR IncomeBenefits.PrivateDisability = 0 OR IncomeBenefits.WorkersComp = 0 OR IncomeBenefits.TANF = 0
            OR IncomeBenefits.WorkersComp = 0 OR IncomeBenefits.GA = 0 OR IncomeBenefits.SocSecRetirement = 0
            OR IncomeBenefits.Pension = 0 OR IncomeBenefits.ChildSupport = 0 OR IncomeBenefits.Alimony = 0 OR IncomeBenefits.OtherIncomeSource = 0))) THEN 1 ELSE 0 END) AS ExitIncomeDataIssue
        FROM ({sql}) AS subquery
        LEFT JOIN Exit ON subquery.EnrollmentID = Exit.EnrollmentID AND subquery.MaxExitDate = Exit.ExitDate
        LEFT JOIN IncomeBenefits ON subquery.EnrollmentID=IncomeBenefits.EnrollmentID
        LEFT JOIN Client ON subquery.PersonalID =Client.PersonalID
        LEFT JOIN Enrollment ON subquery.EnrollmentID = Enrollment.EnrollmentID
    '''

    c.execute(sub, filter_params)
    result = {}

    for row in c.fetchall():
        result['ClientDoesNotKnow_RefusedStartIncome'] = row[0]
        result['StartIncomeInformationMissing'] = row[1]
        result['StartIncomeDataIssue'] = row[2]
        result['ClientDoesNotKnow_RefusedExitIncome'] = row[3]
        result['ExitIncomeInformationMissing'] = row[4]
        result['ExitIncomeDataIssue'] = row[5]
        result['SumDataIssue']=row[2]+row[5]
        result['SumClientDoesNotKnow_Refused']=row[0]+row[3]
        result['SumInformationMissing']=row[1]+row[4]
    
    data_issue_percentage= result['SumDataIssue']/total*100
    client_doesnt_know_percentage=result['SumClientDoesNotKnow_Refused']/total*100
    information_missing_percentage=result['SumInformationMissing']/total*100

    conn.close()
    return result,total,data_issue_percentage,client_doesnt_know_percentage,information_missing_percentage
    
#KPI-0059, individuals who have a date of engagement
def individuals_engaged(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement <=?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''
    
    filter_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#KPI-0060, Individuals moved in or php exit of those who have been engaged
def engaged_individuals_with_movein_or_php_exit(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement < ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate BETWEEN ? AND ? OR Exit.Destination IN (426, 411, 421, 410, 435, 422, 423))
    '''

    filter_params = [end_date, end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#KPI-0054
def exit_to_php_or_referral_attained(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Services ON Enrollment.EnrollmentID = Services.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Exit.Destination IN (426, 411, 421, 410, 435, 422, 423) OR (Services.RecordType=161 AND Services.TypeProvided=7 AND Services.ReferralOutcome=1))
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#KPI-0063
def attained_referral_to_interim_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Event ON Enrollment.EnrollmentID =Event.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Event.Event IN (10,11, 12) AND Event.ReferralResult=1)
    '''

    filter_params = [ end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count
#343603
def engaged_referral_to_interim_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Event ON Enrollment.EnrollmentID =Event.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement < ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Event.Event IN (10,11, 12) AND Event.ReferralResult=1)
    '''

    filter_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#KPI-0062 #264004 #2476
def successful_match_to_php_resource(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Event ON Enrollment.EnrollmentID =Event.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement BETWEEN ? AND ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Event.Event IN (12,13, 14,15) AND Event.ReferralResult=1)
    '''

    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#264002
def attained_any_referral(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Event ON Enrollment.EnrollmentID =Event.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement < ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Event.Event IN (1,2,3,4,5,6,7,8,9,10,11, 12,13,14,15,16,18) AND Event.ReferralResult=1)
    '''

    filter_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#KPI-0061
def successfully_attained_any_referral(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Event ON Enrollment.EnrollmentID =Event.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement < ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Event.Event IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18) AND Event.ReferralResult=1)
    '''

    filter_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#170105 & ID#740006
def time_to_move_in_average_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # Query to select PersonalIDs based on filtering conditions
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

    c.execute(sql, filter_params)

    personal_ids = [row[0] for row in c.fetchall()]

    time_to_move_in = {}  # Initialize a dictionary to store time to move-in
    move_in_times = []  # Initialize a list to store move-in times
    total_time = 0  # Initialize total time for calculating the average
    count = 0  # Initialize a count for the number of individuals with valid times

    for personal_id in personal_ids:
        # Query to select move-in date from enrollment
        move_in_sql = '''
        SELECT MoveInDate
        FROM Enrollment
        WHERE PersonalID = ? 
        '''

        # Query to select entry date from enrollment records
        entry_date_sql = '''
        SELECT EntryDate
        FROM Enrollment
        WHERE PersonalID = ? 
        '''

        # Execute queries to retrieve move-in and entry dates
        c.execute(move_in_sql, (personal_id,))
        move_in_result = c.fetchone()

        c.execute(entry_date_sql, (personal_id,))
        entry_date_result = c.fetchone()

        # Check if move-in and entry dates are available
        if move_in_result and entry_date_result and move_in_result[0] is not None:
            move_in_date = datetime.strptime(move_in_result[0], '%Y-%m-%d')  # Convert to datetime object
            entry_date = datetime.strptime(entry_date_result[0], '%Y-%m-%d')  # Convert to datetime object

            # Calculate the difference in business days between entry date and move-in date
            business_days = 0
            current_date = entry_date

            while current_date < move_in_date:
                if current_date.weekday() < 5:  # Monday to Friday
                    business_days += 1
                current_date += timedelta(days=1)

            time_to_move_in[personal_id] = business_days
            move_in_times.append(business_days)  # Add to the move_in_times list

            total_time += business_days
            count += 1
        
            # Handle the case where move-in or entry date is missing for this PersonalID
            #print(f"Warning: Missing move-in or entry date for PersonalID {personal_id}")

    conn.close()

    # Calculate the average time to move-in in business days
    average_time = total_time / count if count > 0 else 0

    return average_time
#time_to_move_in, average_time = time_to_movein_average_days(start_date, end_date, department=['Metro LA'], program_type=['Rapid Rehousing Services'])

#for personal_id, business_days in time_to_move_in.items():
    print(f'PersonalID {personal_id}: Time to Move In {business_days} business days')
#print(f'Average Move In Time: {average_time:.2f} business days')


#Performance Target ID#558007
def income_over_35k(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN IncomeBenefits ON Enrollment.EnrollmentID =IncomeBenefits.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE IncomeBenefits.InformationDate BETWEEN ? AND ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (IncomeBenefits.EarnedAmount *12 >35000) OR (IncomeBenefits.TotalMonthlyIncome *12 >35000)'''
        
    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#550006
def participants_exited(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ?)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result


#Performance Target ID#550016
#this is looking at incarcerated adult
def incarcerated_adult(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.IncarceratedAdult IN (1,2))'''
        
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#560001
def disabled_veteran(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Client ON Enrollment.PersonalID =Client.PersonalID
        LEFT JOIN Disabilities ON Enrollment.EnrollmentID =Disabilities.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Client.VeteranStatus=1 AND (Enrollment.DisablingCondition=1 OR Disabilities.DisabilityType IN (5,6,7,8,9,10)))'''
        
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#600004
def involuntarily_exits(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND Exit.ProjectCompletionStatus =3 OR Exit.ExpelledReason IN ( 1,2,3)
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#Performance Target ID#600008
def exit_to_no_habitation_and_no_interview(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    sql = '''
        SELECT DISTINCT Enrollment.PersonalID,Enrollment.EnrollmentID
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
    
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination =116 THEN 1 ELSE 0 END) AS NotMeantForHabitation,
        SUM(CASE WHEN Exit.Destination =30 THEN 1 ELSE 0 END) AS NoExitInterview,
        SUM(CASE WHEN Exit.Destination IN (101,118,215,2066,207,225,204,205,302,329,314,332,312,313,327,336,335,422,423,426,410,435,421,411,24,,17,37,8,9,99) THEN 1 ELSE 0 END) AS Other
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.PersonalID = subquery.PersonalID
    WHERE Exit.ExitDate BETWEEN ? AND ?
    '''
    filter_params.extend([start_date, end_date])
    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['total']=row[0] +row[1] +row[2]
        result['NotMeantForHabitation'] = row[0]/result['total']*100
        result['NoExitInterview'] = row[1]/result['total']*100

    conn.close()
    return result

#Performance Target ID#600009
def length_of_stay_for_all(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # Query to select PersonalIDs based on filtering conditions
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

    c.execute(sql, filter_params)

    personal_ids = [row[0] for row in c.fetchall()]

    length_of_stays = {}  # Initialize a dictionary to store length of stays
    total_length = 0  # Initialize total length for calculating the average
    count = 0  # Initialize a count for the number of individuals with valid lengths

    for personal_id in personal_ids:
        # Query to select move-in date from enrollment
        move_in_sql = '''
            SELECT EntryDate
            FROM Enrollment
            WHERE PersonalID = ? 
        '''

        # Query to select exit date from exit records
        exit_sql = '''
            SELECT ExitDate
            FROM Exit
            WHERE PersonalID = ? 
        '''

        # Execute queries to retrieve move-in and exit dates
        c.execute(move_in_sql, (personal_id,))
        move_in_result = c.fetchone()

        c.execute(exit_sql, (personal_id,))
        exit_result = c.fetchone()

        if move_in_result and exit_result:
            move_in_date = datetime.strptime(move_in_result[0], '%Y-%m-%d')  # Convert to datetime object
            exit_date = datetime.strptime(exit_result[0], '%Y-%m-%d')  # Convert to datetime object

            # Calculate the length of stay in business days (excluding weekends)
            business_days = 0
            current_date = move_in_date
            while current_date <= exit_date:
                if current_date.weekday() < 5:  # Monday to Friday are business days (0 to 4)
                    business_days += 1
                current_date += timedelta(days=1)

            length_of_stays[personal_id] = business_days

            total_length += business_days
            count += 1

    conn.close()

    # Calculate the average length of stay in business days
    average_length = total_length / count if count > 0 else 0

    return average_length

#length_of_stays, average_length = length_of_stay_for_all(start_date, end_date, department=['Metro LA'], program_type=['Interim Housing Services'])

#for personal_id, length_of_stay in length_of_stays.items():
    print(f'PersonalID {personal_id}: Length of Stay {length_of_stay} days')
#print(f'Average Length of Stay: {average_length} days')

#664009
def length_of_stay_for_php_exits(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # Query to select PersonalIDs based on filtering conditions
    sql = '''
        SELECT DISTINCT Enrollment.PersonalID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date,start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)

    personal_ids = [row[0] for row in c.fetchall()]

    length_of_stays = {}  # Initialize a dictionary to store length of stays
    total_length = 0  # Initialize total length for calculating the average
    count = 0  # Initialize a count for the number of individuals with valid lengths

    for personal_id in personal_ids:
        # Query to select move-in date from enrollment
        move_in_sql = '''
            SELECT EntryDate
            FROM Enrollment
            WHERE PersonalID = ? 
        '''

        # Query to select exit date from exit records
        exit_sql = '''
            SELECT ExitDate
            FROM Exit
            WHERE PersonalID = ? 
        '''

        # Execute queries to retrieve move-in and exit dates
        c.execute(move_in_sql, (personal_id,))
        move_in_result = c.fetchone()

        c.execute(exit_sql, (personal_id,))
        exit_result = c.fetchone()

        if move_in_result and exit_result:
            move_in_date = datetime.strptime(move_in_result[0], '%Y-%m-%d')  # Convert to datetime object
            exit_date = datetime.strptime(exit_result[0], '%Y-%m-%d')  # Convert to datetime object

            # Calculate the length of stay in business days (excluding weekends)
            business_days = 0
            current_date = move_in_date
            while current_date <= exit_date:
                if current_date.weekday() < 5:  # Monday to Friday are business days (0 to 4)
                    business_days += 1
                current_date += timedelta(days=1)

            length_of_stays[personal_id] = business_days

            total_length += business_days
            count += 1

    conn.close()

    # Calculate the average length of stay in business days
    average_length = total_length / count if count > 0 else 0


    return average_length

#Performance Target ID#600012 #642004 #570003
#interim return to homeless and percentage
def return_to_homelessness(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Enrollment.PersonalID,Enrollment.EnrollmentID
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
    
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination IN (116,118,101)THEN 1 ELSE 0 END) AS Homeless,
        SUM(CASE WHEN Exit.Destination IN (101,118,215,207,204,205,225,314,312,313,302,327,332,426,411,421,410,435,422,423,24,8,9,99,30,17) THEN 1 ELSE 0 END) AS Other
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.PersonalID = subquery.PersonalID
    WHERE Exit.ExitDate BETWEEN ? AND ?
    '''
    filter_params.extend([start_date, end_date])
    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['total']=row[0] +row[1]
        result['Homeless'] = row[0]/result['total']*100
        result['Other'] = row[1]/result['total']*100
    conn.close()
    return result

#Performance Target ID#600015 & #605011 & #607009 
#can this be considered no financial resources at exit(#740005)
def non_cash_benefits_at_exit(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN IncomeBenefits ON Enrollment.EnrollmentID= IncomeBenefits.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (IncomeBenefits.DataCollectionStage=3 AND IncomeBenefits.BenefitsFromAnySource=1)'''
        
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#600016 & #605012 & #607010
def insurance_at_exit(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN IncomeBenefits ON Enrollment.EnrollmentID= IncomeBenefits.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (IncomeBenefits.DataCollectionStage=3 AND IncomeBenefits.InsuranceFromAnySource=1)'''
        
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#606002
def housed_within_30_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate - Enrollment.EntryDate <= 30 )
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#Performance Target ID#606003
def housed_within_45_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate - Enrollment.EntryDate <= 45 )
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#Performance Target ID#606004 #805007
def housed_within_90_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate - Enrollment.EntryDate <= 90 )
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#Performance Target ID#631001
def prior_living_situation(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT DISTINCT Enrollment.PersonalID,Enrollment.EnrollmentID
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
    
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Enrollment.LivingSituation IN (116,101,118) THEN 1 ELSE 0 END) AS Homeless,
        SUM(CASE WHEN Enrollment.LivingSituation IN (215,206,207,225,204,205) THEN 1 ELSE 0 END) AS Institutional,
        SUM(CASE WHEN Enrollment.LivingSituation IN (302,329,314,332,336,335) THEN 1 ELSE 0 END) AS Temporary,       
        SUM(CASE WHEN Enrollment.LivingSituation IN (410,435,421,411) THEN 1 ELSE 0 END) AS Permanent,     
        SUM(CASE WHEN Enrollment.LivingSituation = 8 THEN 1 ELSE 0 END) AS ClientDoesNotKnow,        
        SUM(CASE WHEN Enrollment.LivingSituation = 9 THEN 1 ELSE 0 END) AS ClientRefused,
        SUM(CASE WHEN Enrollment.LivingSituation = 99 THEN 1 ELSE 0 END) AS DatNotCollected
        FROM ({sql}) AS subquery 
        LEFT JOIN Enrollment ON Enrollment.PersonalID = subquery.PersonalID
    '''
    
    c.execute(sub, filter_params)
    
    # Initialize the result dictionary as a list
    result = []

    for row in c.fetchall():
        # Append each row as a dictionary to the result list
        result.append({
            'Homeless': row[0],
            'Institutional': row[1],
            'Temporary': row[2],
            'Permanent': row[3],
            'ClientDoesNotKnow': row[4],
            'ClientRefused': row[5],
            'DatNotCollected': row[6]
        })

    conn.close()
    return result

def zero_income_at_exit(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN IncomeBenefits ON Enrollment.EnrollmentID =IncomeBenefits.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE IncomeBenefits.InformationDate BETWEEN ? AND ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ?)
        AND (IncomeBenefits.EarnedAmount=0 AND IncomeBenefits.DataCollectionStage= 3)'''
        
    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#Performance Target ID#631015
def zero_income(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN IncomeBenefits ON Enrollment.EnrollmentID =IncomeBenefits.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE IncomeBenefits.InformationDate BETWEEN ? AND ?
        AND Enrollment.EntryDate <= ? 
        AND Enrollment.MoveInDate BETWEEN ? AND ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (IncomeBenefits.EarnedAmount=0)'''
        
    filter_params = [start_date, end_date, end_date, start_date,end_date,start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count


#Performance Target ID#631016 & #633009 & #639009 #667007
def exit_to_jail(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Enrollment.PersonalID,Enrollment.EnrollmentID
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
        
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination=207 THEN 1 ELSE 0 END) AS Jail,
        SUM(CASE WHEN Exit.Destination IN (101,118,215,204,205,225,314,312,313,302,327,332,426,411,421,410,435,422,423,24,8,9,99,30,17) THEN 1 ELSE 0 END) AS Other
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.PersonalID = subquery.PersonalID
    WHERE Exit.ExitDate BETWEEN ? AND ?
    '''
    filter_params.extend([start_date, end_date])
    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['total']=row[0] +row[1]
        result['Jail'] = row[0]/result['total']*100
        result['Other'] = row[1]/result['total']*100

    conn.close()
    return result

#Performance Target ID#740001
def total_of_households_served(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#Performance Target ID#740003
#Confirm what "unknown" destination is categorized as
def exit_to_unknown_destination(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Enrollment.PersonalID,Enrollment.EnrollmentID
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

    
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination IN (30,17,37,8,9,99)THEN 1 ELSE 0 END) AS Unknown,
        SUM(CASE WHEN Exit.Destination IN (101,118,215,207,204,205,225,314,312,313,302,327,332,426,411,421,410,435,422,423,24) THEN 1 ELSE 0 END) AS Other
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.PersonalID = subquery.PersonalID
    WHERE Exit.ExitDate BETWEEN ? AND ?
    '''
    filter_params.extend([start_date, end_date])
    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['total']=row[0] +row[1]
        result['Unknown'] = row[0]/result['total']*100
        result['Other'] = row[1]/result['total']*100
    conn.close()
    return result

#Performance Target ID #740007
def return_to_homelessness(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT DISTINCT Enrollment.PersonalID,Enrollment.EnrollmentID
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
    
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''
        
    sub = f'''
    SELECT
        SUM(CASE WHEN Exit.Destination IN (116,118,101)THEN 1 ELSE 0 END) AS Homeless,
        SUM(CASE WHEN Exit.Destination IN (101,118,215,207,204,205,225,314,312,313,302,327,332,426,411,421,410,435,422,423,24,8,9,99,30,17) THEN 1 ELSE 0 END) AS Other
    FROM Exit
    INNER JOIN ({sql}) AS subquery ON Exit.PersonalID = subquery.PersonalID
    WHERE Exit.ExitDate BETWEEN ? AND ?
    '''
    filter_params.extend([start_date, end_date])
    c.execute(sub, filter_params)
    
    result = {}
    for row in c.fetchall():
        result['total']=row[0] +row[1]
        result['Homeless'] = row[0]/result['total']*100
        result['Other'] = row[1]/result['total']*100

    conn.close()
    return result

#PTI #2108001, #600006
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
    
    result ={}
    
    for row in c.fetchall():
        result['AmIndAKNative']=row[0]
        result['Asian']= row[1]
        result['BlackAfAmerican']=row[2]
        result['HispanicLatinaeo']=row[3]
        result['MidEastNAfrican']=row[4]
        result['NativeHIPacific']=row[5]
        result['White']=row[6]
        result['AdditionalRaceEthnicity']=row[7]
        result['ClientDoesNotKnow']=row[8]
        result['ClientRefused']=row[9]
        result['DataNotCollected']=row[10]

    conn.close()
    return result

#PTI #2108001, #600006
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

    result = {}
    
    for row in c.fetchall():
        result['Woman']=row[0]
        result['Man']=row[1]
        result['NonBinary']=row[2]
        result['CulturallySpecific']=row[3]
        result['Transgender']=row[4]
        result['Questioning']=row[5]
        result['DifferentIdentity']=row[6]
        result['ClientDoesNotKnow']=row[7]
        result['ClientRefused']=row[8]
        result['DataNotCollected']=row[9]
    conn.close()
    return result

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

    return result

#PTI 570001
def chronically_homeless_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ChronicallyHomeless = "Yes"
    '''

    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#PTI 170105 #635501
def movein_permanent_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    conn.close()
    return move_in_count

#PTI 733001
def moved_into_or_exited_permanent_destination(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return {"Total PHP Count": move_in_count + exit_to_perm_dest_count,
            "Move-in Count": move_in_count,
            "Exit to Permanent Destination Count": exit_to_perm_dest_count} 
    

#343605 #264005
def engaged_php_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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
        AND (Enrollment.DateOfEngagement < ?)
    '''

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.DateOfEngagement < ?)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination >= 400 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date,end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(move_in_sql, filter_params)
    move_in_count = c.fetchone()[0]
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return {"Total PHP Count": move_in_count + exit_to_perm_dest_count,
            "Move-in Count": move_in_count,
            "Exit to Permanent Destination Count": exit_to_perm_dest_count} 

#PTI 550002 
def temp_or_perm_destinations(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
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
        
    sql += '''
    GROUP BY Enrollment.PersonalID
    '''

    sub = f'''
    SELECT
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
        result['Total Exits']=row[0]+row[1]+row[2]+row[3]+row[4]+row[5]
        result['Temporary Housing Situations'] = row[0], row[0]/result['Total Exits']
        result['Permanent Housing Situations'] = row[1], row[1]/result['Total Exits']
        result['Other'] = row[2], row[2]/result['Total Exits']
        result['ClientDoesNotKnow'] = row[3], row[3]/result['Total Exits']
        result['ClientRefused'] = row[4], row[4]/result['Total Exits']
        result['DataNotCollected'] = row[5], row[5]/result['Total Exits']

    conn.close()
    return result

#662007
def exit_to_emergency_shelter(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination = 101 AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return exit_to_perm_dest_count

#662007
def exit_to_temporary_destination(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    exit_to_temp_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination IN (101,302,215,225) AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        exit_to_temp_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    c.execute(exit_to_temp_dest_sql, filter_params)
    exit_to_temp_dest_count = c.fetchone()[0]
    
    conn.close()

    return exit_to_temp_dest_count

#PTI 605006 #607005
def household_exit_to_perm_php_count(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
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

    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()
    return exit_to_perm_dest_count

#631005 PENDING
def veteran_status_at_movein(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql = '''
        SELECT DISTINCT Enrollment.PersonalID, MIN(Enrollment.MoveInDate)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        GROUP BY Enrollment.PersonalID
        HAVING MIN(Enrollment.MoveInDate) BETWEEN ? AND ?
    '''
    
    filter_params = [end_date, start_date,start_date,end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

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
        result['NotaVeteran']=row[0]
        result['Veteran']=row[1]
        result['ClientDoesNotKnow']=row[2]
        result['ClientRefused']=row[3]
        result['DataNotCollected']=row[4]

    conn.close()
    return result

#PTI 633007,639007
def number_employed(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN EmploymentEducation ON Enrollment.EnrollmentID =EmploymentEducation.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE EmploymentEducation.Employed=1
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)'''
        
    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#316004
def link_to_mental_health_services(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Services ON Enrollment.EnrollmentID =Services.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL
        AND(Services.RecordType=143 AND Services.TypeProvided=10 )'''
        
    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#316004
def link_to_substance_use_treatment(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Services ON Enrollment.EnrollmentID =Services.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL
        AND(Services.RecordType=143 AND Services.TypeProvided=12)
        OR(Services.RecordType=142 AND Services.TypeProvided=17)'''
        
    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]

    conn.close()
    return count

#316006
def link_to_primary_care(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        LEFT JOIN Services ON Enrollment.EnrollmentID =Services.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL
        AND(Services.RecordType=161 AND Services.TypeProvided=3)'''
        
    filter_params = [start_date, end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    count = c.fetchone()[0]
    conn.close()
    return count

#664004
def time_to_engage_average_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # Query to select PersonalIDs based on filtering conditions
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

    c.execute(sql, filter_params)

    personal_ids = [row[0] for row in c.fetchall()]

    time_to_engage = {}  # Initialize a dictionary to store time to move-in
    engagement_times = []  # Initialize a list to store move-in times
    total_time = 0  # Initialize total time for calculating the average
    count = 0  # Initialize a count for the number of individuals with valid times

    for personal_id in personal_ids:
        # Query to select move-in date from enrollment
        engagement_sql = '''
        SELECT DateOfEngagement
        FROM Enrollment
        WHERE PersonalID = ? 
        '''

        # Query to select entry date from enrollment records
        entry_date_sql = '''
        SELECT EntryDate
        FROM Enrollment
        WHERE PersonalID = ? 
        '''

        # Execute queries to retrieve move-in and entry dates
        c.execute(engagement_sql, (personal_id,))
        engagement_result = c.fetchone()

        c.execute(entry_date_sql, (personal_id,))
        entry_date_result = c.fetchone()

        # Check if move-in and entry dates are available
        if engagement_result and entry_date_result and engagement_result[0] is not None:
            engagement_date = datetime.strptime(engagement_result[0], '%Y-%m-%d')  # Convert to datetime object
            entry_date = datetime.strptime(entry_date_result[0], '%Y-%m-%d')  # Convert to datetime object

            
            business_days = 0
            current_date = entry_date

            while current_date < engagement_date:
                if current_date.weekday() < 5:  # Monday to Friday
                    business_days += 1
                current_date += timedelta(days=1)

            time_to_engage[personal_id] = business_days
            engagement_times.append(business_days)  # 
            total_time += business_days
            count += 1
        else:
            # Handle the case where move-in or entry date is missing for this PersonalID
            print(f"Warning: Missing move-in or entry date for PersonalID {personal_id}")

    conn.close()
    average_time = total_time / count if count > 0 else 0
    return time_to_engage, average_time


#606008, #605008, #607006
def return_to_homelessness_after_x_months(start_date, end_date, time_period, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    time_date = f'DATE(Enrollment.MoveInDate, "+{time_period} days")'

    numerator_sql = f'''
        SELECT COUNT (DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services') 
        AND Enrollment.MoveInDate <= ?     
        AND {time_date} <= ?
        AND (
            (Exit.ExitDate IS NULL) OR
            (Exit.ExitDate >= {time_date}) OR
            (Exit.ExitDate < {time_date} AND Exit.Destination >= 400)
        )
        AND (
            Exit.Destination >400
        )
    '''

    numerator_filter_params = [end_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        numerator_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        numerator_filter_params.extend(program_id)


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
            Exit.Destination > 400
        )
    '''

    denominator_filter_params = [end_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        denominator_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        denominator_filter_params.extend(program_id)

    c.execute(denominator_sql, denominator_filter_params)
    denominator = c.fetchone()[0]

    if denominator > 0:
        homeless_rate = numerator/denominator
    else:
        homeless_rate = 0

    return (1-homeless_rate)

#643005
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
        result['AnyInsurance']=row[0]     
        result['Medicaid']=row[1]
        result['Medicare']=row[2]
        result['SCHIP']=row[3]
        result['VHAServices']=row[4]
        result['EmployerProvided']=row[5]
        result['COBRA']=row[6]
        result['PrivatePay']=row[7]
        result['StateHealthIns']=row[8]
        result['IndianHealthServices']=row[9]
        result['OtherInsurance']=row[10]
        result['NoInsurance']=row[11]
        result['ClientDoesNotKnow']=row[12]
        result['ClientRefused']=row[13]
        result['DataNotCollected']=row[14]
    conn.close()

    return result

#664003 #662010
def enrollment_to_engagement_ratio(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # Count total enrollments
    total_enrollments_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    total_enrollments_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        total_enrollments_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        total_enrollments_params.extend(program_id)

    c.execute(total_enrollments_sql, total_enrollments_params)
    total_enrollments = c.fetchone()[0]

    # Count total engagements
    engagements_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.DateOfEngagement <= ?
        AND Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
    '''

    engagements_params = [end_date, end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        engagements_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        engagements_params.extend(program_id)

    c.execute(engagements_sql, engagements_params)
    total_engagements = c.fetchone()[0]

    conn.close()

    # Calculate percentage of enrollments turned into engagements
    if total_enrollments > 0:
        percentage = (total_engagements / total_enrollments) * 100
    else:
        percentage = 0

    return percentage

#664007
def exit_to_long_term_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate IS NULL AND Exit.Destination IN (302,312,313) AND Exit.ExitDate BETWEEN ? AND ?)
    '''

    filter_params = [end_date, start_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return exit_to_perm_dest_count

#550012
def episodic_homeless_enrolled(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ChronicallyHomeless= "Yes"
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID I N ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

#550013
def episodic_homeless_exited(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN HMISSupplement ON Enrollment.EnrollmentID = HMISSupplement.EnrollmentID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ?)
        AND ChronicallyHomeless = "Yes"
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

def exit_to_permanent_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

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
        exit_to_perm_dest_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return exit_to_perm_dest_count

#606006
def households_movedin_permanent_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    move_in_sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
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

    c.execute(move_in_sql, filter_params)
    household_ids = c.fetchone()[0]
    
    conn.close()
    return household_ids

#606001
def new_households_movedin_permanent_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    move_in_sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate >= ? 
        AND Exit.ExitDate <= ?
        AND (Enrollment.MoveInDate BETWEEN ? AND ?)
    '''

    filter_params = [start_date, end_date, start_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        move_in_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(move_in_sql, filter_params)
    household_ids = c.fetchone()[0]
    
    conn.close()
    return household_ids



#616004
def household_exit_to_permanent_housing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    exit_to_perm_dest_sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
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
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)
    
    c.execute(exit_to_perm_dest_sql, filter_params)
    exit_to_perm_dest_count = c.fetchone()[0]
    
    conn.close()

    return exit_to_perm_dest_count

#840002
def housed_within_180_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    sql = '''
        SELECT COUNT(DISTINCT Enrollment.HouseholdID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.MoveInDate - Enrollment.EntryDate <= 180 )
        OR (Exit.ExitDate - Enrollment.EntryDate <= 180 AND Exit.Destination > 300 )
    '''
    
    filter_params = [end_date, start_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(sql, filter_params)
    result = c.fetchone()[0]
    conn.close()
    return result

def average_retention_days(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    numerator_sql = '''
        SELECT SUM(CASE
                    WHEN Exit.ExitDate IS NOT NULL
                    THEN julianday(Exit.ExitDate) - julianday(Enrollment.MoveInDate)
                    ELSE julianday(?) - julianday(Enrollment.MoveInDate)
                END)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services') 
        AND Enrollment.MoveInDate <= ?
        AND (Exit.ExitDate IS NULL OR Exit.ExitDate >= Enrollment.MoveInDate)
    '''

    filter_params = [end_date, end_date]

    if program_id is not None:
        placeholders = ','.join(['?' for _ in program_id])
        numerator_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        filter_params.extend(program_id)

    c.execute(numerator_sql, filter_params)
    total_retention_days = c.fetchone()[0]

    # Get the total number of residents
    residents_sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services') 
        AND Enrollment.MoveInDate <= ?
        '''
    residents_params = [end_date]
    if program_id is not None:
        residents_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
        residents_params.extend(program_id)

    c.execute(residents_sql, residents_params)
    total_residents = c.fetchone()[0]

    # Calculate average retention days
    if total_retention_days is not None and total_residents > 0:
        average_retention_days = total_retention_days / total_residents
    else:
        average_retention_days = 0


    return average_retention_days

