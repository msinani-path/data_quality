import sqlite3
from datetime import datetime,timedelta
import numpy as np
import matplotlib.pyplot as plt
from statistics import mean, median
import csv
import numbers
import os
from matplotlib.ticker import PercentFormatter
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from reportlab.lib.colors import Color, gray, lightgrey, black, white,linen, lightblue, gainsboro
PATHLightBlue = Color((178/255), (231/255), (250.0/255), 1)


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

class Name():

    def name_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality in (2,8,9,99))
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''
                
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality in (2,8,9,99))
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''
                
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        
        if output ==None:
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
        elif output=='list':
            return pt_list
            
    
    def name_client_refused_doesnt_know(self,start_date, end_date,output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality in (8,9))'''
                
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
        if output==None: 
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
        elif output=="count":
            return total_score
        
    def name_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND ((Client.NameDataQuality = 99)
                OR Client.FirstName IS NULL
                OR Client.LastName IS NULL)'''
                
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
        if output==None: 
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
        elif output=="count":
            return total_score

    def name_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Client.NameDataQuality = 2)'''
                
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
        
        if output==None:
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
        elif output=="count":
            return total_score
    
class SSN():

    def ssn_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
        SELECT COUNT (DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (
            Client.SSNDataQuality IN (8, 9, 99)
            OR Client.SSN IS NULL
            OR (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
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
                        OR Client.SSN LIKE '987654321'
                    )
                )
            ))
        '''
        
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE Enrollment.EntryDate <= ? 
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (
            Client.SSNDataQuality IN (8, 9, 99)
            OR Client.SSN IS NULL
            OR (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
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
                        OR Client.SSN LIKE '987654321'
                    )
                )
            ))
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output =="list":
            return pt_list
            
    def ssn_client_refused_doesnt_know(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Client.SSNDataQuality in (8,9))'''
                
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
        if output == None:    
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
        elif output=="count":
            return total_score

    def ssn_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= ? 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND  (Client.SSNDataQuality =99
                    OR Client.SSN IS NULL)'''                    
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
            if output==None:
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
            elif output=="count":
                return total_score
            
    def ssn_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT (DISTINCT Enrollment.PersonalID)
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND (
                (Client.SSNDataQuality = 2)
                OR 
                (
                    Client.SSNDataQuality = 1 
                    AND LENGTH(Client.SSN) = 9
                    AND (
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
                        OR Client.SSN LIKE '987654321'
                    )
                )
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
        if output ==None:    
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
        elif output=="count":
            return total_score        
class DOB():
            
    def dob_total_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9, 99)
                OR Client.DOB IS NULL
                OR Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate) -- After the record creation date
                )'''
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9, 99)
                OR Client.DOB IS NULL
                OR Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate) -- After the record creation date
                )'''
                        
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output=="list":
            return pt_list

    def dob_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality IN (8, 9))'''
                
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
        if output ==None:
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
        elif output=="count":
            return total_score

    def dob_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.DOBDataQuality =99
                OR Client.DOB IS NULL)'''
                
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
        if output==None:
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
        elif output=="count":
            return total_score

    def dob_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Client.DOBDataQuality = 2
                OR (Client.DOB < '1915-01-01') -- Prior to 1/1/1915
                OR (Client.DOB > Enrollment.EntryDate))'''
                
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
        if output==None:
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
        elif output=="count":
            return total_score
class Race():
    def race_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  Client.RaceNone in (8,9,99)'''
        
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  Client.RaceNone in (8,9,99)'''
                
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output =="list":
            return pt_list

    def race_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND Client.RaceNone in (8,9)'''
                
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
        if output==None:
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
        elif output=="count":
            return total_score
        
    def race_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= ? 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND  Client.RaceNone =99'''
                    
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
            if output==None:
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
            elif output=="count":
                return total_score

class Gender():
    def gender_total_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone >= 8)'''
                
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone >= 8)'''
                
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output=="list":
            return pt_list

    def gender_client_refused_doesnt_know(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone in(8,9))'''
                
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
        if output==None:
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
        elif output=="count":
            return total_score

    def gender_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.GenderNone =99)'''
                
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
        if output==None:
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
        elif output=="count":
            return total_score

class Veteran():
    def veteran_total_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (((Client.VeteranStatus in (8,9)) AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18) 
                OR((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18) 
                OR(Client.VeteranStatus = 1 AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 <18 ))'''
                
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (((Client.VeteranStatus in (8,9)) AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18) 
                OR((Client.VeteranStatus = 99 OR Client.VeteranStatus IS NULL) AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18) 
                OR(Client.VeteranStatus = 1 AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 <18 ))'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output=="list":
            return pt_list
        
    def veteran_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18
                AND (Client.VeteranStatus in (8,9) ) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
        if output==None:
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
        elif output=="count":
            return total_score

    def veteran_missing(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= ? 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18
                    AND (Client.VeteranStatus = 99 
                    OR Client.VeteranStatus IS NULL)'''
                    
            sql_active_non_outreach = """
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
                FROM Enrollment
                LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
            """

            sql_active_outreach = """
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            if output==None:
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
            elif output=="count":
                return total_score

    def veteran_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= ? 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND (Client.VeteranStatus = 1 AND (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 <18 )'''
                    
            sql_active_non_outreach = """
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
                FROM Enrollment
                LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
            """

            sql_active_outreach = """
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            if output==None:
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
            elif output=="count":
                return total_score

class Disabling():
    
    def disabling_condition_total_accuracy(self,start_date, end_date, output=None,program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.DisablingCondition in (8,9,99)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''
        
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (Enrollment.DisablingCondition in (8,9,99)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''


        
        sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """
        
        sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()

        conn.close()
        
        total_enrollments = total_outreach + total_non_outreach
        if output==None:
            if isinstance(total_score, numbers.Number):
                if total_enrollments > 0:
                    accuracy = 1 - (total_score / total_enrollments)
                    return max(0, accuracy)  # Ensure accuracy is not negative
                else:
                    return 0
            else:
                return None
        elif output=="list":
            return pt_list

    def disabling_condition_client_refused(self,start_date, end_date, output=None,program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition in (8,9)
        '''
        
        sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """
        
        sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
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
        if output==None:
            if isinstance(total_score, numbers.Number):
                if total_enrollments > 0:
                    accuracy = 1 - (total_score / total_enrollments)
                    return max(0, accuracy)  # Ensure accuracy is not negative
                else:
                    return 0
            else:
                return None
        elif output=="count":
            return total_score

    def disabling_condition_missing(self,start_date, end_date, output=None,program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND  Enrollment.DisablingCondition=99
        '''
        
        sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """
        
        sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
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
        if output==None:
            if isinstance(total_score, numbers.Number):
                if total_enrollments > 0:
                    accuracy = 1 - (total_score / total_enrollments)
                    return max(0, accuracy)  # Ensure accuracy is not negative
                else:
                    return 0
            else:
                return None
        elif output=="count":
            return total_score
        
    def disabling_condition_data_accuracy(self,start_date, end_date,output=None, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        LEFT JOIN Disabilities ON Enrollment.PersonalID = Disabilities.PersonalID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND ((Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType = 6 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType=8 AND Disabilities.DisabilityResponse=1)
        OR (Enrollment.DisablingCondition=0 AND Disabilities.DisabilityType IN (5,7,9,10) AND Disabilities.DisabilityResponse=1 AND Disabilities.IndefiniteAndImpairs=1))
        '''
        
        sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """
        
        sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
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
        if output==None:
            if isinstance(total_score, numbers.Number):
                if total_enrollments > 0:
                    accuracy = 1 - (total_score / total_enrollments)
                    return max(0, accuracy)  # Ensure accuracy is not negative
                else:
                    return 0
            else:
                return None 
        elif output=="count":
            return total_score   

class StartDate():
    def start_date_data_accuracy(self, start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()

        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate < '1915-01-01'
                OR Enrollment.EntryDate > Exit.ExitDate)'''
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Enrollment.EntryDate < '1915-01-01'
                OR Enrollment.EntryDate > Exit.ExitDate)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output=="list":
            return pt_list

class ExitDate():
    def exit_date_data_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Exit.ExitDate<Enrollment.EntryDate)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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

#Double check, these should be people who have exited and have no ove in date?
class MoveIn():
    def movein_date_data_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.HouseholdID) 
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? )
            AND Enrollment.RelationshipToHoH =1
            AND PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services')
            AND Enrollment.MoveInDate IS NULL
        '''

        total_enrollments = """
            SELECT COUNT(DISTINCT Enrollment.HouseholdID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.RelationshipToHoH =1
            AND PATHProgramMasterList.PATHProgramType IN ('Rapid Rehousing Services', 'Scattered Site Housing & Services', 'Site Based Housing & Services')
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            total_enrollments += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            total_enrollments += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            total_enrollments += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            total_enrollments += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(total_enrollments, filter_params)
        total_enrollmentss = c.fetchone()[0]
        
        conn.close()


        if isinstance(total_score, numbers.Number):                       
            if total_enrollmentss > 0:
                if 1-(total_score/total_enrollmentss) <= 0:
                    return 0
                else:
                    return 1-(total_score/total_enrollmentss)
            else:
                return 0
        else:
            return None

class HOH():
    def HoH_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND ((Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL) 
                OR 
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
                        )))'''
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND ((Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL) 
                OR 
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
                        )))'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output=="list":
            return pt_list
        
    def HoH_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (Enrollment.RelationshipToHoH=99 OR Enrollment.RelationshipToHoH IS NULL)'''
                
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
        if output==None:
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
        elif output=="count":
            return total_score

    def HoH_data_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= ? 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND (Enrollment.RelationshipToHoH != 1 AND NOT EXISTS (
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
                            ))'''
                    
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
            if output==None:
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
            elif output=="count":
                return total_score


class Location():
    def enrollment_coc_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  ((Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NULL) 
                OR (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NOT NULL AND Enrollment.EnrollmentCoC NOT IN 
                        ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607', 'CA-603')))'''
                        
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  ((Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NULL) 
                OR (Enrollment.RelationshipToHoH = 1 AND Enrollment.EnrollmentCoC IS NOT NULL AND Enrollment.EnrollmentCoC NOT IN 
                        ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')))'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output==None:
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
        elif output=="list":
            return pt_list
        
    def enrollment_coc_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND Enrollment.RelationshipToHoH = 1 
                AND Enrollment.EnrollmentCoC IS NULL'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
        if output==None:
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
        elif output=="count":
            return total_score

    def enrollment_coc_data_accuracy(self,start_date, end_date,output=None, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    WHERE Enrollment.EntryDate <= ? 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND Enrollment.RelationshipToHoH = 1 
                    AND Enrollment.EnrollmentCoC IS NOT NULL 
                    AND Enrollment.EnrollmentCoC NOT IN 
                            ('CA-600', 'CA-601', 'CA-606', 'CA-602', 'CA-500', 'CA-612', 'CA-614', 'CA-607','CA-603')'''
                    
            sql_active_non_outreach = """
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
                FROM Enrollment
                LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
            """

            sql_active_outreach = """
                SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
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
            if output==None:
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
            elif output=="count":
                return total_score

class Destination():
    def destination_total_accuracy(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this should include just leavers
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND  (Exit.Destination IN (8,9) OR (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)) '''
        sql_list = '''
            SELECT DISTINCT UniqueID.UniqueIdentifier
                FROM UniqueID
				LEFT JOIN Enrollment on UniqueID.PersonalID=Enrollment.PersonalID
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND  (Exit.Destination IN (8,9) OR (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_list += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
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
        
        c.execute(sql_list, filter_params)
        pt_list = c.fetchall()
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        if output == None:
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
        elif output =="list":
            return pt_list
    
    def destination_client_refused_doesnt_know(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this should include just leavers
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND  (Exit.Destination IN (8,9))'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
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
        if output ==None:
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
        elif output=="count":
            return total_score

    def destination_missing(self,start_date, end_date, output=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this should include just leavers
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND (Exit.Destination=30 OR Exit.Destination IS NULL OR Exit.Destination =99)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
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
        if output==None:
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
        elif output=="count":
            return total_score
    def number_leavers(self,start_date, end_date,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this should include just leavers
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
    
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            

        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        

        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        return total_enrollments


class PriorLiving():
    def prior_living_situation_A_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.LivingSituation IN (8,9) OR (Enrollment.LivingSituation = 99 OR Enrollment.LivingSituation IS NULL)) '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND  Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        
        conn.close()

        total_enrollments = total_outreach
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
        
    def prior_living_situation_A_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.LivingSituation IN (8,9)) '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND  Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        
        conn.close()

        total_enrollments = total_outreach
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
    
    def prior_living_situation_A_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.LivingSituation = 99 OR Enrollment.LivingSituation IS NULL)'''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND  Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        
        conn.close()

        total_enrollments = total_outreach
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

    def prior_living_situation_B_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.LivingSituation IN (8,9) OR (Enrollment.LivingSituation = 99 OR Enrollment.LivingSituation IS NULL)) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND  Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_non_outreach
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
        
    def prior_living_situation_B_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.LivingSituation IN (8,9)) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND  Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_non_outreach
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

    def prior_living_situation_B_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.LivingSituation = 99 OR Enrollment.LivingSituation IS NULL) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND  Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_non_outreach
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

class LOS():
    def length_of_stay_A_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.LengthOfStay IN (8,9) OR (Enrollment.LengthOfStay = 99 OR Enrollment.LengthOfStay IS NULL)) '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
        
    def length_of_stay_A_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.LengthOfStay IN (8,9)) '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrolmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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

    def length_of_stay_A_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND Enrollment.LengthOfStay = 99 OR Enrollment.LengthOfStay IS NULL '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
    
    def length_of_stay_B_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.LengthOfStay IN (8,9) OR (Enrollment.LengthOfStay = 99 OR Enrollment.LengthOfStay IS NULL)) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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
    
    def length_of_stay_B_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.LengthOfStay IN (8,9)) '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

    def length_of_stay_B_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LengthOfStay = 99 OR Enrollment.LengthOfStay IS NULL'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1) 
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

class LessThan90():
    def length_of_stay_lessthan_90_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to institutional situtations
        #denominator should just be non outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LivingSituation IN (200,299)
                AND ( Enrollment.LengthOfStay IS NULL)
                OR Enrollment.LOSUnderThreshold = 0 AND Enrollment.LengthOfStay in (2,3,10,11)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  Enrollment.EntryDate <= ? 
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
    
    def length_of_stay_lessthan_90_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to institutional situtations
        #denominator should just be non outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LivingSituation IN (200,299)
                AND ( Enrollment.LengthOfStay IS NULL)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  Enrollment.EntryDate <= ? 
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

    def length_of_stay_lessthan_90_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to institutional situtations
        #denominator should just be non outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LivingSituation IN (200,299)
                AND Enrollment.LOSUnderThreshold = 0 AND Enrollment.LengthOfStay in (2,3,10,11)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  Enrollment.EntryDate <= ? 
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

class LessThan7():
    def length_of_stay_lessthan_7_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LivingSituation IN (300,499)
                AND ( Enrollment.LengthOfStay IS NULL)
                OR Enrollment.LOSUnderThreshold = 0 AND Enrollment.LengthOfStay in (10,11)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.Enrollment)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  Enrollment.EntryDate <= ? 
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
        
    def length_of_stay_lessthan_7_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LivingSituation IN (300,499)
                AND ( Enrollment.LengthOfStay IS NULL)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  Enrollment.EntryDate <= ? 
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

    def length_of_stay_lessthan_7_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND Enrollment.LivingSituation IN (300,499)
                AND Enrollment.LOSUnderThreshold = 0 AND Enrollment.LengthOfStay in (10,11)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1) 
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  Enrollment.EntryDate <= ? 
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

class NightBefore():
    def night_before_missing(start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        sql = '''
        SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE (
            (julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1
        )
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        AND Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND (
            (Enrollment.LivingSituation BETWEEN 200 AND 299 AND Enrollment.LengthOfStay = 3 AND Enrollment.PreviousStreetESSH IS NULL)
            OR
            (Enrollment.LivingSituation BETWEEN 300 AND 499 AND Enrollment.LengthOfStay = 2 AND Enrollment.PreviousStreetESSH IS NULL)
        )
        '''

        sql_active_non_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
        AND Enrollment.EntryDate <= ?
        AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
        SELECT COUNT(DISTINCT Enrollment.PersonalID)
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
        AND Enrollment.EntryDate <= ?
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
                accuracy = 1 - (total_score / total_enrollments)
                return max(0, accuracy)
            else:
                return 0
        else:
            return None

class DateOfHomelessness():
    def date_of_homelessness_A_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND Enrollment.DateToStreetESSH IS NULL
                OR Enrollment.DateToStreetESSH > Enrollment.EntryDate
                OR (Enrollment.DateToStreetESSH IS NULL AND Enrollment.PreviousStreetESSH =1)'''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
    
    def date_of_homelessness_A_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND Enrollment.DateToStreetESSH IS NULL
                OR (Enrollment.DateToStreetESSH IS NULL AND Enrollment.PreviousStreetESSH =1)'''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
        
    def date_of_homelessness_A_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND Enrollment.DateToStreetESSH > Enrollment.EntryDate'''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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

    def date_of_homelessness_B_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.DateToStreetESSH IS NULL AND Enrollment.LivingSituation=116)
                OR (Enrollment.DateToStreetESSH > Enrollment.EntryDate)
                OR (Enrollment.DateToStreetESSH IS NULL AND Enrollment.PreviousStreetESSH =1)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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
        
    def date_of_homelessness_B_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.DateToStreetESSH IS NULL AND Enrollment.LivingSituation=116)
                OR (Enrollment.DateToStreetESSH IS NULL AND Enrollment.PreviousStreetESSH =1)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

    def date_of_homelessness_B_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.DateToStreetESSH > Enrollment.EntryDate)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

class TimesHomeless():
    def times_homeless_A_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears in (8,9,99))
                OR (Enrollment.TimesHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)
                OR ((Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)
    '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)  
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
        
    def times_homeless_A_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.TimesHomelessPastThreeYears in (8,9))
                OR (Enrollment.TimesHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)
    '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)  
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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

    def times_homeless_A_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears =99)
                OR ((Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)
    '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
    
    def times_homeless_B_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND ((Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears in (8,9,99)) AND Enrollment.LivingSituation=116)
                OR (Enrollment.TimesHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)
                OR ((Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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
        
    def times_homeless_B_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.TimesHomelessPastThreeYears in (8,9,99) AND Enrollment.LivingSituation=116)
                OR (Enrollment.TimesHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)'''                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

    def times_homeless_B_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND ((Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears =99) AND Enrollment.LivingSituation=116)
                OR ((Enrollment.TimesHomelessPastThreeYears IS NULL OR Enrollment.TimesHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

class MonthsHomeless():
    
    def months_homeless_A_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears in (8,9,99))
                OR (Enrollment.MonthsHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)
                OR ((Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)
    '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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
        
    def months_homeless_A_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND Enrollment.MonthsHomelessPastThreeYears in (8,9)
                OR (Enrollment.MonthsHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)
    '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1) 
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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

    def months_homeless_A_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #this applies to temp and perm situations
        #denominator should be none outreach?
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType == 'Outreach Services'
                AND (Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears = 99)
                OR ((Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)
    '''
                

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)  
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND Enrollment.DateOfEngagement <= ?
            AND PATHProgramMasterList.PATHProgramType = 'Outreach Services'
        """

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach
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

    def months_homeless_B_total(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND ((Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears in (8,9,99)) AND Enrollment.LivingSituation=116)
                OR (Enrollment.MonthsHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)
                OR ((Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)  
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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
        
    def months_homeless_B_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND (Enrollment.MonthsHomelessPastThreeYears in (8,9) AND Enrollment.LivingSituation=116)
                OR (Enrollment.MonthsHomelessPastThreeYears in (8,9) AND Enrollment.PreviousStreetESSH =1)'''
                                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)  
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

    def months_homeless_B_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):

        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND  Enrollment.EntryDate <= ?
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
                AND ((Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears = 99) AND Enrollment.LivingSituation=116)
                OR ((Enrollment.MonthsHomelessPastThreeYears IS NULL OR Enrollment.MonthsHomelessPastThreeYears =99) AND Enrollment.PreviousStreetESSH=1)'''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
            FROM Enrollment
            LEFT JOIN Exit on Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE   ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
            AND Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """


        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

            
        c.execute(sql, filter_params)
        total_score = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments =total_non_outreach
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

class Income():
    def starting_income_total_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #add adult and head of household aspect
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE (
                ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND IncomeBenefits.DataCollectionStage = 1
                AND (
                    (IncomeBenefits.IncomeFromAnySource IN (8, 9)) 
                    OR (IncomeBenefits.IncomeFromAnySource = 99 OR IncomeBenefits.IncomeFromAnySource IS NULL)
                )
            )
            OR NOT EXISTS (
                SELECT 1
                FROM IncomeBenefits AS IB
                WHERE IB.PersonalID = Enrollment.PersonalID
                AND IB.InformationDate = Enrollment.EntryDate
                AND IB.DataCollectionStage = 1
            )
            OR (
                IncomeBenefits.DataCollectionStage = 1
                AND (
                    IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                    OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                    OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                    OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                    OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                    OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                    OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSOurce IS NULL
                )
            )
            OR (
                (
                    (IncomeBenefits.IncomeFromAnySource = 0) 
                    AND (
                        IncomeBenefits.Earned = 1 OR IncomeBenefits.Unemployment = 1 OR IncomeBenefits.SSI = 1 
                        OR IncomeBenefits.SSDI = 1 OR IncomeBenefits.VADisabilityService = 1
                        OR IncomeBenefits.VADisabilityNonService = 1 OR IncomeBenefits.PrivateDisability = 1 
                        OR IncomeBenefits.WorkersComp = 1 OR IncomeBenefits.TANF = 1
                        OR IncomeBenefits.GA = 1 OR IncomeBenefits.SocSecRetirement = 1 
                        OR IncomeBenefits.Pension = 1 OR IncomeBenefits.ChildSupport = 1 
                        OR IncomeBenefits.Alimony = 1 OR IncomeBenefits.OtherIncomeSOurce = 1
                    )
                )
                OR (
                    (IncomeBenefits.IncomeFromAnySource = 1) 
                    AND (
                        IncomeBenefits.Earned = 0 OR IncomeBenefits.Unemployment = 0 OR IncomeBenefits.SSI = 0 
                        OR IncomeBenefits.SSDI = 0 OR IncomeBenefits.VADisabilityService = 0
                        OR IncomeBenefits.VADisabilityNonService = 0 OR IncomeBenefits.PrivateDisability = 0 
                        OR IncomeBenefits.WorkersComp = 0 OR IncomeBenefits.TANF = 0
                        OR IncomeBenefits.GA = 0 OR IncomeBenefits.SocSecRetirement = 0 
                        OR IncomeBenefits.Pension = 0 OR IncomeBenefits.ChildSupport = 0
                        OR IncomeBenefits.Alimony = 0 OR IncomeBenefits.OtherIncomeSOurce = 0
                    )
                )
            )
        '''

                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
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
        
    def starting_income_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?)
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (IncomeBenefits.DataCollectionStage=1)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))             
            '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
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
    
    def starting_income_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT DISTINCT Enrollment.EnrollmentID
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE  ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Enrollment.EntryDate <= ?)
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND ((IncomeBenefits.IncomeFromAnySource = 1 AND (
                            IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                            OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                            OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                            OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                            OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                            OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                            OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSOurce IS NULL
                        )
                    )
                OR (IncomeBenefits.IncomeFromAnySource=99 or IncomeBenefits.IncomeFromAnySource is NULL))
                
        '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
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
        total_score = c.fetchall()
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        return total_score

    def starting_income_data_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        #add adult and head of household aspect
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT DISTINCT Enrollment.EnrollmentID
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE
                ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Enrollment.EntryDate <= ?)
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (IncomeBenefits.DataCollectionStage = 1)
                AND (
                    IncomeBenefits.Earned = 0
                    AND IncomeBenefits.Unemployment = 0
                    AND IncomeBenefits.SSI = 0
                    AND IncomeBenefits.SSDI = 0
                    AND IncomeBenefits.VADisabilityService = 0
                    AND IncomeBenefits.VADisabilityNonService = 0
                    AND IncomeBenefits.PrivateDisability = 0
                    AND IncomeBenefits.WorkersComp = 0
                    AND IncomeBenefits.TANF = 0
                    AND IncomeBenefits.GA = 0
                    AND IncomeBenefits.SocSecRetirement = 0
                    AND IncomeBenefits.Pension = 0
                    AND IncomeBenefits.ChildSupport = 0
                    AND IncomeBenefits.Alimony = 0
                    AND IncomeBenefits.OtherIncomeSource = 0
                )
                
            '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
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
        total_score = c.fetchall()
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        return total_score

    def annual_income_total_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        return 

    def annual_income_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID)
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (IncomeBenefits.DataCollectionStage=5)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))             
            '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
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

    
    def annual_income_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT DISTINCT Enrollment.EnrollmentID
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE (
                ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Enrollment.EntryDate <= ?)
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM IncomeBenefits AS IB
                        WHERE IB.PersonalID = Enrollment.PersonalID
                        AND julianday(IB.InformationDate) >= julianday(Enrollment.EntryDate, '+1 year') - 30
                        AND julianday(IB.InformationDate) <= julianday(Enrollment.EntryDate, '+1 year') + 30
                        AND IB.DataCollectionStage = 5
                    )
                    OR (
                        EXISTS (
                            SELECT 1
                            FROM IncomeBenefits AS IB2
                            WHERE IB2.PersonalID = Enrollment.PersonalID
                            AND julianday(IB2.InformationDate) >= julianday(Enrollment.EntryDate, '+1 year') - 30
                            AND julianday(IB2.InformationDate) <= julianday(Enrollment.EntryDate, '+1 year') + 30
                            AND IB2.DataCollectionStage = 5
                            AND IB2.IncomeFromAnySource IS NULL
                        )
                    )
                )
            )
        '''
                
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Enrollment.EntryDate <= ?) 
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
        total_score = c.fetchall()
        
        c.execute(sql_active_outreach, outreach_params)
        total_outreach = c.fetchone()[0]
        
        c.execute(sql_active_non_outreach, filter_params)
        total_non_outreach = c.fetchone()[0]
        
        conn.close()

        total_enrollments = total_outreach + total_non_outreach
        return total_score

    #PENDING
    def annual_income_data_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE (
                    ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                    AND (Enrollment.EntryDate <= ?)
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                    AND (
                        (julianday(IncomeBenefits.InformationDate) >= julianday(Enrollment.EntryDate, '+1 year') - 30
                        AND julianday(IncomeBenefits.InformationDate) <= julianday(Enrollment.EntryDate, '+1 year') + 30
                        AND IncomeBenefits.DataCollectionStage = 5
                        AND IncomeBenefits.IncomeFromAnySource = 0
                        AND (
                            (IncomeBenefits.Earned = 1 OR IncomeBenefits.Unemployment = 1 OR IncomeBenefits.SSI = 1 
                            OR IncomeBenefits.SSDI = 1 OR IncomeBenefits.VADisabilityService = 1
                            OR IncomeBenefits.VADisabilityNonService = 1 OR IncomeBenefits.PrivateDisability = 1 
                            OR IncomeBenefits.WorkersComp = 1 OR IncomeBenefits.TANF = 1
                            OR IncomeBenefits.GA = 1 OR IncomeBenefits.SocSecRetirement = 1 
                            OR IncomeBenefits.Pension = 1 OR IncomeBenefits.ChildSupport = 1 
                            OR IncomeBenefits.Alimony = 1 OR IncomeBenefits.OtherIncomeSOurce = 1)
                        )
                        )
                        
                        OR
                        
                        (julianday(IncomeBenefits.InformationDate) >= julianday(Enrollment.EntryDate, '+1 year') - 30
                        AND julianday(IncomeBenefits.InformationDate) <= julianday(Enrollment.EntryDate, '+1 year') + 30
                        AND IncomeBenefits.DataCollectionStage = 5
                        AND IncomeBenefits.IncomeFromAnySource = 1
                        AND (
                            (IncomeBenefits.Earned = 0 OR IncomeBenefits.Unemployment = 0 OR IncomeBenefits.SSI = 0 
                            OR IncomeBenefits.SSDI = 0 OR IncomeBenefits.VADisabilityService = 0
                            OR IncomeBenefits.VADisabilityNonService = 0 OR IncomeBenefits.PrivateDisability = 0 
                            OR IncomeBenefits.WorkersComp = 0 OR IncomeBenefits.TANF = 0
                            OR IncomeBenefits.GA = 0 OR IncomeBenefits.SocSecRetirement = 0 
                            OR IncomeBenefits.Pension = 0 OR IncomeBenefits.ChildSupport = 0
                            OR IncomeBenefits.Alimony = 0 OR IncomeBenefits.OtherIncomeSOurce = 0)
                        )
                        )
                    )
                )
            '''


                    
            sql_active_non_outreach = """
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                    WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                    AND (Enrollment.EntryDate <= ?) 
                    AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
            """

            sql_active_outreach = """
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                    WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                    AND (Enrollment.EntryDate <= ?) 
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

    def exiting_income_total_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE (
                ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND IncomeBenefits.DataCollectionStage = 3
                AND (
                    (IncomeBenefits.IncomeFromAnySource IN (8,9))
                    OR (IncomeBenefits.IncomeFromAnySource = 99 OR IncomeBenefits.IncomeFromAnySource IS NULL)
                    OR NOT EXISTS (
                        SELECT 1
                        FROM IncomeBenefits AS IB
                        WHERE IB.PersonalID = Enrollment.PersonalID
                        AND IB.InformationDate = Exit.ExitDate
                        AND IB.DataCollectionStage = 3
                    )
                    OR (
                        IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                        OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                        OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                        OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                        OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                        OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                        OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSOurce IS NULL
                    )
                    OR IncomeBenefits.InformationDate = Exit.ExitDate
                    AND (
                        (IncomeBenefits.IncomeFromAnySource=0 AND (
                            IncomeBenefits.Earned=1 OR IncomeBenefits.Unemployment=1 OR IncomeBenefits.SSI =1 
                            OR IncomeBenefits.SSDI=1 OR IncomeBenefits.VADisabilityService=1
                            OR IncomeBenefits.VADisabilityNonService=1 OR IncomeBenefits.PrivateDisability =1 
                            OR IncomeBenefits.WorkersComp =1 OR IncomeBenefits.TANF=1
                            OR IncomeBenefits.GA =1 OR IncomeBenefits.SocSecRetirement=1 OR IncomeBenefits.Pension=1 
                            OR IncomeBenefits.ChildSupport=1 OR IncomeBenefits.Alimony =1 OR IncomeBenefits.OtherIncomeSOurce=1
                        ))
                        OR (
                            IncomeBenefits.IncomeFromAnySource=1 AND (
                                IncomeBenefits.Earned=0 OR IncomeBenefits.Unemployment=0 OR IncomeBenefits.SSI =0 
                                OR IncomeBenefits.SSDI=0 OR IncomeBenefits.VADisabilityService=0
                                OR IncomeBenefits.VADisabilityNonService=0 OR IncomeBenefits.PrivateDisability =0 
                                OR IncomeBenefits.WorkersComp =0 OR IncomeBenefits.TANF=0
                                OR IncomeBenefits.GA =0 OR IncomeBenefits.SocSecRetirement=0 OR IncomeBenefits.Pension=0 
                                OR IncomeBenefits.ChildSupport=0 OR IncomeBenefits.Alimony =0 OR IncomeBenefits.OtherIncomeSOurce=0
                            )
                        )
                    )
                )
            )
        '''

                
        
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
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
        
    def exiting_income_client_refused(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND (IncomeBenefits.DataCollectionStage=3)
                AND (IncomeBenefits.IncomeFromAnySource IN (8,9))             
            '''
                
        
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.EnrollmentID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
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
        
    def exiting_income_client_missing(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE (
                ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND IncomeBenefits.DataCollectionStage = 3
                AND (IncomeBenefits.IncomeFromAnySource = 99 OR IncomeBenefits.IncomeFromAnySource IS NULL)
            )
            OR NOT EXISTS (
                SELECT 1
                FROM IncomeBenefits AS IB
                WHERE IB.PersonalID = Enrollment.PersonalID
                AND IB.InformationDate = Exit.ExitDate
                AND IB.DataCollectionStage = 3
            )
            OR (
                IncomeBenefits.Earned IS NULL OR IncomeBenefits.Unemployment IS NULL OR IncomeBenefits.SSI IS NULL 
                OR IncomeBenefits.SSDI IS NULL OR IncomeBenefits.VADisabilityService IS NULL
                OR IncomeBenefits.VADisabilityNonService IS NULL OR IncomeBenefits.PrivateDisability IS NULL 
                OR IncomeBenefits.WorkersComp IS NULL OR IncomeBenefits.TANF IS NULL
                OR IncomeBenefits.GA IS NULL OR IncomeBenefits.SocSecRetirement IS NULL 
                OR IncomeBenefits.Pension IS NULL OR IncomeBenefits.ChildSupport IS NULL 
                OR IncomeBenefits.Alimony IS NULL OR IncomeBenefits.OtherIncomeSOurce IS NULL
            )
        '''

                
        
        sql_active_non_outreach = """
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
        """

        sql_active_outreach = """
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
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

    def exiting_income_data_accuracy(self,start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
            conn = sqlite3.connect(db_name)
            c=conn.cursor()
            
            sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
            INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
            WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 > 18 OR Enrollment.RelationshipToHoH = 1)
            AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
            AND IncomeBenefits.DataCollectionStage = 3
            AND IncomeBenefits.InformationDate = Exit.ExitDate
            AND(IncomeBenefits.IncomeFromAnySource=0 AND
                            (IncomeBenefits.Earned=1 OR IncomeBenefits.Unemployment=1 OR IncomeBenefits.SSI =1 OR IncomeBenefits.SSDI=1 OR IncomeBenefits.VADisabilityService=1
                            OR IncomeBenefits.VADisabilityNonService=1 OR IncomeBenefits.PrivateDisability =1 OR IncomeBenefits.WorkersComp =1 OR IncomeBenefits.TANF=1
                            OR IncomeBenefits.GA =1 OR IncomeBenefits.SocSecRetirement=1 OR IncomeBenefits.Pension=1 OR IncomeBenefits.ChildSupport=1 
                            OR IncomeBenefits.Alimony =1 OR IncomeBenefits.OtherIncomeSOurce=1))
            OR( IncomeBenefits.IncomeFromAnySource=1 AND
                            (IncomeBenefits.Earned=0 OR IncomeBenefits.Unemployment=0 OR IncomeBenefits.SSI =0 OR IncomeBenefits.SSDI=0 OR IncomeBenefits.VADisabilityService=0
                            OR IncomeBenefits.VADisabilityNonService=0 OR IncomeBenefits.PrivateDisability =0 OR IncomeBenefits.WorkersComp =0 OR IncomeBenefits.TANF=0
                            OR IncomeBenefits.GA =0 OR IncomeBenefits.SocSecRetirement=0 OR IncomeBenefits.Pension=0 OR IncomeBenefits.ChildSupport=0
                            OR IncomeBenefits.Alimony =0 OR IncomeBenefits.OtherIncomeSOurce=0))       
                '''
                    
            
            sql_active_non_outreach = """
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                    WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                    AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
                AND PATHProgramMasterList.PATHProgramType != 'Outreach Services'
            """

            sql_active_outreach = """
                SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                    FROM Enrollment
                    LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                    INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                    INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                    INNER JOIN IncomeBenefits ON Enrollment.PersonalID = IncomeBenefits.PersonalID
                    WHERE ((julianday(Enrollment.EntryDate) - julianday(Client.DOB)) / 365.25 >18 OR Enrollment.RelationshipToHoH=1)
                    AND (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
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
class Timeliness():
    def record_creation_start_average(self,start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        
        days_to_create = '''
            SELECT 
                CASE
                    WHEN julianday(Enrollment.DateCreated) - julianday(Enrollment.EntryDate) < 0 THEN 0
                    ELSE julianday(Enrollment.DateCreated) - julianday(Enrollment.EntryDate)
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            days_to_create += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            days_to_create += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            days_to_create += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            days_to_create += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

        days_to_create += ' GROUP BY Enrollment.PersonalID'

        c.execute(days_to_create, filter_params)
        days_to_create_count = c.fetchall()
        
        days_to_create_list = [row[0] for row in days_to_create_count if row[0] is not None]
        
        conn.close()
        if len(days_to_create_list)==0:
            return 0
        else:
            return mean(days_to_create_list)

    #created looking at "leavers"
    def record_creation_exit_average(self,start_date, end_date, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        
        days_to_create = '''
            SELECT 
                CASE
                    WHEN julianday(Exit.DateCreated) - julianday(Exit.ExitDate) < 0 THEN 0
                    ELSE julianday(Exit.DateCreated) - julianday(Exit.ExitDate)
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            days_to_create += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            days_to_create += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            days_to_create += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            days_to_create += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

        days_to_create += ' GROUP BY Enrollment.PersonalID'

        c.execute(days_to_create, filter_params)
        days_to_create_count = c.fetchall()
        
        days_to_create_list = [row[0] for row in days_to_create_count if row[0] is not None]
        
        conn.close()

        if len(days_to_create_list)==0:
            return 0
        else:
            return mean(days_to_create_list)

    def percent_start_records_created_within_x_days(self,start_date, end_date, days, output=None,program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        
        start_sql = '''
            SELECT 
                CASE
                    WHEN julianday(Enrollment.DateCreated) - julianday(Enrollment.EntryDate) < 0 THEN 0
                    ELSE julianday(Enrollment.DateCreated) - julianday(Enrollment.EntryDate)
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            start_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            start_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            start_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            start_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

        start_sql += ' GROUP BY Enrollment.PersonalID'

        c.execute(start_sql, filter_params)
        start_count = c.fetchall()
        
        days_to_create_list = [row[0] for row in start_count if row[0] is not None]

        conn.close()

        total_start = len(days_to_create_list)
        count_created_within_x_days = sum(1 for numdays in days_to_create_list if numdays <= days)
        if output==None:
            if total_start > 0:
                return (count_created_within_x_days / total_start)
            else:
                return 0
        elif output =="count":
            return total_start-count_created_within_x_days

    def percent_exit_records_created_within_x_days(self,start_date, end_date, days, output=None,program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c = conn.cursor()
        
        exit_sql = '''
            SELECT 
                CASE
                    WHEN julianday(Exit.DateCreated) - julianday(Exit.ExitDate) < 0 THEN 0
                    ELSE julianday(Exit.DateCreated) - julianday(Exit.ExitDate)
                END AS DaysBetween
            FROM Enrollment
            LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
            INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
            WHERE (Exit.ExitDate <= ? AND Exit.ExitDate >= ?)
        '''

        filter_params = [end_date, start_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            exit_sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)


        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            exit_sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            filter_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            exit_sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            filter_params.extend(region)


        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            exit_sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            filter_params.extend(program_type)

        exit_sql += ' GROUP BY Enrollment.PersonalID'

        c.execute(exit_sql, filter_params)
        exit_count = c.fetchall()
        
        days_to_create_list = [row[0] for row in exit_count if row[0] is not None]

        conn.close()

        total_exit = len(days_to_create_list)
        count_created_within_x_days = sum(1 for numdays in days_to_create_list if numdays <= days)
        if output==None:
            if total_exit > 0:
                return (count_created_within_x_days / total_exit)
            else:
                return 0
        elif output=="count":
            return total_exit-count_created_within_x_days
        

def LAHSA_data_quality(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    name_instance=Name()
    ssn_instance=SSN()
    dob_instance=DOB()
    race_instance=Race()
    gender_instance=Gender()
    veteran_instance=Veteran()
    disabling_instance=Disabling()
    
    start_instance=StartDate()
    exit_instance=ExitDate()
    destination_instance=Destination()
    hoh_instance=HOH()
    movein_instance=MoveIn()
    location_instance=Location()
    prior_living_instance=PriorLiving()
    los_instance=LOS()
    under_90_instance=LessThan90()
    under_7_instance=LessThan7()
    night_before_instance=NightBefore()
    homelessness_date_instance=DateOfHomelessness()
    times_homeless_instance=TimesHomeless()
    months_homeless_instance=MonthsHomeless()
    
    #Personal
    name=name_instance.name_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db') *0.048711
    ssn=ssn_instance.ssn_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.045845
    dob=dob_instance.dob_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.04298
    race=race_instance.race_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.08596
    gender=gender_instance.gender_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.04298

    #Universal
    veteran=veteran_instance.veteran_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.037249
    disability=disabling_instance.disabling_condition_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*.040155
    start=start_instance.start_date_data_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.051576
    exit=exit_instance.exit_date_data_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.048711
    Hoh=hoh_instance.HoH_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.037249
    destination=destination_instance.destination_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.048711
    location=location_instance.enrollment_coc_total_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.037249
    movein=movein_instance.movein_date_data_accuracy(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.04298
    prior_living_A=prior_living_instance.prior_living_situation_A_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.025788
    prior_living_B=prior_living_instance.prior_living_situation_B_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.025788
    los_A=los_instance.length_of_stay_A_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.028653
    los_B=los_instance.length_of_stay_B_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.028653
    night=night_before_instance.night_before_missing(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.031519
    
    los_under_90=under_90_instance.length_of_stay_lessthan_90_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.025788
    los_under_7=under_7_instance.length_of_stay_lessthan_7_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.028653
    homelessness_date_A=homelessness_date_instance.date_of_homelessness_A_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.031519
    homelessness_date_B=homelessness_date_instance.date_of_homelessness_B_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.031519
    homeless_times_A=times_homeless_instance.times_homeless_A_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.034384
    homeless_times_B=times_homeless_instance.times_homeless_B_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.034384
    homeless_months_A=months_homeless_instance.months_homeless_A_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.031519
    homeless_months_B=months_homeless_instance.months_homeless_B_total(start_date, end_date, program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db')*0.031519


    total =(name+ssn+dob+race+gender+veteran+disability+start+exit+Hoh+destination+location+movein+prior_living_A+prior_living_B+los_A+los_B+night+los_under_90+los_under_7
            +homelessness_date_A+homelessness_date_B+homeless_times_A+homeless_times_B+homeless_months_A+homeless_months_B)
    
    return total
    
#print(LAHSA_data_quality(start_date='2023-01-01',end_date='2023-07-01'))

#Example of modifying function to output either count or list
def ssn_client_refused_doesnt_know(start_date, end_date, type=None,program_id=None, department= None, region=None, program_type=None, db_name='merged_hmis2024.db'):
        conn = sqlite3.connect(db_name)
        c=conn.cursor()
        
        sql = '''
            SELECT COUNT(DISTINCT Enrollment.PersonalID) 
                FROM Enrollment
                LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
                INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
                INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
                WHERE Enrollment.EntryDate <= ? 
                AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
                AND  (Client.SSNDataQuality in (8,9))'''
                
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
        
        list = '''
        SELECT DISTINCT Enrollment.PersonalID
        FROM Enrollment
        LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
        INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        INNER JOIN Client ON Enrollment.PersonalID = Client.PersonalID
        WHERE (
            Enrollment.EntryDate <= ? 
            AND (Exit.ExitDate >= ? OR Exit.ExitDate IS NULL)
            AND (Client.SSNDataQuality IN (8, 9))
        )
    '''

        filter_params = [end_date, start_date]
        outreach_params = [end_date, start_date, end_date]

        if program_id is not None:
            placeholders = ','.join(['?' for _ in program_id])
            sql += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_id)
            outreach_params.extend(program_id)

        if department is not None:
            placeholders = ','.join(['?' for _ in department])
            sql += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Department IN ({placeholders})'
            list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(department)
            outreach_params.extend(department)

        if region is not None:
            placeholders = ','.join(['?' for _ in region])
            sql += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.Region IN ({placeholders})'
            list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(region)
            outreach_params.extend(region)

        if program_type is not None:
            placeholders = ','.join(['?' for _ in program_type])
            sql += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_non_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            sql_active_outreach += f' AND PATHProgramMasterList.PATHProgramType IN ({placeholders})'
            list += f' AND PATHProgramMasterList.MergedProgramID IN ({placeholders})'
            filter_params.extend(program_type)
            outreach_params.extend(program_type)
        
        if type =="count":
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
        
        elif type == "list":
            c.execute(list, filter_params)
            result = c.fetchall()

            # Construct a list of dictionaries with key-value pairs
            records = [{row[0]:'Client Refused'} for row in result]

            return records
        else:
            print("Invalid output type has been provided")
 
#print(ssn_client_refused_doesnt_know(start_date='2023-01-01',end_date='2023-07-01',type="list"))

name=Name()
ssn=SSN()
dob=DOB()
race=Race()
gender=Gender()
veteran=Veteran()
disabling=Disabling()
start=StartDate()
exit=ExitDate()
hoh=HOH()
location=Location()
movein=MoveIn()
destination=Destination()

income=Income()
time=Timeliness()

#print(time.record_creation_exit_average(start_date='2024-01-01',end_date='2024-01-31',program_id=['LA|1319']))

#print(name.name_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',program_id=['LA|5634']))
#print(1-name.name_client_refused_doesnt_know(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-name.name_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-name.name_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))


#print(ssn.ssn_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))
#print(1-ssn.ssn_client_refused_doesnt_know(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))
#print(1-ssn.ssn_missing(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))
#print(1-ssn.ssn_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))

#print(dob.dob_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-dob.dob_client_refused_doesnt_know(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-dob.dob_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-dob.dob_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(race.race_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-race.race_client_refused_doesnt_know(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-race.race_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(gender.gender_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['Veterans']))
#print(gender.gender_client_refused_doesnt_know(start_date='2024-01-01',end_date='2024-01-31',output="count",department=['Veterans']))
#print(gender.gender_missing(start_date='2024-01-01',end_date='2024-01-31',output="count",department=['Veterans']))

#print(veteran.veteran_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',program_id=['LA|5634']))
#print(1-veteran.veteran_client_refused_doesnt_know(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-veteran.veteran_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-veteran.veteran_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(disabling.disabling_condition_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-disabling.disabling_condition_client_refused(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-disabling.disabling_condition_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-disabling.disabling_condition_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(start.start_date_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(exit.exit_date_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(hoh.HoH_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-hoh.HoH_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-hoh.HoH_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(location.enrollment_coc_total_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-location.enrollment_coc_missing(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))
#print(1-location.enrollment_coc_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(movein.movein_date_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['San Diego']))

#print(destination.destination_total_accuracy(start_date='2024-02-01',end_date='2024-02-29',program_id=['LA|5634']))
#print(destination.destination_client_refused_doesnt_know(start_date='2024-02-01',end_date='2024-02-29',program_id=['LA|5634']))
#print(1-destination.destination_missing(start_date='2024-02-01',end_date='2024-02-29',program_id=['LA|5634']))
#print(destination.number_leavers(start_date='2024-02-01',end_date='2024-02-29', program_id=['LA|5821']))

#print(active_clients(start_date='2024-02-01',end_date='2024-02-29',program_id=['LA|5634']))

#print(income.starting_income_client_refused(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))
#print(income.starting_income_missing(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))
#print(income.starting_income_data_accuracy(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))

#print(income.annual_income_client_refused(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))
#print(income.annual_income_missing(start_date='2024-01-01',end_date='2024-01-31',department=['Orange County']))

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter
def line_chart_specific_days(function, filename):
    department_colors = {
    'San Diego': '#FFB7B2',
    'Santa Barbara': '#FFDAC1',
    'Orange County': '#E2F0CB',
    'Santa Clara': '#B5EAD7',
    'Families': '#C7CEEA',
    'Metro LA': '#E6C8FE',
    'Permanent Supportive Services': '#8097BA',
    'South County': '#F3D88E',
    'Veterans': '#A7D1AD',
    'West LA': '#A7D1D1',
    }

    days_of_interest = [1, 4, 7, 11, 14]

    # Create an instance of the class containing the method
    timeliness_instance = Timeliness()

    # Collect results for the specific days
    results_by_department = {d: [] for d in department_colors}
    agency_results = []

    for days in days_of_interest:
        parameters = {"start_date": "2024-01-01", "end_date": "2024-01-31", "days": days}
        agency_number = function(timeliness_instance, **parameters)
        agency_results.append(agency_number)

        for d, color in department_colors.items():
            parameters["department"] = [d]
            number = function(timeliness_instance, **parameters)
            results_by_department[d].append(number)

    # Create the line plot
    width = 3.625
    height = 2.167
    image_dpi = 600
    fig, ax = plt.subplots(figsize=(width, height), dpi=image_dpi)

    for d, values in results_by_department.items():
        ax.plot(days_of_interest, values, linestyle='-', label=f'{d}', linewidth=0.5, markersize=2, color=department_colors[d])

    ax.plot(days_of_interest, agency_results, linestyle='-', label='Agency', linewidth=.75, marker='o',markersize=1, color='black')
    ax.axhline(0, color='white', linestyle='--')

    # Set x-axis tick labels with font size
    ax.set_xticks(days_of_interest)
    ax.set_xticklabels([f'Day {days}' for days in days_of_interest], fontsize=4, ha='center')

    ax.yaxis.set_major_formatter(PercentFormatter(100))
    ax.set_yticklabels([f'{tick * 100:.1f}%' for tick in ax.get_yticks()], fontsize=4)

    # Set y-axis limit to start from zero

    ax.legend(loc='upper left', bbox_to_anchor=(-0.025,-0.1), fontsize=4, ncol=4)

    # Save the plot as an image
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    #plt.show()
    
#print(line_chart_specific_days(Timeliness.percent_exit_records_created_within_x_days,'exit_timeliness.png'))


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

#print(personal_data_quality(start_date='2024-02-01',end_date='2024-02-29'))

#print(name.name_total_accuracy(start_date='2024-01-01', end_date='2024-01-31', output='list'))

def pt_list(start_date, end_date, type=None, program_id=None, department=None, region=None, program_type=None, db_name='merged_hmis2024.db'):
    # Get name and SSN lists
    name_list = name.name_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    ssn_list = ssn.ssn_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    dob_list=dob.dob_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    race_list=race.race_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    gender_list=gender.gender_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    
    # Concatenate the two lists and convert to a set to remove duplicates
    pii_list = set(name_list + ssn_list + dob_list + race_list + gender_list)
    
    # Get veteran and disabling condition lists
    veteran_list = veteran.veteran_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    start_list=start.start_date_data_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    hoh_list=hoh.HoH_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    coc_list=location.enrollment_coc_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    disabling_list = disabling.disabling_condition_total_accuracy(start_date=start_date, end_date=end_date, output='list',program_id=program_id)
    
    universal_list=set(veteran_list +disabling_list + start_list + hoh_list + coc_list)
    
    destination_list=destination.destination_total_accuracy(start_date=start_date, end_date=end_date, output='list', program_id=program_id)
    
    income_list=set(destination_list)
    # Create a dictionary to store IDs and their corresponding issues
    id_issue_dict = {}
    
    for id in pii_list:
        id_issue_dict[id] = 'personal_issue'

    for id in universal_list:
        if id in id_issue_dict:
            if id_issue_dict[id] == 'personal_issue':
                id_issue_dict[id] = 'personal/universal'
        else:
            id_issue_dict[id] = 'universal_issue'

    for id in income_list:
        if id in id_issue_dict:
            if id_issue_dict[id] == 'personal_issue':
                id_issue_dict[id] = 'personal/income'
            elif id_issue_dict[id] == 'universal_issue':
                id_issue_dict[id] = 'universal/income'
            elif id_issue_dict[id] == 'personal/universal':
                id_issue_dict[id] = 'personal/universal/income'
        else:
            id_issue_dict[id] = 'income_issue'

    
    return id_issue_dict



#print(personal_data_quality(start_date='2024-01-01',end_date='2024-01-31',department=['South County']))

