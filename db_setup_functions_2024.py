import sqlite3
import os
import pandas as pd
import requests
import json
import csv

import measure_definitions_2024 as m
import KPI_measures_2024 as kpi

def database_initialization(database_name):

    if os.path.exists(database_name):
        os.remove(database_name)

    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    #D - date
    #T - datetime
    #I - int
    #M - float
    #M+ - float unsigned
    #S# - varchar(#)

    # Export Table
    c.execute('''CREATE TABLE Export
    (ExportID varchar(32) primary key,
    SourceType int,
    SourceID varchar(32),
    SourceName varchar(50),
    SourceContactFirst varchar(50),
    SourceContactLast varchar(50),
    SourceContactPhone varchar(10),
    SourceContactExtension varchar(5),
    SourceContactEmail varchar(320),
    ExportDate datetime,
    ExportStartDate date,
    ExportEndDate date,
    SoftwareName varchar(50),
    SoftwareVersion varchar(50),
    CSVVersion varchar(50),
    ExportPeriodType int,
    ExportDirective int,
    HashStatus int,
    ImplementationID varchar(200)
    )''')

# Organization Table
    c.execute('''CREATE TABLE Organization
    (OrganizationID varchar(32) primary key,
    OrganizationName varchar(200),
    VictimServiceProvider int,
    OrganizationCommonName varchar(200),
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

# User Table
    c.execute('''CREATE TABLE User
    (UserID varchar(32) primary key,
    UserFirstName varchar(50),
    UserLastName varchar(50),
    UserPhone varchar(10),
    UserExtension varchar(5),
    UserEmail varchar(320),
    DateCreated datetime,
    DateUpdated datetime,
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

# Project Table
    c.execute('''CREATE TABLE Project
    (ProjectID varchar(32) primary key,
    OrganizationID varchar(32),
    ProjectName varchar(200),
    ProjectCommonName varchar(200),
    OperatingStartDate date,
    OperatingEndDate date,
    ContinuumProject int,
    ProjectType int,
    HousingType int,
    RRHSubType int,
    ResidentialAffiliation int,
    TargetPopulation int,
    HOPWAMedAssistedLivingFac int,
    PITCount int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID),
    foreign key (OrganizationID) references Organization(OrganizationID)
    )''')

# Funder Table
    c.execute('''CREATE TABLE Funder
    (FunderID varchar(32) primary key,
    ProjectID varchar(32),
    Funder int,
    OtherFunder varchar(100),
    GrantID varchar(100),
    StartDate date,
    EndDate date,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID),
    foreign key (ProjectID) references Project(ProjectID)
    )''')

#ProjectCoc Table
    c.execute('''CREATE TABLE ProjectCoC
    (ProjectCoCID varchar(32) primary key,
    ProjectID varchar(32),
    CoCCode varchar(6),
    Geocode varchar(6),
    Address1 varchar(100),
    Address varchar(100),
    City varchar(50),
    State varchar(2),
    ZIP varchar(5),
    GeographyType int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID),
    foreign key (ProjectID) references Project(ProjectID)
    )''')

#Inventory Table
    c.execute('''CREATE TABLE Inventory
    (InventoryID varchar(32) primary key,
    ProjectID varchar(32),
    CoCCode varchar(6),
    HouseholdType int,
    Availability int,
    UnitInventory int,
    BedInventory int,
    CHVetBedInventory int,
    YouthVetBedInventory int,
    VetBedInventory int,
    CHYouthBedInventory int,
    YouthBedInventory int,
    CHBedInventory int,
    OtherBedInventory int,
    ESBedType int,
    InventoryStartDate date,
    InventoryEndDate date,
    Date Created datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ProjectID) references Project(ProjectID),
    foreign key (CoCCode) references ProjectCoC(ProjectID),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Affiliation Table
    c.execute('''CREATE TABLE Affiliation
    (AffiliationID varchar(32) primary key,
    ProjectID varchar(32),
    ResProjectID varchar(32),
    DateCreated datetime,
    DateUpdate datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ProjectID) references Project(ProjectID),
    foreign key (ProjectID) references Project(ProjectID),
    foreign key (ExportID) references Export(ExportID)
    )''')

#HMISParticipation Table
    c.execute('''CREATE TABLE HMISParticipation
    (HMISParticipationID varchar(32) primary key,
    ProjectID varchar(32),
    HMISParticipationType int,
    HMISParticipationStatusStartDate date,
    HMISParticipationStatusEndDate date,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID),
    foreign key (ProjectID) references Project(ProjectID)
    )''')

#CEParticipation Table
    c.execute('''CREATE TABLE CEParticipation
    (CEParticipationID varchar(32) primary key,
    ProjectID varchar(32),
    AccessPoint int,
    PreventionAssessment int,
    CrisisAssessment int,
    HousingAssessment int,
    DirectServices int,
    ReceivesReferrals int,
    CEParticipationStatusStartDate date,
    CEParticipationStatusEndDate date,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime, 
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID),
    foreign key (ProjectID) references Project(ProjectID)
    )''')

#Client Table
    c.execute('''CREATE TABLE Client
    (PersonalID varchar(32) primary key,
    FirstName varchar(50),
    MiddleName varchar(50),
    LastName varchar(50),
    NameSuffix varchar(50),
    NameDataQuality int,
    SSN varchar(9),
    SSNDataQuality int,
    DOB date,
    DOBDataQuality int,
    AmIndAKNative int,
    Asian int,
    BlackAfAmerican int,
    HispanicLatinaeo int,
    MidEastNAfrican int,
    NativeHIPacific int,
    White int,
    RaceNone int,
    AdditionalRaceEthnicity varchar(100),
    Woman int,
    Man int,
    NonBinary int,
    CulturallySpecific int,
    Transgender int,
    Questioning int,
    DifferentIdentity int,
    GenderNone int,
    DifferentIdentityText varchar(100),
    VeteranStatus int,
    YearEnteredService int,
    YearSeparated int,
    WorldWarII int, 
    KoreanWar int,
    VietnamWar int,
    DesertStorm int,
    AfghanistanOEF int,
    IraqOIF int,
    IraqOND int,
    OtherTheater int,
    MilitaryBranch int,
    DischargeStatus int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Enrollment Table
    c.execute('''CREATE TABLE Enrollment
    (EnrollmentID varchar(32) primary key,
    PersonalID varchar(32),
    ProjectID varchar(32),
    EntryDate date,
    HouseholdID varchar(32),
    RelationshipToHoH int,
    EnrollmentCoC varchar(6),
    LivingSituation int,
    RentalSubsidyType int,
    LengthOfStay int,
    LOSUnderThreshold int,
    PreviousStreetESSH int,
    DateToStreetESSH date,
    TimesHomelessPastThreeYears int,
    MonthsHomelessPastThreeYears int,
    DisablingCondition int,
    DateOfEngagement date,
    MoveInDate date,
    DateOfPATHStatus date,
    ClientEnrolledInPATH int,
    ReasonNotEnrolled int,
    PercentAMI int,
    ReferralSource int,
    CountOutreachReferralApproaches int,
    DateOfBCPStatus date,
    EligibleForRHY int,
    ReasonNoServices int,
    RunawayYouth int,
    SexualOrientation int,
    SexualOrientationOther varchar(100),
    FormerWardChildWelfare int,
    ChildWelfareYears int,
    ChildWelfareMonths int,
    FormerWardJuvenileJustice int,
    JuvenileJusticeYears int,
    JuvenileJusticeMonths int,
    UnemploymentFam int,
    MentalHealthDisorderFam int,
    PhysicalDisabilityFam int,
    AlcoholDrugUseDisorderFam int,
    InsufficientIncome int,
    IncarceratedParent int,
    VAMCStation varchar(5),
    TargetScreenReqd int,
    TimeToHousingLoss int,
    AnnualPercentAMI int,
    LiteralHomelessHistory int,
    ClientLeaseholder int,
    HOHLeaseholder int,
    SubsidyAtRisk int,
    EvictionHistory int,
    CriminalRecord int,
    IncarceratedAdult int,
    PrisonDischarge int,
    SexOffender int,
    DisabledHoH int,
    CurrentPregnant int,
    SingleParent int,
    DependentUnder6 int,
    HH5Plus int,
    CoCPrioritized int,
    HPScreeningScore int,
    ThresholdScore int,
    TranslationNeeded int,
    PreferredLanguage int,
    PreferredLanguageDifferent varchar(100),
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDEleted datetime,
    ExportID varchar(32),
    foreign key (ProjectID) references Project(ProjectID),
    foreign key (EnrollmentCoC) references Project(ProjectID),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Exit Table
    c.execute('''CREATE TABLE Exit
    (ExitID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    ExitDate date,
    Destination int,
    DestinationSubsidyType int,
    OtherDestination varchar(50),
    HousingAssessment int,
    SubsidyInformation int,
    ProjectCompletionStatus int,
    EarlyExitReason int,
    ExchangeForSex int,
    ExchangeForSexPastThreeMonths int,
    CountOfExchangeForSex int,
    AskedOrForcedToExchangeForSex int,
    AskedOrForcedToExchangeForSexPastThreeMonths int,
    WorkplaceViolenceThreats int,
    WorkplacePromiseDifference int,
    CoercedToContinueWork int,
    LaborExploitPastThreeMonths int,
    CounselingReceived int,
    IndividualCounseling int,
    FamilyCounseling int,
    GroupCounseling int,
    SessionCountAtExit int,
    PostExitCounselingPlan int,
    SessionsInPlan int,
    DestinationSafeClient int,
    DestinationSafeWorker int,
    PosAdultConnections int,
    PosPeerConnections int,
    PosCommunityConnections int,
    AftercareDate date,
    AftercareProvided int,
    EmailSocialMedia int,
    Telephone int,
    InPersonIndividual int,
    InPersonGroup int,
    CMExitReason int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#IncomeBenefits Table
    c.execute('''CREATE TABLE IncomeBenefits 
    (IncomeBenefitsID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonaID varchar(32),
    InformationDate date,
    IncomeFromAnySource int,
    TotalMonthlyIncome float unsigned,
    Earned int,
    EarnedAmount float unsigned,
    Unemployment int,
    UnemploymentAmount float signed,
    SSI int,
    SSIAmount float unsigned,
    SSDI int,
    SSDIAmount float unsigned,
    VADisabilityService int,
    VADisabilityServiceAmount float unsigned,
    VADisabilityNonService int,
    VADisabilityNonServiceAmount float unsigned,
    PrivateDisability int,
    PrivateDisabilityAmount float unsigned,
    WorkersComp int,
    WorkersCompAmount float unsigned,
    TANF int,
    TANFAmount float unsigned,
    GA int,
    GAAmount float unsigned,
    SocSecRetirement int,
    SocSecRetirementAmount float unsigned,
    Pension int,
    PensionAmount float unsigned,
    ChildSupport int,
    ChildSupportAmount float unsigned,
    Alimony int,
    AlimonyAmount float unsigned,
    OtherIncomeSource int,
    OtherIncomeSourceAmount float unsigned,
    OtherIncomeSourceIdentify varchar(50),
    BenefitsFromAnySource int,
    SNAP int,
    WIC int,
    TANFChildCare int,
    TANFTransportation int,
    OtherTANF int,
    OtherBenefitsSource int,
    OtherBenefitsSourceIdentity varchar(50),
    InsuranceFromAnySource int,
    Medicaid int,
    NoMedicaidReason int,
    Medicare int,
    NoMedicareReason int,
    SCHIP int,
    NoSCHIPReason int,
    VHAServicesHA int,
    NoVHAReasonHA int,
    EmployerProvided int,
    NoEmployerProvidedReason int,
    COBRA int,
    NoCOBRAReason int,
    PrivatePay int,
    NoPrivatePayReason int,
    StateHealthIns int,
    NoStateHealthInsReason int,
    IndianHealthServices int,
    NoIndianHealthServicesReason int,
    OtherInsurance int,
    OtherInsuranceIdentity varchar(50),
    ADAP int,
    NoADAPReason int,
    RyanWhiteMedDent int,
    NoRyanWhiteReason int,
    ConnectionWithSOAR int,
    DataCollectionStage int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (TotalMonthlyIncome) references IncomeBenefits(IncomeFromAnySource),
    foreign key (ExportID) references Export(ExportID)
    )''')

#HealthAndDV Table
    c.execute('''CREATE TABLE HealthAndDV
    (HealthAndDVID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    InformationDate date,
    DomesticViolenceSurvivor int,
    WhenOccurred int,
    CurrentlyFleeing int,
    GeneralHealthStatus int,
    DentalHealthStatus int,
    MentalHealthStatus int,
    PregnancyStatus int,
    DueDate date,
    DataCollectionStage int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#EmploymentEducation Table
    c.execute('''CREATE TABLE EmploymentEducation
    (EmploymentEducationID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    InformationDate date,
    LastGradeCompleted int,
    SchoolStatus int,
    Employed int,
    EmploymentType int,
    NotEmployedReason int,
    DataCollectionStage int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Disabilities Table
    c.execute('''CREATE TABLE Disabilities
    (DisabilitiesID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    InformationDate date,
    DisabilityType int,
    DisabilityResponse int,
    IndefiniteAndImpairs int,
    TCellCountAvailable int,
    TCellCount int,
    TCellSource int,
    ViralLoadAvailable int,
    ViralLoad int,
    ViralLoadSource int,
    AntiRetroviral int,
    DataCollectionStage int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Services Table
    c.execute('''CREATE TABLE Services
    (ServicesID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    DateProvided date,
    RecordType int,
    TypeProvided int,
    OtherTypeProvided varchar(50),
    MovingOnOtherType varchar(50),
    SubTypeProvided int,
    FAAmount float,
    FAStartDate date,
    FAEndDate date,
    ReferralOutcome int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    ServiceItemId varchar(32),
    ServiceEndDate date,
    foreign key (ExportID) references Export(ExportID)
    )''')

#CurrentLivingSituation Table
    c.execute('''CREATE TABLE CurrentLivingSituation
    (CurrentLivingSitID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    InformationDate date,
    CurrentLivingSituation int,
    CLSSubsidyType int,
    VerifiedBy varchar(100),
    LeaveSituation14Days int,
    SubsequentResidence int,
    ResourcesToObtain int,
    LeaseOwn60Day int,
    MovedTwoOrMore int,
    LocationDetails varchar(250),
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Assessment Table
    c.execute('''CREATE TABLE Assessment
    (AssessmentID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    AssessmentDate date,
    AssessmentLocation varchar(250),
    AssessmentType int,
    AssessmentLevel int,
    PrioritizationStatus int,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    ScreenId varchar(32),
    ScreenName varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#AssessmentQuestions Table    
    c.execute('''CREATE TABLE AssessmentQuestions
    (AssessmentQuestionID varchar(32) primary key,
    AssessmentID varchar(32),
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    AssessmentQuestionGroup varchar(250),
    AssessmentQuestionOrder int,
    AssessmentQuestion varchar(250),
    AssessmentAnswer varchar(500),
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#AssessmentResults Table
    c.execute('''CREATE TABLE AssessmentResults
    (AssessmentResultID varchar(32) primary key,
    AssessmentID varchar(32),
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    AssessmentResultType varchar(250),
    AssessmentResult varchar(250),
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#Event Table
    c.execute('''CREATE TABLE Event
    (EventID varchar(32) primary key,
    EnrollmentID varchar(32),
    PersonalID varchar(32),
    EventDate date,
    Event int,
    ProbSolDivRRResult int,
    ReferralCaseManageAfter int,
    LocationCrisisOrPHHousing varchar(250),
    ReferralResult int,
    ResultDate date,
    DateCreated datetime,
    DateUpdated datetime,
    UserID varchar(32),
    DateDeleted datetime,
    ExportID varchar(32),
    foreign key (ExportID) references Export(ExportID)
    )''')

#YouthEducationStatus
    c.execute('''CREATE TABLE YouthEducationStatus
        (YouthEducationStatusID varchar(32) primary key,
        EnrollmentID varchar(32),
        PersonalID varchar(32),
        InformationDate date,
        CurrentSchoolAttend int,
        MostRecentEdStatus int,
        CurrentEdStatus int,
        DataCollectionStage int,
        DateCreated datetime,
        DateUpdated datetime,
        UserID varchar(32),
        DateDeleted datetime,
        ExportID varchar(32),
        foreign key (ExportID) references Export(ExportID)
        )''')    

#CustomClientService Table
    c.execute('''CREATE TABLE CustomClientServices
    (CustomClientServiceID varchar(32) primary key,
    ClientID varchar(32),
    ClientProgramID varchar(32),
    AgencyID varchar(32),
    ServiceItemID varchar(32),
    ServiceItemName varchar(32),
    StartDate date,
    EndDate date,
    UserID varchar(32),
    AddedDate datetime,
    LastUpdated datetime,
    Private int,
    DepartmentID varchar(32),
    UserUpdatedID varchar(32),
    Deleted varchar(32),
    AgencyDeletedID varchar(32),
    foreign key (ClientID) references Client(ClientID)
    )''')

#CustomClientServiceAttendance Table
    c.execute('''CREATE TABLE CustomClientServiceAttendance
    (CustomClientServiceAttendanceID varchar(32) primary key,
    CustomClientServiceID varchar(32),
    Date date,
    time datetime,
    UserID int, 
    AddedDate datetime,
    LastUpdated datetime,
    UserUpdatedID int       
    )''')

#CustomClientServiceExpense
    c.execute('''CREATE TABLE CustomClientServiceExpense
        (CustomClientServiceExpenseID varchar(32) primary key,
        CustomClientServiceID varchar(32),
        ServiceItemID varchar(32),
        FundingID varchar(32),
        Date date,
        Amount varchar(32),
        CheckNumber varchar(32),
        Vendor varchar(32),
        Notes varchar(32),
        AttendanceID varchar(32),
        AddedDate datetime,
        LastUpdated datetime,
        UserUpdated varchar(32)
        )''')
    
    # AdditionalInformation Table
    c.execute('''CREATE TABLE AdditionalInformation
            (EnrollmentID varchar(32) primary key,
            PersonalID varchar(32),
            HouseholdID varchar(32),
            ChronicallyHomeless boolean)''')
    
    # DocReady Table
    c.execute('''CREATE TABLE DocReady
            (PersonalID varchar(32),
            EnrollmentID varchar(32),
            HouseholdID varchar(32),
            SSCard varchar(32),
            DriverLicense varchar(32))''')
    
    # HSPPlan Table
    c.execute('''CREATE TABLE HSPPlan
            (PersonalID varchar(32),
            EnrollmentID varchar(32),
            HouseholdID varchar(32),
            HSPPlan varchar(32))''')


    print("Initialized", database_name)

    conn.commit()
    conn.close

def append_db_name_to_id_columns(database_name):
    # Open the SQLite database
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Get the list of tables in the current database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    database_short_name = database_name.rsplit('\\')[-1].removesuffix('.db')

    if database_short_name == "LB":
        database_short_name = "LA"

    # Iterate through the tables and append the database name to "ID" columns
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [column[1] for column in cursor.fetchall()]

        for column_name in columns:

            if column_name.endswith("ID") or column_name.endswith("Id"):
                print("Appending ID to", database_name, table_name, column_name)
                cursor.execute(
                    f"UPDATE {table_name} SET {column_name} = '{database_short_name}' || '|' || {column_name} ;"
                )

    conn.commit()
    conn.close()

def load_data_from_csv(database_name, folder_name):
    conn = sqlite3.connect(os.path.join(folder_name, database_name))
    cursor = conn.cursor()

    # Get the list of tables in the current database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    # Iterate through the tables and load data from CSV files with the same names
    for table in tables:
        table_name = table[0]
        csv_filename = os.path.join(database_name.removesuffix('.db'), table_name + ".csv")
        
        if os.path.exists(csv_filename):
            print("Loading Data to table", database_name, table_name)
            df = pd.read_csv(csv_filename, low_memory=False)
            # Add missing columns to SB
            if 'SB.db' in database_name:
                if table_name == 'Assessment':
                    df['ScreenId'] = None
                    df['ScreenName'] = None
                if table_name == 'Services':
                    df['ServiceItemID'] = None
                    df['ServiceEndDate'] = None
            if table_name=='DocReady':
                df.rename(columns={'Clients Client ID': 'PersonalID','Enrollments Enrollment ID':'EnrollmentID','Enrollments Household ID':'HouseholdID','Social Security':'SSCard',"Driver's License or State ID Card":'DriverLicense'},inplace=True)
                df.to_csv(csv_filename,index=False)
            if table_name=='HSPPlan':
                df.rename(columns={'Clients Client ID': 'PersonalID','Enrollments Enrollment ID':'EnrollmentID','Enrollments Household ID':'HouseholdID','Clients Client File Other Name':'HSPPlan'},inplace=True) 
                df.to_csv(csv_filename,index=False)

            df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Commit changes and close the database
    conn.commit()
    conn.close()
    
def sharepoint_master_programs(database_directory, merged_database, PATH_master_list):
    
    if os.path.exists(PATH_master_list):
        os.remove(PATH_master_list)
    
    # Activation code
    client_id = 'abaf8d90-cfa8-488c-b1fc-2d69cd05cd61'
    client_secret = 'X2adVMKMJk/6Bi4S8c5OsUODCgyePXU+MWtxje2Tzw8='
    tenant = 'path940'
    tenant_id = '624b8bb6-d053-4011-b5e6-ca2264413f6a'
    client_id = client_id + '@' + tenant_id

    data = {    
        'grant_type': 'client_credentials',
        'resource': "00000003-0000-0ff1-ce00-000000000000/" + tenant + ".sharepoint.com@" + tenant_id,
        'client_id': client_id,
        'client_secret': client_secret,
    }

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    url = "https://accounts.accesscontrol.windows.net/624b8bb6-d053-4011-b5e6-ca2264413f6a/tokens/OAuth/2"
    try:
        r = requests.post(url, data=data, headers=headers)
        r.raise_for_status()  # Raise an exception for unsuccessful requests
        json_data = r.json()
    except requests.exceptions.HTTPError as e:
        print(f"Failed to fetch access token. Error: {e}")
        exit()

    headers = {
        'Authorization': "Bearer " + json_data.get('access_token', ''),
        'Accept': 'application/json;odata=verbose',
        'Content-Type': 'application/json;odata=verbose'
    }

    all_data = []

    # Initial URL for the first page
    url = "https://path940.sharepoint.com/sites/fdfds/_api/web/lists/getbytitle('Program Master List')/items"

    while url:
        try:
            r2 = requests.get(url, headers=headers)
            r2.raise_for_status()  # Raise an exception for unsuccessful requests
            data = r2.json()

            # Append the data from this page to the all_data list
            all_data.extend(data.get('d', {}).get('results', []))

            # Check if there are more pages to retrieve
            next_link = data.get('d', {}).get('__next')
            if next_link:
                # Extract the URL for the next page from the next_link string
                url = next_link
            else:
                # No more pages, break out of the loop
                break
        except requests.exceptions.HTTPError as e:
            print(f"Failed to fetch data. Status code: {e.response.status_code}")
            break
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response. Error: {e}")
            break

    # Now 'all_data' contains all the entries
    print(f"Total programs pulled from sharepoint: {len(all_data)}")

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(os.path.join(database_directory, PATH_master_list))
    cursor = conn.cursor() 

    cursor.execute('DROP TABLE IF EXISTS PATHProgramMasterList')

    cursor.execute('''
        CREATE TABLE PATHProgramMasterList (
            MergedProgramID TEXT PRIMARY KEY,
            ProgramName Text,
            Region TEXT,
            Department TEXT,
            CoCCode TEXT,
            PrimaryDataSystem TEXT,
            SecondaryDataSystem TEXT,
            DataSystemID TEXT,
            DataSystemProgramName TEXT,
            DataSystemProgramType TEXT,
            DataSystemStatus TEXT,
            PATHProgramType TEXT,
            GrantCode TEXT,
            ContractTerm TEXT,
            ContractStatus TEXT,
            NotesQuestions TEXT,
            foreign key (MergedProgramID) references Project(ProjectID)     
        )
    ''')
    # Insert SharePoint data into the database
    for item in all_data:

        cursor.execute('''
            INSERT INTO PATHProgramMasterList (
                ProgramName, Region, Department,
                CoCCode, PrimaryDataSystem, SecondaryDataSystem,
                DataSystemID, DataSystemProgramName, DataSystemProgramType,
                DataSystemStatus, PATHProgramType, ContractStatus,
                NotesQuestions, GrantCode, MergedProgramID, ContractTerm
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (            
            item.get('Title'), item.get('field_1'), item.get('field_2'),
            item.get('field_3'), item.get('field_4'), item.get('field_5'),
            item.get('field_6'), item.get('field_7'), item.get('field_8'),
            item.get('field_9'), item.get('field_10'), item.get('field_11'),
            item.get('field_12'), item.get('GrantCode'),
            item.get('Merged_x0020_Program_x0020_ID'), item.get('Grant_x0028_s_x0029_Term0')
        ))

    conn.commit()
    conn.close()

    conn = sqlite3.connect(merged_database)
    cursor = conn.cursor()

    print("Data has been successfully inserted into the PATH Program Database.")

    cursor.execute(f"ATTACH DATABASE '{os.path.join(database_directory, PATH_master_list)}' AS SharePoint")

    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='PATHProgramMasterList';")

    existing_table = cursor.fetchone()
    if not existing_table:
        # Get the CREATE TABLE statement for the table in the attached SharePoint database
        cursor.execute(f"SELECT sql FROM SharePoint.sqlite_master WHERE name='PATHProgramMasterList';")
        create_table_sql = cursor.fetchone()[0]

        # Execute the CREATE TABLE statement in the merged database
        cursor.execute(create_table_sql)

    # Insert data from the attached SharePoint table into the merged table

    cursor.execute(f"INSERT INTO PATHProgramMasterList SELECT * FROM SharePoint.PATHProgramMasterList;")

    print("Successfully Added PATH Master List to merged database")

    conn.commit()
    conn.close()
    
def update_move_in_dates(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Fetch unique HouseholdID-MoveInDate pairs
    cursor.execute("SELECT DISTINCT HouseholdID, MoveInDate FROM Enrollment INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID WHERE MoveInDate IS NOT NULL AND PATHProgramMasterList.MergedProgramID IS NOT NULL;")
    household_data = cursor.fetchall()

    # Iterate through the unique HouseholdID-MoveInDate pairs
    for household_id, move_in_date in household_data:
        # Update all records with the same HouseholdID
        cursor.execute("UPDATE Enrollment SET MoveInDate = ? WHERE HouseholdID = ?;", (move_in_date, household_id))
        print(f"Updated MoveInDate for records with HouseholdID {household_id}")

    conn.commit()
    conn.close()

def update_engagement_dates(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT HouseholdID, DateOfEngagement FROM Enrollment INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID WHERE DateOfEngagement IS NOT NULL AND PATHProgramMasterList.MergedProgramID IS NOT NULL;")
    household_data = cursor.fetchall()

    for household_id, date_of_engagement in household_data:
        cursor.execute("UPDATE Enrollment SET DateOfEngagement = ? WHERE HouseholdID = ?;", (date_of_engagement, household_id))
        print(f"Updated DateOfEngagement for records with HouseholdID {household_id}")

    conn.commit()
    conn.close()

def update_SD_entry_dates(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    
    sql_delete = """
    DELETE FROM Enrollment
    WHERE EXISTS (
        SELECT 1
        FROM PATHProgramMasterList
        WHERE Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        AND PATHProgramMasterList.Region = "San Diego County"
        AND PATHProgramMasterList.DataSystemProgramType IN ('PH - Housing with Services (no disability required for entry)', 'PH - Permanent Supportive Housing (disability required for entry)')
    )
    AND MoveInDate IS NULL
    """
    cursor.execute(sql_delete)

    sql_delete = """
    DELETE FROM Enrollment
    WHERE EXISTS (
        SELECT 1
        FROM PATHProgramMasterList
        WHERE Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
        AND PATHProgramMasterList.Region = "San Diego County"
        AND PATHProgramMasterList.DataSystemProgramType IN ('PH - Housing with Services (no disability required for entry)', 'PH - Permanent Supportive Housing (disability required for entry)')
    )
    AND MoveInDate IS NULL
    """
    cursor.execute(sql_delete)

    conn.commit()
    conn.close()

def import_agency_indicators(filename):

    indicator_dict = {}
    with open(filename, 'r') as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            ProgramType = row['ProgramType']
            IndicatorCategory = row['IndicatorCategory']
            IndicatorName = row['IndicatorName']
            IndicatorFunction = getattr(m, row['IndicatorFunction'], None)
            IndicatorParameter = row['IndicatorParameter']
            ParameterArgument = row['ParameterArgument']   
            Format = row['Format']  
            Target = row['Target']      
            IndicatorDomain = row['IndicatorDomain']
            Definition = row['Definition']  
            IndicatorType = row['IndicatorType']
            IndicatorFooter = row['IndicatorFooter']      
            # convert string to number for arguement and target
            try:
                Target = float(Target)
                if Target > 1:
                    Target = int(Target)
                else:
                    Target = float(Target)
            except ValueError:
                pass

            try:
                ParameterArgument = float(ParameterArgument)
            except ValueError:
                try:
                    ParameterArgument = int(ParameterArgument)
                except ValueError:
                    pass
            
            
            ParameterDict = {}
            if IndicatorParameter:
                ParameterDict = {IndicatorParameter : ParameterArgument}

            if ProgramType in indicator_dict:
                if IndicatorCategory in indicator_dict[ProgramType]:
                    if IndicatorName in indicator_dict[ProgramType][IndicatorCategory]:
                        indicator_dict[ProgramType][IndicatorCategory][IndicatorName].update({
                            'IndicatorFunction': IndicatorFunction,
                            'IndicatorParameter': ParameterDict,
                            'Format': Format,
                            'Target': Target,
                            'IndicatorDomain':IndicatorDomain,
                            'Definition':Definition,
                            'IndicatorType':IndicatorType,
                            'IndicatorFooter':IndicatorFooter
                        })
                    else:
                        indicator_dict[ProgramType][IndicatorCategory][IndicatorName] = {
                            'IndicatorFunction': IndicatorFunction,
                            'IndicatorParameter': ParameterDict,
                            'Format': Format,
                            'Target': Target,
                            'IndicatorDomain':IndicatorDomain,
                            'Definition':Definition,
                            'IndicatorType':IndicatorType,
                            'IndicatorFooter':IndicatorFooter
                        }
                else:
                    indicator_dict[ProgramType][IndicatorCategory] = {IndicatorName: {
                        'IndicatorFunction': IndicatorFunction,
                        'IndicatorParameter': ParameterDict,
                        'Format': Format,
                        'Target': Target,
                        'IndicatorDomain':IndicatorDomain,
                        'Definition':Definition,
                        'IndicatorType':IndicatorType,
                        'IndicatorFooter':IndicatorFooter
                    }}
            else:
                indicator_dict[ProgramType] = {IndicatorCategory: {IndicatorName: {
                    'IndicatorFunction': IndicatorFunction,
                    'IndicatorParameter': ParameterDict,
                    'Format': Format,
                    'Target': Target,
                    'IndicatorDomain':IndicatorDomain,
                    'Definition':Definition,
                    'IndicatorType':IndicatorType,
                    'IndicatorFooter':IndicatorFooter
                }}}
            
    return indicator_dict

def import_kpi(filename):

    import_kpi_dict = {}
    with open(filename, 'r') as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            Region = row['RegionFull']
            RegionShort = row['RegionShort']
            Department = row['Department']
            ProgramType = row['ProgramType']
            GrantCode = tuple(row['GrantCode'].split(','))
            ContractName = row['ContractName']   
            ProgramID = [RegionShort+"|"+pid for pid in row['ProgramID'].split(',')]
            IndicatorName = row['IndicatorName']      
            IndicatorTarget = row['IndicatorTarget']
            IndicatorFunction = row['IndicatorFunction']
            FunctionLocation = row['FunctionLocation']
            IndicatorParameter = row['IndicatorParameter']
            ParameterArgument = row['ParameterArgument']
            Format = row['Format']  
            ReportingCadence = row['ReportingCadence']
            

            if FunctionLocation == "measure_definitions_2024":
                IndicatorFunction = getattr(m, row['IndicatorFunction'], None)
            elif FunctionLocation == "KPI_measures_2024":
                IndicatorFunction = getattr(kpi, row['IndicatorFunction'], None)
            else:
                IndicatorFunction = None


            try:
                IndicatorTarget = float(IndicatorTarget)
                if IndicatorTarget > 1:
                    IndicatorTarget = int(IndicatorTarget)
                else:
                    IndicatorTarget = float(IndicatorTarget)
            except ValueError:
                pass

            try:
                ParameterArgument = float(ParameterArgument)
            except ValueError:
                try:
                    ParameterArgument = int(ParameterArgument)
                except ValueError:
                    pass

            ParameterDict = {}
            if IndicatorParameter:
                ParameterDict = {IndicatorParameter : ParameterArgument}

            if Region in import_kpi_dict:
                if Department in import_kpi_dict[Region]:
                    if ProgramType in import_kpi_dict[Region][Department]:
                        if GrantCode in import_kpi_dict[Region][Department][ProgramType]:
                            if IndicatorName in import_kpi_dict[Region][Department][ProgramType][GrantCode]:
                                import_kpi_dict[Region][Department][ProgramType][GrantCode][IndicatorName].update({
                                    'ContractName':ContractName,
                                    'ProgramID': ProgramID,
                                    'IndicatorFunction': IndicatorFunction,
                                    'IndicatorTarget': IndicatorTarget,
                                    'IndicatorParameter': ParameterDict,
                                    'Format': Format,
                                    'ReportingCadence': ReportingCadence
                                })
                            else:
                                import_kpi_dict[Region][Department][ProgramType][GrantCode][IndicatorName] = {
                                    'ContractName':ContractName,
                                    'ProgramID': ProgramID,
                                    'IndicatorFunction': IndicatorFunction,
                                    'IndicatorTarget': IndicatorTarget,
                                    'IndicatorParameter': ParameterDict,
                                    'Format': Format,
                                    'ReportingCadence': ReportingCadence
                                }
                        else:
                            import_kpi_dict[Region][Department][ProgramType][GrantCode] = {IndicatorName: {
                                'ContractName':ContractName,
                                'ProgramID': ProgramID,
                                'IndicatorFunction': IndicatorFunction,
                                'IndicatorTarget': IndicatorTarget,
                                'IndicatorParameter': ParameterDict,
                                'Format': Format,
                                'ReportingCadence': ReportingCadence                            
                                }}
                    else:
                        import_kpi_dict[Region][Department][ProgramType] = {GrantCode: {IndicatorName: {
                            'ContractName':ContractName,
                            'ProgramID': ProgramID,
                            'IndicatorFunction': IndicatorFunction,
                            'IndicatorTarget': IndicatorTarget,
                            'IndicatorParameter': ParameterDict,
                            'Format': Format,
                            'ReportingCadence': ReportingCadence                        
                            }}}
                else:
                    import_kpi_dict[Region][Department] = {ProgramType: {GrantCode: {IndicatorName: {
                        'ContractName':ContractName,
                        'ProgramID': ProgramID,
                        'IndicatorFunction': IndicatorFunction,
                        'IndicatorTarget': IndicatorTarget,
                        'IndicatorParameter': ParameterDict,
                        'Format': Format,
                        'ReportingCadence': ReportingCadence                    
                        }}}}
            else:
                import_kpi_dict[Region] =  {Department: {ProgramType: {GrantCode: {IndicatorName: {
                    'ContractName':ContractName,
                    'ProgramID': ProgramID,
                    'IndicatorFunction': IndicatorFunction,
                    'IndicatorTarget': IndicatorTarget,
                    'IndicatorParameter': ParameterDict,
                    'Format': Format,
                    'ReportingCadence': ReportingCadence                
                    }}}}}


    return import_kpi_dict
    
def add_non_HMIS(non_hmis_folder_location, merged_database):
    
    with open(os.path.join(non_hmis_folder_location,'Client.csv'), 'w', newline='') as client_csv:
        csv_writer = csv.writer(client_csv)
        csv_writer.writerow([
            "PersonalID", "FirstName", "MiddleName", "LastName", "NameSuffix",
            "NameDataQuality", "SSN", "SSNDataQuality", "DOB", "DOBDataQuality",
            "AmIndAKNative", "Asian", "BlackAfAmerican", "HispanicLatinaeo",
            "MidEastNAfrican", "NativeHIPacific", "White", "RaceNone",
            "AdditionalRaceEthnicity", "Woman", "Man", "NonBinary",
            "CulturallySpecific", "Transgender", "Questioning", "DifferentIdentity",
            "GenderNone", "DifferentIdentityText", "VeteranStatus",
            "YearEnteredService", "YearSeparated", "WorldWarII", "KoreanWar",
            "VietnamWar", "DesertStorm", "AfghanistanOEF", "IraqOIF", "IraqOND",
            "OtherTheater", "MilitaryBranch", "DischargeStatus", "DateCreated",
            "DateUpdated", "UserID", "DateDeleted", "ExportID"
        ])
    client_df = pd.read_csv(os.path.join(non_hmis_folder_location,'Client.csv'))
        
    with open(os.path.join(non_hmis_folder_location,'Enrollment.csv'), 'w', newline='') as enrollment_csv:
        csv_writer = csv.writer(enrollment_csv)
        csv_writer.writerow([
            "EnrollmentID", "PersonalID", "ProjectID", "EntryDate", "HouseholdID",
            "RelationshipToHoH", "EnrollmentCoC", "LivingSituation", "RentalSubsidyType",
            "LengthOfStay", "LOSUnderThreshold", "PreviousStreetESSH", "DateToStreetESSH",
            "TimesHomelessPastThreeYears", "MonthsHomelessPastThreeYears",
            "DisablingCondition", "DateOfEngagement", "MoveInDate", "DateOfPATHStatus",
            "ClientEnrolledInPATH", "ReasonNotEnrolled", "PercentAMI", "ReferralSource",
            "CountOutreachReferralApproaches", "DateOfBCPStatus", "EligibleForRHY",
            "ReasonNoServices", "RunawayYouth", "SexualOrientation", "SexualOrientationOther",
            "FormerWardChildWelfare", "ChildWelfareYears", "ChildWelfareMonths",
            "FormerWardJuvenileJustice", "JuvenileJusticeYears", "JuvenileJusticeMonths",
            "UnemploymentFam", "MentalHealthDisorderFam", "PhysicalDisabilityFam",
            "AlcoholDrugUseDisorderFam", "InsufficientIncome", "IncarceratedParent",
            "VAMCStation", "TargetScreenReqd", "TimeToHousingLoss", "AnnualPercentAMI",
            "LiteralHomelessHistory", "ClientLeaseholder", "HOHLeaseholder", "SubsidyAtRisk",
            "EvictionHistory", "CriminalRecord", "IncarceratedAdult", "PrisonDischarge",
            "SexOffender", "DisabledHoH", "CurrentPregnant", "SingleParent",
            "DependentUnder6", "HH5Plus", "CoCPrioritized", "HPScreeningScore",
            "ThresholdScore", "TranslationNeeded", "PreferredLanguage",
            "PreferredLanguageDifferent", "DateCreated", "DateUpdated", "UserID",
            "DateDeleted", "ExportID"
        ])
    enrollment_df = pd.read_csv(os.path.join(non_hmis_folder_location,'Enrollment.csv'))

    with open(os.path.join(non_hmis_folder_location,'Exit.csv'), 'w', newline='') as exit_csv:
        csv_writer = csv.writer(exit_csv)
        csv_writer.writerow([
            "ExitID", "EnrollmentID", "PersonalID", "ExitDate", "Destination",
            "DestinationSubsidyType", "OtherDestination", "HousingAssessment",
            "SubsidyInformation", "ProjectCompletionStatus", "EarlyExitReason",
            "ExchangeForSex", "ExchangeForSexPastThreeMonths", "CountOfExchangeForSex",
            "AskedOrForcedToExchangeForSex", "AskedOrForcedToExchangeForSexPastThreeMonths",
            "WorkplaceViolenceThreats", "WorkplacePromiseDifference",
            "CoercedToContinueWork", "LaborExploitPastThreeMonths", "CounselingReceived",
            "IndividualCounseling", "FamilyCounseling", "GroupCounseling",
            "SessionCountAtExit", "PostExitCounselingPlan", "SessionsInPlan",
            "DestinationSafeClient", "DestinationSafeWorker", "PosAdultConnections",
            "PosPeerConnections", "PosCommunityConnections", "AftercareDate",
            "AftercareProvided", "EmailSocialMedia", "Telephone", "InPersonIndividual",
            "InPersonGroup", "CMExitReason", "DateCreated", "DateUpdated", "UserID",
            "DateDeleted", "ExportID"
        ])
    exit_df = pd.read_csv(os.path.join(non_hmis_folder_location,'Exit.csv'))

    for root, dirs, files in os.walk(non_hmis_folder_location):
        for file in files:            
            if file.endswith((".csv", ".xlsx")) and not file.startswith(('Client', 'Enrollment', 'Exit')):
                if file.endswith(".csv"):
                    df = pd.read_csv(os.path.join(root,file))  
                elif file.endswith(".xlsx"):
                    df = pd.read_excel(os.path.join(root,file))

                if "VASH" in os.path.basename(root):
                    df = df.rename(columns={
                                            'Ethnicity':'HispanicLatinaeo',
                                            'Female':'Woman',
                                            'Male':'Man',
                                            'NoSingleGender':'NonBinary'
                                            })
                    
                    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['EntryDate'] = pd.to_datetime(df['EntryDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['ExitDate'] = pd.to_datetime(df['ExitDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['MoveInDate'] = pd.to_datetime(df['MoveInDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                    
                    client_data = df[['PersonalID', 'FirstName', 'MiddleName', 'LastName', 'SSN', 'SSNDataQuality', 'DOB', 'DOBDataQuality', 'AmIndAKNative', 'Asian','BlackAfAmerican','HispanicLatinaeo','NativeHIPacific','White','Woman','Man','NonBinary','Transgender','Questioning','VeteranStatus']]
                    enrollment_data = df[['EnrollmentID','PersonalID','EntryDate','RelationshipToHoH','DisablingCondition','LivingSituation','LengthOfStay','LOSUnderThreshold','TimesHomelessPastThreeYears','MonthsHomelessPastThreeYears','MoveInDate']]                   
                    exit_data = df[['ExitDate','EnrollmentID','PersonalID','Destination']].dropna() 

                    enrollment_data['HouseholdID'] = 'H' + enrollment_data['EnrollmentID'].astype(str)
                    enrollment_data['ProjectID'] = os.path.basename(root)

                    exit_data['ExitID'] = 'EX' + enrollment_data['EnrollmentID'].astype(str)
                    
                    client_df = pd.concat([client_df,client_data],ignore_index=True)
                    enrollment_df = pd.concat([enrollment_df,enrollment_data],ignore_index=True)
                    exit_df = pd.concat([exit_df,exit_data],ignore_index=True)

                if "MediCal" in os.path.basename(root):
                    df = df.rename(columns={
                                            'Clients Client ID':'PersonalID',
                                            'Enrollments Enrollment ID':'EnrollmentID',
                                            'Enrollments Household ID':'HouseholdID',
                                            'Enrollments Project Start Date':'EntryDate',
                                            'Enrollments Project Exit Date':'ExitDate',
                                            'Update/Exit Screen Destination':'Destination',
                                            'Enrollments Household Move-In Date':'MoveInDate',
                                            'Clients First Name':'FirstName',
                                            'Clients Middle Name':'MiddleName',
                                            'Clients Last Name':'LastName',
                                            'Clients Name Data Quality':'NameDataQuality',
                                            'Entry Screen Head of Household (Yes / No)':'HoH',
                                            'Entry Screen Relationship to Head of Household':'RelationshipToHoH',
                                            'Clients SSN':'SSN',
                                            'Clients SSN Data Quality':'SSNDataQuality',
                                            'Clients Date of Birth Date':'DOB',
                                            'Clients DoB Data Quality':'DOBDataQuality',
                                            'Clients Gender':'Gender',
                                            'Clients Race and Ethnicity':'RaceEthnicity',
                                            'Clients Veteran Status':'VeteranStatus'
                    })
                    
                    df['ProjectID'] = "MediCal"
                    
                    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['EntryDate'] = pd.to_datetime(df['EntryDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['ExitDate'] = pd.to_datetime(df['ExitDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                   
                    df['EnrollmentID'] = 'MediCal' + df['EnrollmentID'].astype(str)
                    df['HouseholdID'] = 'MediCal' + df['HouseholdID'].astype(str)
                    df['ExitID'] = 'EX' + df['EnrollmentID'].astype(str)

                    df['NameDataQuality'] = df['NameDataQuality'].astype(str).apply(lambda x: 1 if 'Full' in x else (2 if 'Approximate' in x else 99))
                    df['SSNDataQuality'] = df['SSNDataQuality'].astype(str).apply(lambda x: 1 if 'Full' in x else (2 if 'Approximate' in x else 99))
                    df['DOBDataQuality'] = df['DOBDataQuality'].astype(str).apply(lambda x: 1 if 'Full' in x else (2 if 'Approximate' in x else 99))

                    df['AmIndAKNative'] = df['RaceEthnicity'].str.contains('American Indian').astype(int)
                    df['Asian'] = df['RaceEthnicity'].str.contains('Asian').astype(int)
                    df['BlackAfAmerican'] = df['RaceEthnicity'].str.contains('Black').astype(int)
                    df['HispanicLatinaeo'] = df['RaceEthnicity'].str.contains('Hispanic/Latin').astype(int)
                    df['MidEastNAfrican'] = df['RaceEthnicity'].str.contains('Middle East').astype(int)
                    df['NativeHIPacific'] = df['RaceEthnicity'].str.contains('Pacific').astype(int)
                    df['White'] = df['RaceEthnicity'].str.contains('White').astype(int)
                    df['RaceEthnicity'].fillna('', inplace=True)

                    df['Gender'].fillna('', inplace=True)
                    df['Woman'] = df['Gender'].str.contains('Woman').astype(int)
                    df['Man'] = df['Gender'].str.contains('Man').astype(int)
                    df['NonBinary'] = df['Gender'].str.contains('Non-Binary').astype(int)
                    df['CulturallySpecific'] = df['Gender'].str.contains('Culturally Specific').astype(int)
                    df['Transgender'] = df['Gender'].str.contains('Transgender').astype(int)
                    df['Questioning'] = df['Gender'].str.contains('Questioning').astype(int)
                    df['DifferentIdentity'] = df['Gender'].str.contains('Different').astype(int)

                    df['VeteranStatus'] = df['VeteranStatus'].astype(str).apply(lambda x: 1 if 'Yes' in x else (0 if 'No' in x else 99))

                    yes = df['HoH'] == 'Yes'
                    child = (df['HoH'] != 'Yes') & (df['RelationshipToHoH'].str.contains('child', case=False, na=False))
                    spouse = (df['HoH'] != 'Yes') & (df['RelationshipToHoH'].str.contains('spouse', case=False, na=False))
                    other_relation = (df['HoH'] != 'Yes') & (df['RelationshipToHoH'].str.contains('Other relation', case=False, na=False))
                    non_relation = (df['HoH'] != 'Yes') & (df['RelationshipToHoH'].str.contains('Other: Non-Relation', case=False, na=False))

                    df.loc[yes, 'RelationshipToHoH'] = 1
                    df.loc[child, 'RelationshipToHoH'] = 2
                    df.loc[spouse, 'RelationshipToHoH'] = 3
                    df.loc[other_relation, 'RelationshipToHoH'] = 4
                    df.loc[non_relation, 'RelationshipToHoH'] = 5

                    df['RelationshipToHoH'].fillna(99, inplace=True)

                    df['Destination'] = df['Destination'].fillna('')

                    destination_mapping = {
                        'habitation':116,
                        'Emergency shelter':101,
                        'Safe Haven':118,
                        'foster care':215,
                        'non-psychiatric':206,
                        'Jail':207,
                        'Long-term':225,
                        'Psychiatric hospital':204,
                        'Substance':205,
                        'Transitional':302,
                        'halfway':329,
                        'without emergency':118,
                        'Host Home':332,
                        'family, temporary tenure':312,
                        'friends, temporary tenure':313,
                        'HOPWA TH':327,
                        'friend’s room':336,
                        'family member’s room':335,
                        'family, permanent':422,
                        'friends, permanent':433,
                        'HOPWA PH':426,
                        'Rental by client, no':410,
                        'Rental by client, with':435,
                        'Owned by client, no':411,
                        'Owned by client, with':421,
                        'No exit':30,
                        'Deceased':24,
                        'unable to determine':37,
                        'doesn\'t know':8,
                        'prefers not to answer':9,
                        'refused':9,
                        'not collected':99,
                        'GPD':435,
                        'VASH':435,
                        'RRH':435,
                        'HCV':435,
                        'Public housing':435,
                        'other ongoing':435,
                        'Emergency Housing':435,
                        'Unification':435,
                        'Fost Youth to Independence':435,
                        'Permanent Supportive':435,
                        'Other permanent housing':435
                    }

                    df['Destination'] = df['Destination'].apply(lambda x: next((code for word, code in destination_mapping.items() if word in x.lower()), 99))
                    
                    
                    client_data = df[['PersonalID', 'FirstName', 'MiddleName', 'LastName','NameDataQuality','SSN', 'SSNDataQuality', 'DOB', 'DOBDataQuality', 'AmIndAKNative', 'Asian','BlackAfAmerican','HispanicLatinaeo','MidEastNAfrican','NativeHIPacific','White','Woman','Man','NonBinary','CulturallySpecific','Transgender','Questioning','DifferentIdentity','VeteranStatus']]
                    enrollment_data = df[['EnrollmentID','ProjectID','PersonalID','EntryDate','HouseholdID','RelationshipToHoH','MoveInDate']]                   
                    exit_data = df[['ExitID','ExitDate','EnrollmentID','PersonalID','Destination']].dropna()

                    client_df = pd.concat([client_df,client_data],ignore_index=True)
                    enrollment_df = pd.concat([enrollment_df,enrollment_data],ignore_index=True)
                    exit_df = pd.concat([exit_df,exit_data],ignore_index=True)
                    
                if "Vertical Change" in os.path.basename(root):
                    df = df.rename(columns={
                                            'program_id':'ProjectID',
                                            'uid':'PersonalID',
                                            'enrollment_date':'EntryDate',
                                            'exit_date':'ExitDate',
                                            'dest':'Destination',
                                            'first_name':'FirstName',
                                            'middle_name':'MiddleName',
                                            'last_name':'LastName',
                                            'person_common_fields__name_quality':'NameDataQuality',
                                            'person_common_fields__relationship_hoh':'RelationshipToHoH',
                                            'person_common_fields__ssn':'SSN',
                                            'person_common_fields__ssn_quality':'SSNDataQuality',
                                            'person_common_fields__dob':'DOB',
                                            'person_common_fields__dob_quality':'DOBDataQuality',
                                            'person_common_fields__veteran_status':'VeteranStatus',
                                            })
                    
                    df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['EntryDate'] = pd.to_datetime(df['EntryDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['ExitDate'] = pd.to_datetime(df['ExitDate'], errors='coerce').dt.strftime('%Y-%m-%d')
                    
                    df['ProjectID'] = 'VC' + df['ProjectID'].astype(str)
                    df['PersonalID'] = 'VC' + df['PersonalID'].astype(str).str[:8]
                    
                    df['EnrollmentID'] = 'E' + df['PersonalID'].astype(str).str[:8]
                    df['HouseholdID'] = 'H' + df['PersonalID'].astype(str).str[:8]
                    df['ExitID'] = 'EX' + df['PersonalID'].astype(str).str[:8]
                    
                    df['NameDataQuality'] = df['NameDataQuality'].astype(str).apply(lambda x: 1 if 'Full' in x else (2 if 'Approximate' in x else 99))
                    df['SSNDataQuality'] = df['SSNDataQuality'].astype(str).apply(lambda x: 1 if 'Full' in x else (2 if 'Approximate' in x else 99))
                    df['DOBDataQuality'] = df['DOBDataQuality'].astype(str).apply(lambda x: 1 if 'Full' in x else (2 if 'Approximate' in x else 99))

                    df['person_common_fields__race'].fillna('', inplace=True)
                    df['person_common_fields__ethnicity'].fillna('', inplace=True)
                    df['AmIndAKNative'] = df['person_common_fields__race'].str.contains('American Indian').astype(int)
                    df['Asian'] = df['person_common_fields__race'].str.contains('Asian').astype(int)
                    df['BlackAfAmerican'] = df['person_common_fields__race'].str.contains('Black').astype(int)
                    df['HispanicLatinaeo'] = df['person_common_fields__ethnicity'].str.contains('Hispanic/Latino').astype(int)
                    df['MidEastNAfrican'] = df['person_common_fields__race'].str.contains('Middle East').astype(int)
                    df['NativeHIPacific'] = df['person_common_fields__race'].str.contains('Pacific').astype(int)
                    df['White'] = df['person_common_fields__race'].str.contains('White').astype(int)
                    
                    df['person_common_fields__gender_identity'].fillna('', inplace=True)
                    df['Woman'] = df['person_common_fields__gender_identity'].str.contains('Female').astype(int)
                    df['Man'] = df['person_common_fields__gender_identity'].str.contains('Male').astype(int)
                    df['NonBinary'] = df['person_common_fields__gender_identity'].str.contains('Gender Non-Conforming').astype(int)
                    df['CulturallySpecific'] = df['person_common_fields__gender_identity'].str.contains('Culturally Specific').astype(int)
                    df['Transgender'] = df['person_common_fields__gender_identity'].str.contains('Trans').astype(int)
                    df['Questioning'] = df['person_common_fields__gender_identity'].str.contains('Questioning').astype(int)
                    df['DifferentIdentity'] = 0
                    
                    df['VeteranStatus'] = df['VeteranStatus'].astype(str).apply(lambda x: 1 if 'Yes' in x else (0 if 'No' in x else 99))
                    
                    yes = df['person_common_fields__hoh'] == 'Yes'
                    child = (df['person_common_fields__hoh'] != 'Yes') & (df['RelationshipToHoH'].str.contains('child', case=False, na=False))
                    spouse = (df['person_common_fields__hoh'] != 'Yes') & (df['RelationshipToHoH'].str.contains('spouse', case=False, na=False))
                    other_relation = (df['person_common_fields__hoh'] != 'Yes') & (df['RelationshipToHoH'].str.contains('Other relation', case=False, na=False))
                    non_relation = (df['person_common_fields__hoh'] != 'Yes') & (df['RelationshipToHoH'].str.contains('Other: Non-Relation', case=False, na=False))

                    df.loc[yes, 'RelationshipToHoH'] = 1
                    df.loc[child, 'RelationshipToHoH'] = 2
                    df.loc[spouse, 'RelationshipToHoH'] = 3
                    df.loc[other_relation, 'RelationshipToHoH'] = 4
                    df.loc[non_relation, 'RelationshipToHoH'] = 5

                    df['RelationshipToHoH'].fillna(99, inplace=True)

                    df['Destination'] = df['Destination'].fillna('')

                    destination_mapping = {
                        'habitation':116,
                        'Emergency shelter':101,
                        'Safe Haven':118,
                        'foster care':215,
                        'non-psychiatric':206,
                        'Jail':207,
                        'Long-term':225,
                        'Psychiatric hospital':204,
                        'Substance':205,
                        'Transitional':302,
                        'halfway':329,
                        'without emergency':118,
                        'Host Home':332,
                        'family, temporary tenure':312,
                        'friends, temporary tenure':313,
                        'HOPWA TH':327,
                        'friend’s room':336,
                        'family member’s room':335,
                        'family, permanent':422,
                        'friends, permanent':433,
                        'HOPWA PH':426,
                        'Rental by client, no':410,
                        'Rental by client, with':435,
                        'Owned by client, no':411,
                        'Owned by client, with':421,
                        'No exit':30,
                        'Deceased':24,
                        'unable to determine':37,
                        'doesn\'t know':8,
                        'prefers not to answer':9,
                        'refused':9,
                        'not collected':99,
                        'GPD':435,
                        'VASH':435,
                        'RRH':435,
                        'HCV':435,
                        'Public housing':435,
                        'other ongoing':435,
                        'Emergency Housing':435,
                        'Unification':435,
                        'Fost Youth to Independence':435,
                        'Permanent Supportive':435,
                        'Other permanent housing':435
                    }

                    df['Destination'] = df['Destination'].apply(lambda x: next((code for word, code in destination_mapping.items() if word in x.lower()), 99))
                    
                    
                    client_data = df[['PersonalID', 'FirstName', 'MiddleName', 'LastName','NameDataQuality','SSN', 'SSNDataQuality', 'DOB', 'DOBDataQuality', 'AmIndAKNative', 'Asian','BlackAfAmerican','HispanicLatinaeo','MidEastNAfrican','NativeHIPacific','White','Woman','Man','NonBinary','CulturallySpecific','Transgender','Questioning','DifferentIdentity','VeteranStatus']]
                    enrollment_data = df[['EnrollmentID','ProjectID','PersonalID','EntryDate','HouseholdID','RelationshipToHoH']]                   
                    exit_data = df[['ExitID','ExitDate','EnrollmentID','PersonalID','Destination']].dropna()

                    client_df = pd.concat([client_df,client_data],ignore_index=True)
                    enrollment_df = pd.concat([enrollment_df,enrollment_data],ignore_index=True)
                    exit_df = pd.concat([exit_df,exit_data],ignore_index=True)

                 
    conn = sqlite3.connect(merged_database)
    
    query = '''
    SELECT SUBSTR(MergedProgramID, INSTR(MergedProgramID, '|') + 1) AS ProjectID, Region
    FROM PATHProgramMasterList
    '''

    region_df = pd.read_sql_query(query, con=conn)

    enrollment_df = pd.merge(enrollment_df, region_df, on='ProjectID', how='left')
    exit_df = exit_df.merge(enrollment_df[['EnrollmentID','Region']])
    client_df = client_df.merge(enrollment_df[['PersonalID','Region']])

    for column in enrollment_df.columns:
        if 'ID' in column:
            enrollment_df.loc[enrollment_df['Region'] == 'Los Angeles County', column] = "LA" + "|" + enrollment_df[column].astype(str)
            enrollment_df.loc[enrollment_df['Region'] == 'San Diego County', column] = "SD" + "|" + enrollment_df[column].astype(str)
            enrollment_df.loc[enrollment_df['Region'] == 'Santa Barbara County', column] = "SB" + "|" + enrollment_df[column].astype(str)
            enrollment_df.loc[enrollment_df['Region'] == 'Orange County', column] = "OC" + "|" + enrollment_df[column].astype(str)
            enrollment_df.loc[enrollment_df['Region'] == 'Santa Clara County', column] = "SCC" + "|" + enrollment_df[column].astype(str)

    for column in exit_df.columns:
        if 'ID' in column:
            exit_df.loc[exit_df['Region'] == 'Los Angeles County', column] = "LA" + "|" + exit_df[column].astype(str)
            exit_df.loc[exit_df['Region'] == 'San Diego County', column] = "SD" + "|" + exit_df[column].astype(str)
            exit_df.loc[exit_df['Region'] == 'Santa Barbara County', column] = "SB" + "|" + exit_df[column].astype(str)
            exit_df.loc[exit_df['Region'] == 'Orange County', column] = "OC" + "|" + exit_df[column].astype(str)
            exit_df.loc[exit_df['Region'] == 'Santa Clara County', column] = "SCC" + "|" + exit_df[column].astype(str)

    for column in client_df.columns:
        if 'ID' in column:
            client_df.loc[client_df['Region'] == 'Los Angeles County', column] = "LA" + "|" + client_df[column].astype(str)
            client_df.loc[client_df['Region'] == 'San Diego County', column] = "SD" + "|" + client_df[column].astype(str)
            client_df.loc[client_df['Region'] == 'Santa Barbara County', column] = "SB" + "|" + client_df[column].astype(str)
            client_df.loc[client_df['Region'] == 'Orange County', column] = "OC" + "|" + client_df[column].astype(str)
            client_df.loc[client_df['Region'] == 'Santa Clara County', column] = "SCC" + "|" + client_df[column].astype(str)

    enrollment_df.drop(columns=['Region'], inplace=True)
    exit_df.drop(columns=['Region'], inplace=True)
    client_df.drop(columns=['Region'], inplace=True)

    enrollment_df.drop_duplicates(inplace=True)
    exit_df.drop_duplicates(inplace=True)
    client_df.drop_duplicates(inplace=True)

    enrollment_df.to_csv(os.path.join(non_hmis_folder_location, 'Enrollment.csv'), index=False, mode='a', header=not os.path.isfile(os.path.join(non_hmis_folder_location, 'Enrollment.csv')))
    exit_df.to_csv(os.path.join(non_hmis_folder_location, 'Exit.csv'), index=False, mode='a', header=not os.path.isfile(os.path.join(non_hmis_folder_location, 'Exit.csv')))
    client_df.to_csv(os.path.join(non_hmis_folder_location, 'Client.csv'), index=False, mode='a', header=not os.path.isfile(os.path.join(non_hmis_folder_location, 'Client.csv')))

    cursor = conn.cursor()
    print(non_hmis_folder_location,merged_database)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        csv_filename = os.path.join(non_hmis_folder_location.removesuffix('.db'), table_name + ".csv")
        
        if os.path.exists(csv_filename):
            print("Loading Data to table", merged_database, table_name)
            df = pd.read_csv(csv_filename, low_memory=False)
            df.to_sql(table_name, conn, if_exists='append', index=False)

    conn.commit()
    conn.close()  

def populate_additional_information(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Select all columns from the Enrollment table
    cursor.execute('SELECT EnrollmentID, PersonalID, HouseholdID FROM Enrollment;')
    enrollment_data = cursor.fetchall()

    # Insert or replace into AdditionalInformation table
    for enrollment_id, personal_id, household_id in enrollment_data:
        cursor.execute('''
            INSERT OR REPLACE INTO AdditionalInformation (EnrollmentID, PersonalID, HouseholdID)
            VALUES (?, ?, ?)
        ''', (enrollment_id, personal_id, household_id))  # Set ChronicallyHomeless to True for each ID
        print(f"Insert Information for EnrollmentID:  {enrollment_id}")

    conn.commit()
    conn.close() 

def ch_fill_in(db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    base_query = '''
SELECT DISTINCT
    Enrollment.PersonalID,Enrollment.EnrollmentID
FROM Enrollment
LEFT JOIN Exit ON Enrollment.EnrollmentID = Exit.EnrollmentID
INNER JOIN PATHProgramMasterList ON Enrollment.ProjectID = PATHProgramMasterList.MergedProgramID
LEFT JOIN Project ON Enrollment.ProjectID = Project.ProjectID
LEFT JOIN Client ON Enrollment.PersonalID = Client.PersonalID
WHERE (
        (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>=6750) AND
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            Project.ProjectType IN (0, 1, 4, 8) AND 
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) >= 365)
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>=6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            Project.ProjectType IN (0, 1, 4, 8) AND 
            Enrollment.DateToStreetESSH IS NULL AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            Project.ProjectType IN (0, 1, 4, 8) AND 
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) < 365) AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            (Enrollment.LivingSituation BETWEEN 100 AND 199) AND 
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) >= 365)
        )
        OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            (Enrollment.LivingSituation BETWEEN 100 AND 199) AND 
            Enrollment.DateToStreetESSH IS NULL AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>=6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            (Enrollment.LivingSituation BETWEEN 100 AND 199) AND 
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) < 365) AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>=6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            (Enrollment.LivingSituation BETWEEN 200 AND 299) AND 
            Enrollment.LOSUnderThreshold = 1 AND
            Enrollment.PreviousStreetESSH = 1 AND
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) >= 365)
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            (Enrollment.LivingSituation BETWEEN 200 AND 299) AND 
            Enrollment.LOSUnderThreshold = 1 AND
            Enrollment.PreviousStreetESSH = 1 AND
            Enrollment.DateToStreetESSH IS NULL AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        ) OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            (Enrollment.LivingSituation BETWEEN 200 AND 299) AND 
            Enrollment.LOSUnderThreshold = 1 AND
            Enrollment.PreviousStreetESSH = 1 AND
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) < 365) AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        )OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            ((Enrollment.LivingSituation BETWEEN 300 AND 499) OR (Enrollment.LivingSituation BETWEEN 0 AND 99))AND 
            Enrollment.LOSUnderThreshold = 1 AND
            Enrollment.PreviousStreetESSH = 1 AND
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) >= 365)
        )OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            ((Enrollment.LivingSituation BETWEEN 300 AND 499) OR (Enrollment.LivingSituation BETWEEN 0 AND 99))AND 
            Enrollment.LOSUnderThreshold = 1 AND
            Enrollment.PreviousStreetESSH = 1 AND
            Enrollment.DateToStreetESSH IS NULL AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        )OR (
            (Enrollment.RelationshipToHoH = 1 OR julianday(Enrollment.EntryDate)-julianday(Client.DOB)>= 6750) AND 
            Enrollment.DisablingCondition NOT IN (0, 8, 9, 99) AND 
            ((Enrollment.LivingSituation BETWEEN 300 AND 499) OR (Enrollment.LivingSituation BETWEEN 0 AND 99))AND 
            Enrollment.LOSUnderThreshold = 1 AND
            Enrollment.PreviousStreetESSH = 1 AND
            (julianday(Enrollment.EntryDate) - julianday(Enrollment.DateToStreetESSH) < 365) AND 
            Enrollment.TimesHomelessPastThreeYears = 4 AND 
            Enrollment.MonthsHomelessPastThreeYears >= 112
        )
    )
'''
    filter_params=[]

    c.execute(base_query, filter_params)
    results = c.fetchall()
    amount = len(results)
    
    for personal_id, enrollment_id in results:
        # Update ChronicallyHomeless to 1 for the existing (EnrollmentID, PersonalID) pair
        c.execute('''
            UPDATE AdditionalInformation
            SET ChronicallyHomeless = 1
            WHERE EnrollmentID = ? AND PersonalID = ?
        ''', (enrollment_id, personal_id))
        print(f"Updating CH Status for Personal ID:{personal_id}")

        if c.rowcount == 0:
            print(f"No existing record found for: {personal_id}")

    conn.commit()
    conn.close()

    return results,amount

def apply_chronically_homeless_to_household(db_name='merged_hmis2024.db'):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # Get all chronically homeless PersonalIDs with HouseholdID
    c.execute('''
        SELECT PersonalID, HouseholdID
        FROM AdditionalInformation
        WHERE ChronicallyHomeless = 1 AND HouseholdID IS NOT NULL
    ''')
    chronically_homeless_data = c.fetchall()

    for personal_id, household_id in chronically_homeless_data:
        # Update ChronicallyHomeless status for others in the same HouseholdID
        c.execute('''
            UPDATE AdditionalInformation
            SET ChronicallyHomeless = 1
            WHERE HouseholdID = ? AND PersonalID != ?
        ''', (household_id, personal_id))
        print(f"Updating HouseholdID for PersonalID:  {personal_id}")

    conn.commit()
    conn.close()
    
def import_glossary(filename):

    glossary_dict = {}
    with open(filename, 'r') as f:
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            Category = row['Category']
            Name = row['Name']
            Definition = row['Definition']

            if Category in glossary_dict:
                glossary_dict[Category][Name] = Definition
            else:
                glossary_dict[Category] = {Name: Definition}
            
    return glossary_dict
def drop_non_PATH_programs(database_name):
    conn = sqlite3.connect(database_name)
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()
    print(f"Begining to delete non-PATH programs.")

    for table in tables:
        table_name = table[0]
        print(f"\tStarting: {table_name}")

        c.execute(f"PRAGMA table_info({table_name})")
        columns = c.fetchall()
        column_names = [column[1] for column in columns]

        if 'EnrollmentID' in column_names and table_name != 'Enrollment':
            c.execute(f'''
                DELETE FROM {table_name}
                WHERE EnrollmentID NOT IN (
                    SELECT EnrollmentID
                    FROM Enrollment
                        WHERE ProjectID IN (
                            SELECT MergedProgramID
                            FROM PATHProgramMasterList
                        )
                    )
            ''')   
        elif table_name == 'Client':
            c.execute(f'''
                DELETE FROM {table_name}
                WHERE PersonalID NOT IN (
                    SELECT PersonalID
                    FROM Client
                    WHERE PersonalID IN (
                        SELECT PersonalID
                        FROM Enrollment
                        WHERE ProjectID IN (
                            SELECT MergedProgramID
                            FROM PATHProgramMasterList
                        )
                    )
                )            
            ''')
        elif 'CustomClientServiceID' in column_names:
            c.execute(f'''
                DELETE FROM {table_name}
                WHERE CustomClientServiceId NOT IN (
                    SELECT CustomClientServiceId
                    FROM CustomClientServices
                    WHERE ClientProgramId IN(
                        SELECT ClientProgramId AS EnrollmentID
                        FROM CustomClientServices
                        WHERE EnrollmentID IN (
                            SELECT EnrollmentID 
                            FROM Enrollment
                            WHERE ProjectID IN (
                                SELECT MergedProgramID
                                FROM PATHProgramMasterList
                            )
                        )
                    )
                )
            ''')
        elif table_name == 'CustomClientServices':
            c.execute(f'''
                DELETE FROM {table_name}
                WHERE ClientProgramId NOT IN(
                    SELECT ClientProgramId AS EnrollmentID
                    FROM CustomClientServices
                    WHERE EnrollmentID IN (
                        SELECT EnrollmentID 
                        FROM Enrollment
                        WHERE ProjectID IN (
                            SELECT MergedProgramID
                            FROM PATHProgramMasterList
                        )
                    )
                )
            ''')
        else:
            continue
   
    c.execute(f'''
        DELETE FROM Enrollment
        WHERE EnrollmentID NOT IN (
            SELECT EnrollmentID
            FROM Enrollment
                WHERE ProjectID IN (
                    SELECT MergedProgramID
                    FROM PATHProgramMasterList
                )
            )
        ''')
    
    c.execute(f'''
        DELETE FROM Project
        WHERE ProjectID NOT IN (
            SELECT MergedProgramID
            FROM PATHProgramMasterList
            )        
        ''')
    

    conn.commit()
    conn.execute("VACUUM")

    conn.close()

    
    print(f"Finished deleting non-PATH programs.")
    