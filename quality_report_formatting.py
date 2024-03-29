import os

from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER, TA_JUSTIFY
from reportlab.lib.colors import Color, gray, lightgrey, black, white,red,green
from reportlab.lib.pagesizes import inch
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import TableStyle


import Data_Report as report


# Register Fonts
pdfmetrics.registerFont(TTFont('Montserrat', os.path.join(report.font_directory, 'Montserrat-Regular.ttf')))
pdfmetrics.registerFont(TTFont('MontserratSemiBold', os.path.join(report.font_directory, 'Montserrat-SemiBold.ttf')))
pdfmetrics.registerFont(TTFont('OpenSans', os.path.join(report.font_directory, 'OpenSans-Regular.ttf')))
pdfmetrics.registerFont(TTFont('OpenSansBold', os.path.join(report.font_directory, 'OpenSans-Bold.ttf')))
pdfmetrics.registerFont(TTFont('OpenSansItalic', os.path.join(report.font_directory, 'OpenSans-Italic.ttf')))


# Color definitions
PATHBlue = Color((0.0/255), (174.0/255), (239.0/255), 1)
PATHLightBlue = Color((178/255), (231/255), (250.0/255), 1)
PATHBabyBlue = Color((178/255), (231/255), (250.0/255), 0.5)
PATHPurple = Color((138.0/255), (40.0/255), (143.0/255), 1)
PATHLightPurple = Color((220/255), (190/255), (221/255), 1)
PATHRed = Color((237/255), (28/255), (36/255), 1)
PATHLightRed = Color((250/255), (186/255), (189/255), 1)
PATHGreen = Color((34.0/255), (178/255), (76/255), 1)
PATHLightGreen = Color((188.0/255), (232.0/255), (201.0/255), 1)
babygrey='#E5E4E2'

def cellColor(value):
    numeric_accuracy = float(value.strip('%')) / 100.0  # Convert to a float between 0 and 1
    if numeric_accuracy == 1.0:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor='#73FA0F')
    elif 1.0 >numeric_accuracy>=0.95:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor='#D0FB0F')
    elif 0.95 >numeric_accuracy>=0.90:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor='#F9F908')
    elif 0.90 >numeric_accuracy>=0.80:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor='#FBBC01')
    else:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor='#FE1306')

def textColor(value,threshold):
    numeric_accuracy = float(value.strip('%')) / 100.0  # Convert to a float between 0 and 1
    if numeric_accuracy>=threshold:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,textColor='#0EBA26')
    elif numeric_accuracy <threshold:
        return ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,textColor='#FE1306')



# Paragraph Styles and Other Text Formatting
titlePageStyle = ParagraphStyle('Title Page',fontName="Montserrat",fontSize=40, leading=35, alignment=TA_LEFT, justifyLastLine=1)
titlesubPageStyle = ParagraphStyle('Title Page',fontName="MontserratSemiBold",fontSize=80, leading=35, alignment=TA_LEFT)
titlePageSubHeaderStyle = ParagraphStyle('Title Page Sub',fontName="OpenSans",fontSize=13, leading=12, alignment=TA_LEFT)
pageHeaderStyle = ParagraphStyle('Page Header', fontName="MontserratSemiBold", fontSize=18, leading=20, alignment=TA_CENTER, textColor=black)
pageHeaderStylesmall = ParagraphStyle('Page Header', fontName="MontserratSemiBold", fontSize=14, leading=20, alignment=TA_CENTER, textColor=black)
pageHeaderStyle2 = ParagraphStyle('Page Header', fontName="Montserrat", fontSize=18, leading=20, alignment=TA_CENTER, textColor=black)
pageHeaderStyle3 = ParagraphStyle('Page Header', fontName="MontserratSemiBold", fontSize=18, leading=20, alignment=TA_CENTER, textColor=PATHPurple)
pageHeaderStyle4 = ParagraphStyle('Page Header', fontName="MontserratSemiBold", fontSize=18, leading=20, alignment=TA_CENTER, textColor=PATHGreen)
pageHeaderStyle5 = ParagraphStyle('Page Header', fontName="MontserratSemiBold", fontSize=18, leading=20, alignment=TA_CENTER, textColor=PATHRed)
pageSubHeaderStyle = ParagraphStyle('Page Sub-Header', fontName="MontserratSemiBold", fontSize=20, leading=20, alignment=TA_CENTER, textColor=PATHPurple)
sectionHeaderStyle = ParagraphStyle('Section Header', fontName="MontserratSemiBold", fontSize=16, leading=24, alignment=TA_LEFT, borderWidth=3)
subSectionHeaderStyle = ParagraphStyle('Sub-Section Section Header', fontName="Montserrat", fontSize=14, alignment=TA_CENTER, borderWidth=3)
programHeaderStyle = ParagraphStyle('Program Header', fontName="MontserratSemiBold", fontSize=12, alignment=TA_LEFT, borderWidth=3, leftIndent=8, textColor=PATHGreen)
tableHeaderStyle = ParagraphStyle('Table Header', fontName="Montserrat", fontSize=10, leading=15, alignment=TA_LEFT, borderWidth=0, textColor=PATHPurple)
tableSubHeader = ParagraphStyle('Table Text', fontName="MontserratSemiBold", fontSize=8, leading=12, alignment=TA_LEFT, leftIndent=8)
tableSecondaryHeader = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_CENTER)
tableSecondaryHeaderPurple = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER,textColor=PATHPurple)
tableSecondaryHeaderLeftBlue = ParagraphStyle('Table Text', fontName="OpenSansItalic", fontSize=8, leading=12, alignment=TA_LEFT,textColor=PATHBlue)
tableSecondaryHeaderLeftPurple = ParagraphStyle('Table Text', fontName="OpenSansItalic", fontSize=8, leading=12, alignment=TA_LEFT,textColor=PATHPurple)
tableSecondaryHeaderLeftGreen = ParagraphStyle('Table Text', fontName="OpenSansItalic", fontSize=8, leading=12, alignment=TA_LEFT,textColor=PATHGreen)
tableSecondaryHeaderLeftRed = ParagraphStyle('Table Text', fontName="OpenSansItalic", fontSize=8, leading=12, alignment=TA_LEFT,textColor=PATHRed)
tableSecondaryHeader = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER)
tableSecondaryHeader2 = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER,textColor=PATHGreen)
tableSecondaryHeader3 = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER,textColor=PATHPurple)
tableSecondaryHeader4 = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER,textColor=PATHRed)
tableSecondaryHeader5 = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER,textColor=PATHBlue)
tableTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_LEFT, borderWidth=4)
tableTextStyleCenter = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4)
tableValuesStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4)
tableValuesStyleBig = ParagraphStyle('Table Values', fontName="OpenSans", fontSize=10, leading=12, alignment=TA_CENTER, borderWidth=4)
tableValuesStylegray = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor=lightgrey)
tableTargetStyle = ParagraphStyle('Table Values', fontName="OpenSansItalic", fontSize=8, alignment=TA_CENTER, borderWidth=0)
tableCadenceStyle = ParagraphStyle('Table Values', fontName="OpenSans", fontSize=8, alignment=TA_CENTER, borderWidth=0)
tableFooterStyle = ParagraphStyle('Table Footer', fontName="OpenSansItalic", fontSize=6, leading=8, alignment=TA_JUSTIFY, borderWidth=0)
tableValuesExceedStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, alignment=TA_CENTER, borderWidth=0, textColor=PATHGreen)
tableValuesMissStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, alignment=TA_CENTER, borderWidth=0, textColor=PATHRed)
TOCSectionStyle = ParagraphStyle('TOCSectionStyle', fontName="Montserrat", fontSize=11, alignment=TA_LEFT)
TOCSubSectionStyle = ParagraphStyle('TOCSubSectionStyle', fontName="Montserrat", fontSize=9, alignment=TA_LEFT, leftIndent=8)
TOCHeaderStyle = ParagraphStyle('TOCSectionStyle', fontName="Montserrat", fontSize=24, alignment=TA_CENTER)
programsummarytitlePageStyle = ParagraphStyle('programsummarytitlePageStyle',fontName="Montserrat",fontSize=40, leading=35, alignment=TA_LEFT, justifyLastLine=1)
noteStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=6, leading=12, alignment=TA_RIGHT)
noteStyle2 = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=12, alignment=TA_LEFT)
ComingSoonStyle = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=10, alignment=TA_CENTER, borderWidth=4)
tableTextStyleCenterRed = ParagraphStyle('Table Text', fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,textColor=red)
tableTextStyleCenterGreen = ParagraphStyle('Table Text', fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,textColor=green)
tableValuesStyleRed = ParagraphStyle('Table Values', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER, borderWidth=4,backColor=red)



# Agency Indicator by Dept Table
IndicatorByDeptIndicatorNameStyle = ParagraphStyle('Table Header', fontName="MontserratSemiBold", fontSize=9, leading=9, alignment=TA_LEFT, borderWidth=0)


# Agency Indicator Styles
IndicTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=8, alignment=TA_CENTER, borderWidth=4)
IndicNameTextStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=8, leading=8, alignment=TA_LEFT, borderWidth=4, leftIndent=4)
IndicProgTypeStyle = ParagraphStyle('Table Header', fontName="Montserrat", fontSize=10, leading=15, alignment=TA_LEFT, borderWidth=0, textColor=PATHPurple)
IndicSecondaryHeader = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=9, leading=12, alignment=TA_CENTER)
IndicResult = ParagraphStyle('Table Text', fontName="OpenSansBold", fontSize=8, leading=12, alignment=TA_CENTER)

# Glossary Styles
GlossaryNameStyle = ParagraphStyle('Table Text', fontName="MontserratSemiBold", fontSize=9, leading=12, alignment=TA_RIGHT, borderWidth=4)
GlossaryDefinitionStyle = ParagraphStyle('Table Text', fontName="OpenSans", fontSize=9, leading=12, alignment=TA_JUSTIFY, borderWidth=4)

# Reformat Agency Indicator by Dept Function Names - allows for wordwrap
styles = getSampleStyleSheet()
styles['Normal'].fontName = 'OpenSans'
styles['Normal'].fontSize = 9
functionNameStyle = styles['Normal']
functionNameStyle.wordWrap = 'CJK'


# Table Styles
AgencyIndicatorTableStructureStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), lightgrey),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 
    ('LINEABOVE', (0, 2), (-1, 2), 1, black),
    ('LINEBELOW', (0, -1), (-1, -1), 1, black),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [lightgrey,white]),

])

AgencyIndicatorHeaderStyle = TableStyle([
    ('BACKGROUND', (0,0), (-1,0), lightgrey),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0), 

])



demosTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), lightgrey),
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, gray),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),
    ('LINEBELOW', (0, 8), (-1, 8), .2, gray),
    ('LINEBELOW', (0, 9), (-1, 9), .2, gray),
    ('LINEBELOW', (0, 10), (-1, 10), .2, gray),
    ('LINEBELOW', (0, 11), (-1, 11), .2, gray),
    ('LINEBELOW', (0, 12), (-1, 12), .2, gray),
    ('LINEBELOW', (0, 13), (-1, 13), .2, gray),
    ('LINEBELOW', (0, 14), (-1, 14), .2, gray),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white,white]),
])

ptTableStyle = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), white),
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, gray),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [babygrey,white]),
])

demosTableStylewhite = TableStyle([
    ('BACKGROUND', (0, 0), (-1, -1), white),
    ('LINEBELOW', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('LINEBELOW', (0, 1), (-1, 1), .2, gray),
    ('LINEBELOW', (0, 2), (-1, 2), .2, gray),
    ('LINEBELOW', (0, 3), (-1, 3), .2, gray),
    ('LINEBELOW', (0, 4), (-1, 4), .2, gray),
    ('LINEBELOW', (0, 5), (-1, 5), .2, gray),
    ('LINEBELOW', (0, 6), (-1, 6), .2, gray),
    ('LINEBELOW', (0, 7), (-1, 7), .2, gray),
    ('LINEBELOW', (0, 8), (-1, 8), .2, gray),
    ('LINEBELOW', (0, 9), (-1, 9), .2, gray),
    ('LINEBELOW', (0, 10), (-1, 10), .2, gray),
    ('LINEBELOW', (0, 11), (-1, 11), .2, gray),
    ('LINEBELOW', (0, 12), (-1, 12), .2, gray),
    ('LINEBELOW', (0, 13), (-1, 13), .2, gray),
    ('LINEBELOW', (0, 14), (-1, 14), .2, gray),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white,white]),
])


demoPageTableStyle = TableStyle([
    
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ])


progInfoTableStyle = TableStyle([
    ('LINEABOVE', (0, 0), (-1, 0), .5, black),
    ('FONT', (0, 0), (0, 0), 'OpenSans', 7),
    ('FONT', (1, 0), (1, 0), 'OpenSansBold', 6),
    ('FONT', (2, 0), (2, 0), 'OpenSans', 7),
    ('FONT', (3, 0), (3, 0), 'OpenSansBold', 6),
    ('FONT', (4, 0), (4, 0), 'OpenSans', 7),
    ('FONT', (5, 0), (5, 0), 'OpenSansBold', 6),
    ('VALIGN', (0, 0), (-1, -1), "MIDDLE"),
    ('LINEBELOW', (0, -1), (-1, -1), .5, black),
    ('COLBACKGROUNDS', (0, 0), (-1, -1), [white, lightgrey]),
    ('ALIGN', (0, 0), (0, 0), "CENTER"),
    ('ALIGN', (1, 0), (1, 0), "LEFT"),
    ('ALIGN', (2, 0), (2, 0), "CENTER"),
    ('ALIGN', (3, 0), (3, 0), "CENTER"),
    ('ALIGN', (4, 0), (4, 0), "CENTER"),
    ('ALIGN', (5, 0), (5, 0), "CENTER"),

    
])


ProgTypeIndicatorsAlignmentTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('TOPPADDING', (0, 0), (-1, -1), 0),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
])

GlossaryTableStyle = TableStyle([
    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ('LINEABOVE', (0, 0), (-1, 0), .8, black),
    ('LINEBELOW', (0, -1), (-1, -1), .8, black),
    ('LINEABOVE', (0, 1), (-1, 1), .2, gray),
    ('LINEABOVE', (0, 2), (-1, 2), .2, gray),
    ('LINEABOVE', (0, 3), (-1, 3), .2, gray),
    ('LINEABOVE', (0, 4), (-1, 4), .2, gray),
    ('LINEABOVE', (0, 5), (-1, 5), .2, gray),
    
])

# Table Column and Row Formatting
agencyIndicatorsByDeptRegionColWidths = [inch*1.5, inch*.4, inch*.4, inch*.4, inch*.4, inch*.4, inch*.4]
agencyIndicatorsByDeptDeptColWidths = [inch*.4, inch*.4, inch*.4, inch*.4, inch*.4, inch*.4]
agencyIndicatorsByDeptAllRowHeights = inch/2.75
walkerGridColWidths = [1.8*inch,1.8*inch,1.8*inch,1.8*inch]
allIndicatorsRow1Height = [inch/4,inch/4,inch/4,inch/4,inch/4,inch/4,inch/4,inch/4]
allIndicatorsRow2Height = [inch/4,inch/4,inch/4,inch/4,inch/4,None]

progIndicatorsColWidths = [2.5*inch, .75*inch, .75*inch]

def ProgTypeIndicatorsRowHeights(rows):
    row_heights = []
    for r in range(rows):
        row_heights.append(inch/4)
    return row_heights

def ProgramLevelProgTypeIndicatorsRowWidth(num_of_indicators):
    row_heights =[]
    for indic in range(num_of_indicators+1):
        row_heights.append(18)
    return row_heights


def colummWidths(PrimaryDataSystem):
    if PrimaryDataSystem == "Vertical Change":
        return [inch,3*inch, inch, inch, inch,.5*inch]
    else:
        return [inch,3.5*inch, inch,.5*inch, inch,.5*inch]
