import xml.etree.ElementTree as ET
import os
import pandas as pd


# list all subdirectories in the "fundamentals" directory
companies = [ name for name in os.listdir('fundamentals') if os.path.isdir(os.path.join('fundamentals', name)) ]

def process_ReportsFinStatements(xml_file):
    return
    tree = ET.parse(xml_file)
    root = tree.getroot()

    for child in root:
        print(child.tag, child.attrib)



def process_RESC(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    # RESC has several sources of information:

    # 1. Security Info
    # 2. Company profile
    # 3. Periods
    # 4. Actuals
    # 5. Consensus estimates (fiscal year; net profit)


    """
    2. Process company profile
    This should in the future done reversely: for all companies, get the company profile and store it in a single dataframe
    Currently however, it's done for a single company.
    """
    def process_company_profile(xml_root):
        company_el = xml_root.find('Company')
        column_mapping_companyInfo = {
            'values': {
                'name': 'CoName/Name',
                'RepNo': "CoIds/CoId[@type='RepNo']",
                'IssueID': "CoIds/CoId[@type='IssueID']",
                'IsPrimaryIssue': "CoIds/CoId[@type='IsPrimaryIssue']",
                'sectorName': 'CompanyInfo/Sector',
                'primaryConsensus': "CompanyInfo/Primary[@type='Consensus']",
                'primaryEstimate': "CompanyInfo/Primary[@type='Estimate']",
                'Currency': "CompanyInfo/Currency",
            }, 
            'attributes': {
                'sectorCode': ('CompanyInfo/Sector', 'code'),
                'sectorSet': ('CompanyInfo/Sector', 'set'),
                'curFiscalPeriod_fyear': ('CompanyInfo/CurFiscalPeriod', 'fYear'),
                'curFiscalPeriod_fyem': ('CompanyInfo/CurFiscalPeriod', 'fyem'),
                'curFiscalPeriod_periodType': ('CompanyInfo/CurFiscalPeriod', 'periodType')
            },
        }
        df = pd.DataFrame(columns=list(column_mapping_companyInfo['values'].keys()).append(column_mapping_companyInfo['attributes'].keys()))

        df_loc = len(df)

        for key, value in column_mapping_companyInfo['values'].items():
            df.loc[df_loc, key] = company_el.find(value).text
            
        for key, (value, attribute) in column_mapping_companyInfo['attributes'].items():
            df.loc[df_loc, key] = company_el.find(value).attrib[attribute]

        return df
    """
    3a. Process periods (annual)
    """
    def process_periods_annual(tree, ticker):
        company_el = tree.find('Company')
        attrib_list = 'fYear', 'periodLength', 'periodUnit', 'endMonth', 'fyNum'

        df = pd.DataFrame(columns=attrib_list)

        for a in attrib_list:
            df[a] = [ e.attrib[a] for e in company_el.findall('CompanyInfo/CompanyPeriods/Annual') ]

        df['ticker'] = [ticker] * len(df)
        return df

    """
    3b. Process periods (interim)
    """
    def process_periods_interim(tree, ticker):
        company_el = tree.find('Company')
        attrib_list = 'type', 'periodNum', 'periodLength', 'periodUnit', 'endMonth', 'endCalYear'

        df = pd.DataFrame(columns=attrib_list)

        for a in attrib_list:
            df[a] = [ e.attrib[a] for e in company_el.findall('CompanyInfo/CompanyPeriods/Annual/Interim') ]
        df['ticker'] = [ticker] * len(df)
        return df
    
    """
    4a. Process actuals (annual)
    """
    def process_actuals_helper(tree, ticker, periodType):
        actuals = tree.findall(f"Actuals/FYActuals/FYActual")
        actual_mappings = {
            'values': {
                'ActValue': f"FYPeriod[@periodType='{periodType}']/ActValue",
            }, 
            'attributes': {
                'fYear': (f"FYPeriod[@periodType='{periodType}']", 'fYear'),
                'endMonth': (f"FYPeriod[@periodType='{periodType}']", 'endMonth'),
                'endCalYear': (f"FYPeriod[@periodType='{periodType}']", 'endCalYear'),
                'updated': (f"FYPeriod[@periodType='{periodType}']/ActValue", 'updated')
            },
        }
        
        # Create all the columns:
        columns = ['ticker', 'actualType', 'actualUnit'] + list(actual_mappings['values'].keys()) + list(actual_mappings['attributes'].keys())
        column_data = {c: [] for c in columns}

        for fyactual in actuals:
            for key, value in actual_mappings['values'].items():
                column_data[key] += [ e.text for e in fyactual.findall(value)]
                
            for key, (value, attribute) in actual_mappings['attributes'].items():
                column_data[key] += [ e.attrib[attribute] for e in fyactual.findall(value)]

            column_data['ticker'] += [ticker] * (len(column_data[key]) - len(column_data['ticker']))
            column_data['actualType'] += [fyactual.attrib['type']] * (len(column_data[key]) - len(column_data['actualType']))
            column_data['actualUnit'] += [fyactual.attrib['unit']]  * (len(column_data[key]) - len(column_data['actualUnit']))

        # Fill dataframe:
        df = pd.DataFrame(columns=columns)
        for c in columns:
            print("Inserting", c)
            df[c] = column_data[c]
        return df
    
    def process_actuals_annual(xml_root, ticker):
        return process_actuals_helper(xml_root, ticker, 'A')

    """
    4b. Process actuals (interim)
    """
    def process_actuals_interim(xml_root, ticker):
        return process_actuals_helper(xml_root, ticker, 'Q')
    
    """
    5a. Fiscal Year Estimates 
    """
    def process_fiscal_year_estimates_helper(xml_root, ticker, periodType='A'):
        actuals = tree.findall(f"ConsEstimates/FYEstimates/FYEstimate")
        actual_mappings = {
            'values': {
                'high_curr': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='High']/ConsValue[@dateType='CURR']",
                'low_curr': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='Low']/ConsValue[@dateType='CURR']",
                'mean_curr': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='Mean']/ConsValue[@dateType='CURR']",
                'mean_1ma': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='Mean']/ConsValue[@dateType='1MA']",
                'mean_3ma': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='Mean']/ConsValue[@dateType='3MA']",
                'median_curr': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='Median']/ConsValue[@dateType='CURR']",
                'stdev_curr': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='StdDev']/ConsValue[@dateType='CURR']",
                'numberOfEst_curr': f"FYPeriod[@periodType='{periodType}']/ConsEstimate[@type='NumOfEst']/ConsValue[@dateType='CURR']",
            }, 
            'attributes': {
                'fYear': (f"FYPeriod[@periodType='{periodType}']", 'fYear'),
                'endMonth': (f"FYPeriod[@periodType='{periodType}']", 'endMonth'),
                'endCalYear': (f"FYPeriod[@periodType='{periodType}']", 'endCalYear')
            },
        }
        
        # Create all the columns:
        columns = ['ticker', 'type', 'unit'] + list(actual_mappings['values'].keys()) + list(actual_mappings['attributes'].keys())
        column_data = {c: [] for c in columns}

        for fyactual in actuals:
            for key, value in actual_mappings['values'].items():
                column_data[key] += [ e.text for e in fyactual.findall(value)]
                
            for key, (value, attribute) in actual_mappings['attributes'].items():
                column_data[key] += [ e.attrib[attribute] for e in fyactual.findall(value)]

            column_data['ticker'] += [ticker] * (len(column_data[key]) - len(column_data['ticker']))
            column_data['type'] += [fyactual.attrib['type']] * (len(column_data[key]) - len(column_data['type']))
            column_data['unit'] += [fyactual.attrib['unit']]  * (len(column_data[key]) - len(column_data['unit']))

        # Fill dataframe:
        df = pd.DataFrame(columns=columns)
        for c in columns:
            print("Inserting", c)
            df[c] = column_data[c]
        return df

    """
    5b. Net Profit Estimates
    """
    def process_net_profit_estimates(xml_root, ticker):
        pass

def process_ReportSnapshot(xml_file):
    return
    tree = ET.parse(xml_file)
    root = tree.getroot()

    for child in root:
        print(child.tag, child.attrib)


functionmapping = {
    'ReportsFinStatements': process_ReportsFinStatements
}

for comp in companies:
    for report in os.listdir(f'./fundamentals/{comp}'):
        if report.endswith('.xml'):
            functionmapping[report.split('.')[0]](f'./fundamentals/{comp}/{report}')