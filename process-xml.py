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
    # RESC has several sources of information:

    # 1. Security Info
    # 2. Company profile
    # 3. Periods
    # 4. Actuals
    # 5. Consensus estimates (fiscal year; net profit)

    def xml_processor_helper(rootelementpath: str, mappings: dict = {'values': {}, 'attributes': {}}, toplevelattributes: dict = {}, fixed_columns: dict = {}):
        collection = tree.findall(rootelementpath)

        columns = list(mappings['values'].keys()) + list(mappings['attributes'].keys()) + list(toplevelattributes.keys()) + list(fixed_columns.keys())
        column_data = {c: [] for c in columns}

        rowno = 1
        for el in collection:
            for key, value in mappings['values'].items():
                column_data[key] += [ e.text for e in el.findall(value)]
                
            for key, (value, attribute) in mappings['attributes'].items():
                column_data[key] += [ e.attrib[attribute] for e in el.findall(value)]

            # Fill top level attributes for every row that is added:
            for key, value in toplevelattributes.items():
                column_data[key] += [ el.attrib[value] ] * (max([len(a) for a in column_data.values()] + [rowno]) - len(column_data[key]))
            
            # Fill fixed column values for every row that is added:
            for key, value in fixed_columns.items():
                column_data[key] += [ value ] * (max([len(a) for a in column_data.values()] + [rowno]) - len(column_data[key]))
            
            # Insert empty column values for columns that are not filled in this iteration:
            for key in column_data:
                column_data[key] += [None] * (max([len(a) for a in column_data.values()] + [rowno]) - len(column_data[key]))
            rowno += 1

        # Fill dataframe:
        df = pd.DataFrame(columns=columns)
        for c in columns:
            df[c] = column_data[c]
        return df
    
    """
    1. Process security info
    """
    def process_security_info(tree):

        security_mappings = {
            'values': {
                'ISIN': f"SecIds/SecId[@type='ISIN']",
                'RIC': f"SecIds/SecId[@type='RIC']",
                'TICKER': f"SecIds/SecId[@type='TICKER']",
                'InstrumentPI': f"SecIds/SecId[@type='InstrumentPI']",
                'CLPRICE': f"MarketData/MarketDataItem[@type='CLPRICE']",
                'MARKETCAP': f"MarketData/MarketDataItem[@type='CLPRICE']",
                '52WKHIGH': f"MarketData/MarketDataItem[@type='CLPRICE']",
                '52WKLOW': f"MarketData/MarketDataItem[@type='CLPRICE']", 
            }, 
            'attributes': {
                'CLPRICE_Unit': (f"MarketData/MarketDataItem[@type='CLPRICE']", 'unit'),
                'CLPRICE_CurrCode': (f"MarketData/MarketDataItem[@type='CLPRICE']", 'currCode'),
                'MARKETCAP_Unit': (f"MarketData/MarketDataItem[@type='MARKETCAP']", 'unit'),
                'MARKETCAP_CurrCode': (f"MarketData/MarketDataItem[@type='MARKETCAP']", 'currCode'),
                '52WKHIGH_Unit': (f"MarketData/MarketDataItem[@type='52WKHIGH']", 'unit'),
                '52WKHIGH_CurrCode': (f"MarketData/MarketDataItem[@type='52WKHIGH']", 'currCode'),
                '52WKLOW_Unit': (f"MarketData/MarketDataItem[@type='52WKLOW']", 'unit'),
                '52WKLOW_CurrCode': (f"MarketData/MarketDataItem[@type='52WKLOW']", 'currCode'),
            },
        }
        return xml_processor_helper('Company/SecurityInfo/Security', security_mappings, toplevelattributes={'code': 'code'})

    """
    2. Process company profile
    This should in the future done reversely: for all companies, get the company profile and store it in a single dataframe
    Currently however, it's done for a single company.
    """
    def process_company_profile(tree, ticker):
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

        return xml_processor_helper('Company', column_mapping_companyInfo, fixed_columns={'ticker': ticker})

    """
    3a. Process periods (annual)
    """
    def process_periods_annual(tree, ticker):
        column_mapping_companyInfo = {
            'values': {}, 
            'attributes': {},
        }
        attrib_list = {'fYear': 'fYear', 'periodLength': 'periodLength', 'periodUnit': 'periodUnit', 'endMonth': 'endMonth', 'fyNum': 'fyNum'}


        return xml_processor_helper('Company/CompanyInfo/CompanyPeriods/Annual', fixed_columns={'ticker': ticker}, toplevelattributes=attrib_list)

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
        return xml_processor_helper('Actuals/FYActuals/FYActual', actual_mappings, fixed_columns={'ticker': ticker}, toplevelattributes={'actualType': 'type', 'actualUnit': 'unit'})
    
    def process_actuals_annual(tree, ticker):
        return process_actuals_helper(tree, ticker, 'A')

    """
    4b. Process actuals (interim)
    """
    def process_actuals_interim(tree, ticker):
        return process_actuals_helper(tree, ticker, 'Q')
    
    """
    5a. Fiscal Year Estimates 
    """
    def process_fiscal_year_estimates_helper(tree, ticker, periodType='A'):
        fy_collectionlevel_attrs = {'type': 'type', 'unit': 'unit'}
        fy_extra_fixed_columns = {'ticker': ticker}
        fy_itemlevel_mappings = {
                    'values': {
                        'high_curr': f"ConsEstimate[@type='High']/ConsValue[@dateType='CURR']",
                        'low_curr': f"ConsEstimate[@type='Low']/ConsValue[@dateType='CURR']",
                        'mean_curr': f"ConsEstimate[@type='Mean']/ConsValue[@dateType='CURR']",
                        'mean_1ma': f"ConsEstimate[@type='Mean']/ConsValue[@dateType='1MA']",
                        'mean_3ma': f"ConsEstimate[@type='Mean']/ConsValue[@dateType='3MA']",
                        'median_curr': f"ConsEstimate[@type='Median']/ConsValue[@dateType='CURR']",
                        'stdev_curr': f"ConsEstimate[@type='StdDev']/ConsValue[@dateType='CURR']",
                        'numberOfEst_curr': f"ConsEstimate[@type='NumOfEst']/ConsValue[@dateType='CURR']",
                    },
                    'attributes': {}
                }
        fy_itemlevel_attribs = {
                        'fYear': 'fYear',
                        'endMonth': 'endMonth',
                        'endCalYear': 'endCalYear'
                    }
        # Voor de helper stapt deze een niveau te hoog in. De FYEstimate heeft namelijk per FYPeriod verschillende aantallen waarden.
        # Daarom halen we eerst de algemene attributen van FYEstimate op. Daarna gaan we een niveau dieper.
        fyestimates = tree.findall(f"ConsEstimates/FYEstimates/FYEstimate")
        columns = [ a for a in fy_collectionlevel_attrs.keys() ] + [ c for c in fy_extra_fixed_columns.keys() ] + list(fy_itemlevel_mappings['values'].keys()) + list(fy_itemlevel_attribs.keys())
        column_data = {c: [] for c in columns}

        for f in fyestimates:
            extra_fixed_columns = {'ticker': ticker, 'type': f.attrib['type'], 'unit': f.attrib['unit']}
            fyperiods = f.findall(f"FYPeriod[@periodType='{periodType}']")

            for el in fyperiods:
                for key, value in fy_itemlevel_mappings['values'].items():
                    column_data[key] += [ e.text for e in el.findall(value)]
                    
                for key, (value, attribute) in fy_itemlevel_mappings['attributes'].items():
                    column_data[key] += [ e.attrib[attribute] for e in el.findall(value)]

                for key, value in fy_itemlevel_attribs.items():
                    column_data[key] += [ el.attrib[value] ] * (max([len(a) for a in column_data.values()]) - len(column_data[key]))
                
                for key, value in extra_fixed_columns.items():
                    column_data[key] += [ value ] * (max([len(a) for a in column_data.values()]) - len(column_data[key]))
                
                for key in column_data:
                    column_data[key] += [None] * (max([len(a) for a in column_data.values()]) - len(column_data[key]))
            
        # Fill dataframe:
        df = pd.DataFrame(columns=columns)
        for c in columns:
            df[c] = column_data[c]
        return df
    
        

    def process_fiscal_year_estimates_annual(tree, ticker):
        return process_fiscal_year_estimates_helper(tree, ticker, 'A')

    def process_fiscal_year_estimates_interim(tree, ticker):
        return process_fiscal_year_estimates_helper(tree, ticker, 'Q')

    """
    5b. Net Profit Estimates
    """
    def process_net_profit_estimates(tree, ticker):
        estimates = tree.findall(f"ConsEstimates/NPEstimates/NPEstimate")
        estimate_mappings = {
            'values': {
                'high_curr': f"ConsEstimate[@type='High']/ConsValue[@dateType='CURR']",
                'low_curr': f"ConsEstimate[@type='Low']/ConsValue[@dateType='CURR']",
                'mean_curr': f"ConsEstimate[@type='Mean']/ConsValue[@dateType='CURR']",
                'median_curr': f"ConsEstimate[@type='Median']/ConsValue[@dateType='CURR']",
            },
            'attributes': {}
        }
        
        columns = ['ticker', 'type', 'unit'] + list(estimate_mappings['values'].keys()) + list(estimate_mappings['attributes'].keys())
        column_data = {c: [] for c in columns}

        for npestimate in estimates:
            for key, value in estimate_mappings['values'].items():
                column_data[key] += [ e.text for e in npestimate.findall(value)]
                
            for key, (value, attribute) in estimate_mappings['attributes'].items():
                column_data[key] += [ e.attrib[attribute] for e in npestimate.findall(value)]

            column_data['ticker'] += [ticker] * (len(column_data[key]) - len(column_data['ticker']))
            column_data['type'] += [npestimate.attrib['type']] * (len(column_data[key]) - len(column_data['type']))
            column_data['unit'] += [npestimate.attrib['unit']]  * (len(column_data[key]) - len(column_data['unit']))

        # Fill dataframe:
        df = pd.DataFrame(columns=columns)
        for c in columns:
            df[c] = column_data[c]
        return df

    # Process the XML file
    process_company_profile(tree)

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