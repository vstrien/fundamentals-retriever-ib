import xml.etree.ElementTree as ET
import os
import pandas as pd


# list all subdirectories in the "fundamentals" directory
companies = [ name for name in os.listdir('fundamentals') if os.path.isdir(os.path.join('fundamentals', name)) ]

class ib_xml_processor():
    def __init__(self, xml_file):
        self.tree = ET.parse(xml_file)

        self.processing_methods = {}

    def _xml_processor(self, tree, rootelementpath: str, mappings: dict = {'values': {}, 'attributes': {}}, toplevelattributes: dict = {}, fixed_columns: dict = {}):
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

class ReportsFinStatements_Processor(ib_xml_processor):
    def __init__(self, xml_file):
        super().__init__(xml_file)
        self.processing_methods = {
            'toplevel_info': self.process_toplevel_info,
            'issues': self.process_issues,
            'financial_statement_column_mapping': self.process_financial_statement_column_mapping,
            'balance_sheet_annual': self.process_balance_sheet_annual,
            'income_statement_annual': self.process_income_statement_annual,
            'cash_flow_annual': self.process_cash_flow_annual,
            'balance_sheet_interim': self.process_balance_sheet_interim,
            'income_statement_interim': self.process_income_statement_interim,
            'cash_flow_interim': self.process_cash_flow_interim
        }
    

    def process_toplevel_info(self):
        toplevel_mappings = {
            'values': {
                'RepNo': 'CoIDs/CoID[@Type="RepNo"]',
                'CompanyName': 'CoIDs/CoID[@Type="CompanyName"]',
                'IRSNo': 'CoIDs/CoID[@Type="IRSNo"]',
                'CIKNo': 'CoIDs/CoID[@Type="CIKNo"]',
                'OrganizationPermID': 'CoIDs/CoID[@Type="OrganizationPermID"]',
                'CashFlowMethod': 'StatementInfo/CashFlowMethod',
                'BalanceSheetDisplay': 'StatementInfo/BalanceSheetDisplay',
                'COAType': 'StatementInfo/COAType',
            },
            'attributes': {
                'CashFlowMethodCode': ('StatementInfo/CashFlowMethod', 'Code'),
                'BlanceSheetDisplayCode': ('StatementInfo/BalanceSheetDisplay', 'Code'),
                'COATypeCode': ('StatementInfo/COAType', 'Code'),
            }
        }
        return self._xml_processor(self.tree, '.', mappings=toplevel_mappings)
    
    def process_issues(self):
        issue_mappings = {
            'values': {
                'Issue Name': 'IssueID[@Type="Name"]',
                'Issue Ticker': 'IssueID[@Type="Ticker"]',
                'Issue RIC': 'IssueID[@Type="RIC"]',
                'Issue DisplayRIC': 'IssueID[@Type="DisplayRIC"]',
                'Issue InstrumentPI': 'IssueID[@Type="InstrumentPI"]',
                'Issue QuotePI': 'IssueID[@Type="QuotePI"]',
                'Issue InstrumentPermID': 'IssueID[@Type="InstrumentPermID"]',
                'Issue QuotePermID': 'IssueID[@Type="QuotePermID"]',
                'Issue Exchange': 'Exchange',
                'Issue MostRecentSplit': 'MostRecentSplit',
            },
            'attributes': {
                'ExchangeCode': ('Exchange', 'Code'),
                'ExchangeCountry': ('Exchange', 'Country'),
                'MostRecentSplit Date': ('MostRecentSplit', 'Date'),
            }
        }
        return self._xml_processor(self.tree, 'Issues/Issue', issue_mappings, {'IssueID': 'ID', 'IssueType': 'Type', 'IssueDesc': 'Desc', 'IssueOrder': 'Order'})

    def process_financial_statement_column_mapping(self):
        fs_mappings = {
            'values': {
                'ColumnDesc': 'mapItem',
            },
            'attributes': {
                'ColumnCode': ('mapItem', 'coaItem'),
                'StatementType': ('mapItem', 'statementType'),
                'lineID': ('mapItem', 'lineID'),
                'precision': ('mapItem', 'precision'),
            }
        }
        return self._xml_processor(self.tree, 'FinancialStatements/COAMap', fs_mappings)
    
    def _process_financial_statements_helper(self, periodType = 'Annual', statementType = 'INC'):
        # The financial statements are too deeply nested to be processed by the xml processor.
        # Therefor, we will traverse the statements and hand over to the xml processor for the final step.
        
        # First, let's get the possible columns for all types of financial statements:
        mapitems = self.tree.findall(f"FinancialStatements/COAMap/mapItem[@statementType='{statementType}']")
        coaitems = []

        for map in mapitems:
            coaitems += [map.attrib['coaItem']]

        dfs = []
        
        itemlevel_mappings = {
            'values': {
                f"{coacode}": f"lineItem[@coaCode='{coacode}']" for coacode in coaitems
            },
            'attributes': {
                'periodTypeCode': ('FPHeader/periodType', 'Code'),
                'UpdateTypeCode': ('FPHeader/UpdateType', 'Code'),
                'SourceDate': ('FPHeader/Source', 'Date'),
            }
        }
        itemlevel_mappings['values'].update({
                'PeriodLength': 'FPHeader/PeriodLength',
                'periodType': 'FPHeader/periodType',
                'UpdateType': 'FPHeader/UpdateType',
                'StatementDate': 'FPHeader/StatementDate',
                'Source': 'FPHeader/Source',
        })
        itemlevel_attribs = {
                    'StatementType': 'Type'
        }
        list_fixed_columns_map = {
            'FiscalPeriodType': 'Type', 
            'FiscalPeriodEndDate': 'EndDate', 
            'FiscalPeriodYear': 'FiscalYear'
        }
        if periodType == 'Interim':
            list_fixed_columns_map['FiscalPeriodNumber'] = 'FiscalPeriodNumber'
        
        
        # Second, let's get the actual fiscal periods. We need to iterate those, because there are some attributes we want to add to this dataset:
        fiscalperiods = self.tree.findall(f'FinancialStatements/{periodType}Periods/FiscalPeriod[@Type="{periodType}"]')
        for f in fiscalperiods:
            fixed_columns = {k: f.attrib[v] for k, v in list_fixed_columns_map.items()}
            
            dfs += [ self._xml_processor(f, f"Statement[@Type='{statementType}']", itemlevel_mappings, itemlevel_attribs, fixed_columns) ]
        
        if len(fiscalperiods) == 0:
            # If there are no fiscal periods, we need to return an empty dataframe with the correct columns.

            # This is kinda ugly: the columns to be returned are calculated on two places.
            return pd.DataFrame(columns= list(itemlevel_mappings['values'].keys()) + list(itemlevel_mappings['attributes'].keys()) + list(itemlevel_attribs.keys()) + list(list_fixed_columns_map.keys()))


        # Concatenate all dataframes
        return pd.concat(dfs)

    def process_balance_sheet_annual(self):
        return self._process_financial_statements_helper('Annual', 'BAL')
    
    def process_income_statement_annual(self):
        return self._process_financial_statements_helper('Annual', 'INC')

    def process_cash_flow_annual(self):
        return self._process_financial_statements_helper('Annual', 'CAS')

    def process_balance_sheet_interim(self):
        return self._process_financial_statements_helper('Interim', 'BAL')
    
    def process_income_statement_interim(self):
        return self._process_financial_statements_helper('Interim', 'INC')

    def process_cash_flow_interim(self):
        return self._process_financial_statements_helper('Interim', 'CAS')
class RESC_Processor(ib_xml_processor):
    def __init__(self, xml_file):
        super().__init__(xml_file)
    
        self.processing_methods = {
            'security_info': self.process_security_info,
            'company_profile': self.process_company_profile,
            'periods_annual': self.process_periods_annual,
            'periods_interim': self.process_periods_interim,
            'actuals_annual': self.process_actuals_annual,
            'actuals_interim': self.process_actuals_interim,
            'fiscal_year_estimates_annual': self.process_fiscal_year_estimates_annual,
            'fiscal_year_estimates_interim': self.process_fiscal_year_estimates_interim,
            'net_profit_estimates': self.process_net_profit_estimates,
        }

    """
    1. Process security info
    """
    def process_security_info(self):

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
        
        return self._xml_processor(self.tree, 'Company/SecurityInfo/Security', security_mappings, toplevelattributes={'code': 'code'})

    """
    2. Process company profile
    This should in the future done reversely: for all companies, get the company profile and store it in a single dataframe
    Currently however, it's done for a single company.
    """
    def process_company_profile(self):
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

        return self._xml_processor(self.tree, 'Company', column_mapping_companyInfo)

    """
    3a. Process periods (annual)
    """
    def process_periods_annual(self):
        attrib_list = {'fYear': 'fYear', 'periodLength': 'periodLength', 'periodUnit': 'periodUnit', 'endMonth': 'endMonth', 'fyNum': 'fyNum'}

        return self._xml_processor(self.tree, 'Company/CompanyInfo/CompanyPeriods/Annual', toplevelattributes=attrib_list)

    """
    3b. Process periods (interim)
    """
    def process_periods_interim(self):
        attrib_list = {'type': 'type', 'periodNum': 'periodNum', 'periodLength': 'periodLength', 'periodUnit': 'periodUnit', 'endMonth': 'endMonth', 'endCalYear': 'endCalYear'}

        return self._xml_processor(self.tree, 'Company/CompanyInfo/CompanyPeriods/Annual/Interim', toplevelattributes=attrib_list)

    """
    4a. Process actuals (annual)
    """
    def _process_actuals_helper(self, periodType):
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
        return self._xml_processor(self.tree, 'Actuals/FYActuals/FYActual', actual_mappings, toplevelattributes={'actualType': 'type', 'actualUnit': 'unit'})
    
    def process_actuals_annual(self):
        return self._process_actuals_helper('A')

    """
    4b. Process actuals (interim)
    """
    def process_actuals_interim(self):
        return self._process_actuals_helper('Q')
    
    """
    5a. Fiscal Year Estimates 
    """
    def _process_fiscal_year_estimates_helper(self, periodType='A'):
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
        fyestimates = self.tree.findall(f"ConsEstimates/FYEstimates/FYEstimate")
        
        dfs = []

        for f in fyestimates:
            extra_fixed_columns = {'type': f.attrib['type'], 'unit': f.attrib['unit']}

            dfs += [ self._xml_processor(f, f"FYPeriod[@periodType='{periodType}']", fy_itemlevel_mappings, fy_itemlevel_attribs, extra_fixed_columns) ]

        # Concatenate all dataframes
        return pd.concat(dfs)
    
        

    def process_fiscal_year_estimates_annual(self):
        return self._process_fiscal_year_estimates_helper('A')

    def process_fiscal_year_estimates_interim(self):
        return self._process_fiscal_year_estimates_helper('Q')

    """
    5b. Net Profit Estimates
    """
    def process_net_profit_estimates(self):
        estimate_mappings = {
            'values': {
                'high_curr': f"ConsEstimate[@type='High']/ConsValue[@dateType='CURR']",
                'low_curr': f"ConsEstimate[@type='Low']/ConsValue[@dateType='CURR']",
                'mean_curr': f"ConsEstimate[@type='Mean']/ConsValue[@dateType='CURR']",
                'median_curr': f"ConsEstimate[@type='Median']/ConsValue[@dateType='CURR']",
            },
            'attributes': {}
        }
        
        return self._xml_processor(self.tree, 'ConsEstimates/NPEstimates/NPEstimate', estimate_mappings, toplevelattributes={'type': 'type', 'unit': 'unit'})


class ReportSnapshot_Processor(ib_xml_processor):
    def __init__(self, xml_file):
        super().__init__(xml_file)
    
        self.processing_methods = {
            'toplevel_info': self.process_toplevel_info, 
            'issues': self.process_issues, 
            'ratios': self.process_ratios, 
            'forecast_data': self.process_forecast_data,
        }

    def process_toplevel_info(self):
        toplevel_mappings = {
            'values': {
                'RepNo': 'CoIDs/CoID[@Type="RepNo"]',
                'CompanyName': 'CoIDs/CoID[@Type="CompanyName"]',
                'IRSNo': 'CoIDs/CoID[@Type="IRSNo"]',
                'CIKNo': 'CoIDs/CoID[@Type="CIKNo"]',
                'OrganizationPermID': 'CoIDs/CoID[@Type="OrganizationPermID"]',
                'LatestAvailableAnnual': 'CoGeneralInfo/LatestAvailableAnnual',
                'LatestAvailableInterim': 'CoGeneralInfo/LatestAvailableInterim',
                'ReportingCurrency': 'CoGeneralInfo/ReportingCurrency',
                'SharesOutstanding': 'CoGeneralInfo/SharesOut',
                'Business Summary': 'TextInfo/Text[@Type="Business Summary"]',
                'Financial Summary': 'TextInfo/Text[@Type="Financial Summary"]',
                'IndustryInfo_TRBC': 'peerInfo/IndustryInfo/Industry[@type="TRBC"][@order="1"]',
                'IndustryInfo_NAICS_1': 'peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="1"]',
                'IndustryInfo_NAICS_2': 'peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="2"]',
                'IndustryInfo_NAICS_3': 'peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="3"]',
                'IndustryInfo_NAICS_4': 'peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="4"]',
                'IndustryInfo_NAICS_5': 'peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="5"]',
                'IndustryInfo_SIC_1': 'peerInfo/IndustryInfo/Industry[@type="SIC"][@order="1"]',
                'IndustryInfo_SIC_2': 'peerInfo/IndustryInfo/Industry[@type="SIC"][@order="2"]',
                'IndustryInfo_SIC_3': 'peerInfo/IndustryInfo/Industry[@type="SIC"][@order="3"]',
                'IndustryInfo_SIC_4': 'peerInfo/IndustryInfo/Industry[@type="SIC"][@order="4"]',
                'IndustryInfo_SIC_5': 'peerInfo/IndustryInfo/Industry[@type="SIC"][@order="5"]',
                'website': 'webLinks/webSite[@mainCategory="Home Page"]',
                'email': 'webLinks/webSite[@mainCategory="Company Contact/E-mail"]'
            },
            'attributes': {
                'CashFlowMethodCode': ('StatementInfo/CashFlowMethod', 'Code'),
                'BlanceSheetDisplayCode': ('StatementInfo/BalanceSheetDisplay', 'Code'),
                'COATypeCode': ('StatementInfo/COAType', 'Code'),
                'IndustryInfo_lastUpdated': ('peerInfo', 'lastUpdated'),
                'IndustryInfo_lastUpdated': ('peerInfo', 'lastUpdated'),
                'IndustryInfo_TRBC_Code': ('peerInfo/IndustryInfo/Industry[@type="TRBC"][@order="1"]', 'code'),
                'IndustryInfo_NAICS_1_Code': ('peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="1"]', 'code'),
                'IndustryInfo_NAICS_2_Code': ('peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="2"]', 'code'),
                'IndustryInfo_NAICS_3_Code': ('peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="3"]', 'code'),
                'IndustryInfo_NAICS_4_Code': ('peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="4"]', 'code'),
                'IndustryInfo_NAICS_5_Code': ('peerInfo/IndustryInfo/Industry[@type="NAICS"][@order="5"]', 'code'),
                'IndustryInfo_SIC_1Code': ('peerInfo/IndustryInfo/Industry[@type="SIC"][@order="1"]', 'code'),
                'IndustryInfo_SIC_2Code': ('peerInfo/IndustryInfo/Industry[@type="SIC"][@order="2"]', 'code'),
                'IndustryInfo_SIC_3Code': ('peerInfo/IndustryInfo/Industry[@type="SIC"][@order="3"]', 'code'),
                'IndustryInfo_SIC_4Code': ('peerInfo/IndustryInfo/Industry[@type="SIC"][@order="4"]', 'code'),
                'IndustryInfo_SIC_5Code': ('peerInfo/IndustryInfo/Industry[@type="SIC"][@order="5"]', 'code'),
            }
        }
        return self._xml_processor(self.tree, '.', mappings=toplevel_mappings)

    def process_issues(self):
        toplevel_mappings = {
            'values': {
                'Issue Name': 'IssueID[@Type="Name"]',
                'Issue Ticker': 'IssueID[@Type="Ticker"]',
                'Issue RIC': 'IssueID[@Type="RIC"]',
                'Issue DisplayRIC': 'IssueID[@Type="DisplayRIC"]',
                'Issue InstrumentPI': 'IssueID[@Type="InstrumentPI"]',
                'Issue QuotePI': 'IssueID[@Type="QuotePI"]',
                'Issue InstrumentPermID': 'IssueID[@Type="InstrumentPermID"]',
                'Issue QuotePermID': 'IssueID[@Type="QuotePermID"]',
                'Issue Exchange': 'Exchange',
                'Issue MostRecentSplit': 'MostRecentSplit',
            },
            'attributes': {
                'ExchangeCode': ('Exchange', 'Code'),
                'ExchangeCountry': ('Exchange', 'Country'),
                'MostRecentSplit Date': ('MostRecentSplit', 'Date'),
            }
        }
        return self._xml_processor(self.tree, 'Issues/Issue', toplevel_mappings, toplevelattributes={'IssueID': 'ID', 'IssueType': 'Type', 'IssueDesc': 'Desc', 'IssueOrder': 'Order'})

    def process_ratios(self):
        toplevel_mappings = {
            'values': {
                'NPRICE'   : 'Group[@ID="Price and Volume"]/Ratio[@FieldName="NPRICE"   ]',
                "NHIG"     : 'Group[@ID="Price and Volume"]/Ratio[@FieldName="NHIG"     ]',
                "NLOW"     : 'Group[@ID="Price and Volume"]/Ratio[@FieldName="NLOW"     ]',
                "PDATE"    : 'Group[@ID="Price and Volume"]/Ratio[@FieldName="PDATE"    ]',
                "VOL10DAVG": 'Group[@ID="Price and Volume"]/Ratio[@FieldName="VOL10DAVG"]',
                "EV"       : 'Group[@ID="Price and Volume"]/Ratio[@FieldName="EV"       ]',

                "MKTCAP"  : 'Group[@ID="Income Statement"]/Ratio[@FieldName="MKTCAP"  ]',
                "TTMREV"  : 'Group[@ID="Income Statement"]/Ratio[@FieldName="TTMREV"  ]',
                "TTMEBITD": 'Group[@ID="Income Statement"]/Ratio[@FieldName="TTMEBITD"]',
                "TTMNIAC" : 'Group[@ID="Income Statement"]/Ratio[@FieldName="TTMNIAC" ]',

                "TTMEPSXCLX": 'Group[@ID="Per share data"]/Ratio[@FieldName="TTMEPSXCLX"]',
                "TTMREVPS"  : 'Group[@ID="Per share data"]/Ratio[@FieldName="TTMREVPS"  ]',
                "QBVPS"     : 'Group[@ID="Per share data"]/Ratio[@FieldName="QBVPS"     ]',
                "QCSHPS"    : 'Group[@ID="Per share data"]/Ratio[@FieldName="QCSHPS"    ]',
                "TTMCFSHR"  : 'Group[@ID="Per share data"]/Ratio[@FieldName="TTMCFSHR"  ]',
                "TTMDIVSHR" : 'Group[@ID="Per share data"]/Ratio[@FieldName="TTMDIVSHR" ]',

                "TTMGROSMGN": 'Group[@ID="Other Ratios"]/Ratio[@FieldName="TTMGROSMGN"]',
                "TTMROEPCT" : 'Group[@ID="Other Ratios"]/Ratio[@FieldName="TTMROEPCT" ]',
                "TTMPR2REV" : 'Group[@ID="Other Ratios"]/Ratio[@FieldName="TTMPR2REV" ]',
                "PEEXCLXOR" : 'Group[@ID="Other Ratios"]/Ratio[@FieldName="PEEXCLXOR" ]',
                "PRICE2BK"  : 'Group[@ID="Other Ratios"]/Ratio[@FieldName="PRICE2BK"  ]',
                "Employees" : 'Group[@ID="Other Ratios"]/Ratio[@FieldName="Employees" ]',
            },
            'attributes': {}
        }
        return self._xml_processor(self.tree, 'Ratios', toplevel_mappings, toplevelattributes={'PriceCurrency': 'PriceCurrency', 'ReportingCurrency': 'ReportingCurrency', 'ExchangeRate': 'ExchangeRate', 'LatestAvailableDate': 'LatestAvailableDate'})

    def process_forecast_data(self):
        toplevel_mappings = {
            'values': {
                'ConsRecom': 'Ratio[@FieldName="ConsRecom"]/Value[@PeriodType="CURR"]',
                'TargetPrice': 'Ratio[@FieldName="TargetPrice"]/Value[@PeriodType="CURR"]',
                'ProjLTGrowthRate': 'Ratio[@FieldName="ProjLTGrowthRate"]/Value[@PeriodType="CURR"]',
                'ProjPE': 'Ratio[@FieldName="ProjPE"]/Value[@PeriodType="CURR"]',
                'ProjSales': 'Ratio[@FieldName="ProjSales"]/Value[@PeriodType="CURR"]',
                'ProjSalesQ': 'Ratio[@FieldName="ProjSalesQ"]/Value[@PeriodType="CURR"]',
                'ProjEPS': 'Ratio[@FieldName="ProjEPS"]/Value[@PeriodType="CURR"]',
                'ProjEPSQ': 'Ratio[@FieldName="ProjEPSQ"]/Value[@PeriodType="CURR"]',
                'ProjProfit': 'Ratio[@FieldName="ProjProfit"]/Value[@PeriodType="CURR"]',
                'ProjDPS': 'Ratio[@FieldName="ProjDPS"]/Value[@PeriodType="CURR"]',
            },
            'attributes': {}
        }
        return self._xml_processor(self.tree, 'ForecastData', toplevel_mappings, toplevelattributes={'ConsensusType': 'ConsensusType', 'CurFiscalYear': 'CurFiscalYear', 'CurFiscalYearEndMonth': 'CurFiscalYearEndMonth', 'CurInterimEndCalYear': 'CurInterimEndCalYear', 'CurInterimEndMonth': 'CurInterimEndMonth', 'EarningsBasis': 'EarningsBasis'})

functionmapping = {
    'ReportsFinStatements': ReportsFinStatements_Processor,
    'RESC': RESC_Processor,
    'ReportSnapshot': ReportSnapshot_Processor
}

if not os.path.exists(f'./export'):
    os.makedirs(f'./export')

results = {}

for reportType, function in functionmapping.items():
    for comp in companies:
        # if file 'ReportsFinStatements.xml' does not exist, skip
        file_to_process = f'./fundamentals/{comp}/{reportType}.xml'
        if os.path.exists(file_to_process):
            # Check first 2 characters of the file to see if it is valid XML and not an empty JSON list
            with open(file_to_process, 'r') as f:
                contents = f.read(2)
                if contents.startswith('[]'):
                    print(f'File {reportType}.xml for {comp} is empty. Skipping...')
                    continue
            
            proc_object = function(f'./fundamentals/{comp}/{reportType}.xml')
            for subreport_type, f in proc_object.processing_methods.items():
                print(f"Processing {comp} {reportType} {subreport_type}")
                df = f()
                df['symbol'] = comp
                df['reportType'] = reportType
                if subreport_type not in results:
                    results[subreport_type] = df
                else:
                    results[subreport_type] = pd.concat([results[subreport_type], df])

    # Write report type to the export directory
    for subreport_type, df in results.items():
        # Remove empty columns
        df = df.dropna(axis=1, how='all')

        # Detect data types
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col])
                except:
                    pass

        df.to_parquet(f'./export/{reportType}_{subreport_type}.parquet')
        print(f'Processed {reportType}_{subreport_type}')
