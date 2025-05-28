import dateutil.relativedelta
import pandas as pd
import numpy as np
import re
import json
import datetime
import traceback
# from datetime import date, datetime
from datetime import timedelta
from myproject.basicfunctions import Finders
import decimal
from babel.numbers import format_currency


class Deliveryformatting:
    # def __init__(self, config_file):
    #     with open(config_file, 'r') as f:
    #         self.config = json.load(f)
    #
    # def get_delivery(self, delivery_name):
    #     for delivery in self.config['deliveries']:
    #         if delivery['name'] == delivery_name:
    #             return delivery
    #     raise ValueError(f"Delivery '{delivery_name}' not found in configuration.")

    # def inject_session_dates(filters):
    #     for f in filters:
    #         if f["function"] in ["date_of_current_price_except_reits", "period_end_date_except_reits"]:
    #             f["args"]["Period End Date"] = st.session_state.period_end_date.strftime('%Y-%m-%d')
    #         if f["function"] in ["some_func"]:
    #             f["args"]["Date of Extraction"] = st.session_state.date_extraction.strftime('%Y-%m-%d')
    #     return filters

    def resolve_date(value):
        today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
        next_month = f'{today.month:02d}'
        year = datetime.date.today().year
        if value == "first_day_month":
            new_value = datetime.datetime(int(year), int(next_month), int(1)).strftime('%d-%m-%Y')
            return new_value
        # elif value == ''
        # try:
        #     return datetime.datetime(new_value).strftime('%d-%m-%Y')
        # except Exception:
        #     return value  # fallback if not a date

    def resolve_format(client, value):
        today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
        next_month = f'{today.month:02d}'
        year = datetime.date.today().year
        if value == "client_01{month}{year}":
            new_value = f'{client}_01{next_month}{year}'
        else:
            new_value=value

        return new_value

    def apply_other_functions(universe, config, rb=None):
        """
        Apply additional processing functions specified in the config's "other_functions" section.

        Args:
            universe (dict): Dictionary containing all dataframes
            config (dict): Delivery configuration dictionary
            rb: Optional rulebook parameter

        Returns:
            Updated universe dictionary with new sheets added
        """
        if "other_functions" not in config:
            return universe

        for func_config in config["other_functions"]:
            func_name = func_config["function"]
            sheet_names = func_config.get("sheets_names", [])
            inputs = func_config.get("inputs", [])

            # Prepare arguments for the function
            kwargs = {}
            for input_spec in inputs:
                # Handle direct universe reference shorthand (your style)
                if input_spec in universe:  # Like "CC"
                    kwargs['file'] = universe[input_spec]

                # Handle explicit universe['key'] syntax
                elif input_spec.startswith("universe[") and input_spec.endswith("]"):
                    key = input_spec.split("[")[1].split("]")[0].strip("'\"")
                    param_name = input_spec.split("=")[0].strip() if "=" in input_spec else "raw_file"
                    kwargs[param_name] = universe.get(key)

                # Handle direct rb parameter
                elif input_spec == "rb":
                    kwargs["rb"] = rb

                # Handle other direct parameters
                else:
                    kwargs[input_spec] = locals().get(input_spec) or globals().get(input_spec)

            # Get the function from current scope
            processing_func = None

            # 1. First try as standalone function
            processing_func = globals().get(func_name)
            if not processing_func:
                print(f"Function {func_name} not found")
                continue

            # Call the function and handle results
            try:
                results = processing_func(**kwargs)

                # Handle single return vs multiple returns
                if len(sheet_names) == 1:
                    universe[sheet_names[0]] = results
                else:
                    if isinstance(results, (tuple, list)):
                        for i, sheet_name in enumerate(sheet_names):
                            if i < len(results):
                                universe[sheet_name] = results[i]
                    else:
                        print(f"Function {func_name} didn't return expected number of results")

            except Exception as e:
                print(f"Error applying function {func_name}: {str(e)}")
                print(traceback.format_exc())

        return universe


    # def create_delivery_json(name, input_files, filters, comparisons, output_columns, table_data):
    #     """
    #     Creates a JSON structure for a delivery.
    #
    #     Args:
    #         name (str): Name of the delivery (e.g., "Delivery_A").
    #         input_files (int): Number of input files required.
    #         filters (list): List of filter criteria (e.g., [{"column": "Status", "value": "Pending", "operation": "equals"}]).
    #         comparisons (list): List of comparison criteria (e.g., [{"file1_column": "Order_ID", "file2_column": "Order_ID", "operation": "match"}]).
    #         output_columns (list): List of output columns (e.g., ["Order_ID", "Status", "Amount"]).
    #         table_data (list): List of dictionaries representing the table data (e.g., [{"Order_ID": 101, "Status": "Pending", "Amount": 150}]).
    #
    #     Returns:
    #         dict: JSON structure for the delivery.
    #     """
    #     delivery_json = {
    #         "name": name,
    #         "input_files": input_files,
    #         "filters": filters,
    #         "comparisons": comparisons,
    #         "output_columns": output_columns,
    #         "table": table_data
    #     }
    #     return delivery_json



    # Example usage


    # def create_delivery_method(class_name, client_name, region=None):
    #     """
    #     Dynamically creates a delivery method based on client and region
    #
    #     Args:
    #         class_name: The class to add the method to (e.g. GlobalDeliveries, SAUDI_DELIVERIES)
    #         client_name: Name of the client
    #         region: Optional region specifier (e.g. SAUDI, GCC, US)
    #     """
    #
    #     def delivery_method(self, criteria_dict):
    #         # Common delivery logic
    #         try:
    #             # Get file columns based on client configuration
    #             columns = self.find_all_columns(self.raw_file)
    #
    #             # Basic validation
    #             if not all(col in self.raw_file.columns for col in columns.values()):
    #                 raise ValueError(f"Missing required columns for {client_name} delivery")
    #
    #             # Process data based on criteria
    #             result_df = self.raw_file.copy()
    #
    #             # Apply criteria filters
    #             for criterion, value in criteria_dict.items():
    #                 if criterion in result_df.columns:
    #                     result_df = result_df[result_df[criterion] == value]
    #
    #             # Add standard processing
    #             result_df = self.add_standard_columns(result_df)
    #
    #             return result_df
    #
    #         except Exception as e:
    #             raise Exception(f"Error in {client_name} delivery: {str(e)}")
    #
    #     # Create method name
    #     method_name = f"{client_name.upper()}_{'_'.join(filter(None, [region, 'Delivery']))}"
    #
    #     # Add method to class
    #     setattr(class_name, method_name, delivery_method)
    #
    #     return method_name
    #
    # def configure_new_delivery():
    #     """
    #     Interface function to collect delivery configuration from user
    #     Returns dict with delivery configuration
    #     """
    #     config = {
    #         'client_name': '',
    #         'region': '',
    #         'required_columns': [],
    #         'criteria_fields': [],
    #         'output_format': '',
    #         'additional_processing': []
    #     }
    #
    #     # Get basic info
    #     config['client_name'] = input("Enter client name: ")
    #     config['region'] = input("Enter region (GLOBAL/SAUDI/GCC/US/EGYPT or blank): ")
    #
    #     # Get required columns
    #     while True:
    #         col = input("Enter required column (or blank to finish): ")
    #         if not col:
    #             break
    #         config['required_columns'].append(col)
    #
    #     # Get criteria fields
    #     while True:
    #         field = input("Enter criteria field (or blank to finish): ")
    #         if not field:
    #             break
    #         config['criteria_fields'].append(field)
    #
    #     return config
    #
    # def implement_new_delivery(config):
    #     """
    #     Implements a new delivery method based on configuration
    #     """
    #     # Determine appropriate class based on region
    #     class_mapping = {
    #         'GLOBAL': GlobalDeliveries,
    #         'SAUDI': SAUDI_DELIVERIES,
    #         'GCC': GCC_DELIVERIES,
    #         'US': US_DELIVERIES,
    #         'EGYPT': EgyptDeliveries
    #     }
    #
    #     target_class = class_mapping.get(config['region'].upper(), GlobalDeliveries)
    #
    #     # Create the delivery method
    #     method_name = create_delivery_method(
    #         target_class,
    #         config['client_name'],
    #         config['region'] if config['region'] else None
    #     )
    #
    #     return f"Created new delivery method: {method_name}"
    #
    # def add_new_delivery():
    #     """
    #     Main function to add a new delivery method
    #     """
    #     try:
    #         # Get configuration from user
    #         config = configure_new_delivery()
    #
    #         # Implement the delivery
    #         result = implement_new_delivery(config)
    #
    #         print(f"Successfully {result}")
    #
    #     except Exception as e:
    #         print(f"Error creating delivery: {str(e)}")

class Functions:

    def market_cap(new_file:dict, criteria:dict, file:pd.DataFrame,**kwargs):

        excluded = {}
        universe = {}

        Marketcap = Finders.market_cap_finder(file)

        value = float(criteria['Market Cap'])
        for ident, data in new_file.items():
            mc_data = re.sub(r"[^a-zA-Z0-9-.]", "", str(data[Marketcap]))
            if mc_data in ['', ' ', 'N/A'] or float(mc_data) < value:
                data['Comments'] = 'Failing Market Cap'
                excluded[ident] = data
            else:
                universe[ident] = data
        return universe, excluded

    def issue_active_status(new_file: dict, criteria:dict, file: pd.DataFrame, **kwargs):
        universe = {}
        excluded = {}
        issueactive = Finders.issue_active_finder(file)

        for ident, data in new_file.items():
            if str(data[issueactive]).strip().upper() == 'FALSE':
                data['Comments'] = 'Failing Issue Active Status'
                excluded[ident] = data
            else:
                universe[ident] = data
        return universe, excluded

    def ticker_isin_null(new_file:dict, criteria:dict, file:pd.DataFrame,**kwargs):
        universe = {}
        excluded = {}
        ticker = Finders.ticker_finder(file)
        isin = Finders.isin_finder(file)

        for ident, data in new_file.items():
            if (str(data[ticker]).strip() == '' or data[ticker] == np.nan) and (str(data[isin]).strip() == '' or data[isin] == np.nan):
                data['Comments'] = 'Blank Ticker and ISIN'
                excluded[ident] = data
            else:
                universe[ident] = data

        return universe, excluded

    def period_end_date(new_file:dict, criteria,**kwargs):
        universe = {}
        excluded = {}

        min_date = criteria['Period End Date']
        min_date = datetime.datetime.strptime(str(min_date), '%Y-%m-%d')

        for ident, data in new_file.items():
            if str(data['Period End Date']).strip() == '' or data['Period End Date'] == np.nan or data['Period End Date'] == 'N/A':
                data['Comments'] = 'Failing Period End Date'
                excluded[ident] = data
            else:
                current_date = data['Period End Date']
                current_date = datetime.datetime.strptime(str(current_date), '%Y-%m-%d')
                if current_date < (min_date - dateutil.relativedelta.relativedelta(days=30)):
                    data['Comments'] = 'Failing Period End Date'
                    excluded[ident] = data
                else:
                    universe[ident] = data
        return universe, excluded

    def ric_code(file: pd.DataFrame,**kwargs):
        ric = Finders.riccode_finder(file)
        if ric.strip() != "":
            file[ric] = ""
        else:
            file = file
        return file


    def issue_active_status_except_reits(new_file: dict, file:pd.DataFrame, **kwargs):
            universe = {}
            excluded = {}

            Ticker = Finders.ticker_finder(file)
            Issueactivestatus = Finders.issue_active_finder(file)
            for ident, data in new_file.items():
                if str(data[Ticker]).startswith('43') and 'reit' in str(data['Name']).lower():
                    universe[ident] = data
                else:
                    if str(data[Issueactivestatus]).strip().upper() == 'FALSE':
                        data['Comments'] = 'Failing Issue Active Status'
                        excluded[ident] = data
                    else:
                        universe.update({ident: data})
            return universe, excluded


    def isin_null(new_file: dict, file: pd.DataFrame, **kwargs):
        universe = {}
        excluded = {}

        for ident, data in new_file.items():
            if str(data['ISIN']).strip() == '' or data['ISIN'] == np.nan:
                data['Comments'] = 'Blank ISIN'
                excluded[ident] = data
            else:
                universe[ident] = data

        return universe, excluded


    def date_of_current_price_except_reits(new_file: dict,criteria, file:pd.DataFrame,**kwargs):
        universe = {}
        excluded = {}

        Ticker = Finders.ticker_finder(file)

        date_value_raw = criteria['Date Of Current Price']
        date_value_raw = datetime.datetime.strptime(str(date_value_raw), '%Y-%m-%d')
        for ident, data in new_file.items():
            if str(data[Ticker]).startswith('43'):
                universe[ident] = data
            else:
                current_date = data['Date Of Current Price']
                if str(current_date).strip() == '' or current_date == np.nan or current_date == 'N/A':
                    data['Comments'] = 'Failing Date of Current Price'
                    excluded[ident] = data
                else:
                    current_date = datetime.datetime.strptime(str(current_date), '%Y-%m-%d')
                    new_date = date_value_raw - timedelta(days=90)
                    if current_date < new_date:
                        data['Comments'] = 'Failing Date of Current Price'
                        excluded[ident] = data
                    else:
                        universe[ident] = data

        return universe, excluded

    def period_end_date_except_reits(new_file: dict, criteria, file: pd.DataFrame, **kwargs):
        universe = {}
        excluded = {}

        # name = Finders.name_finder(file)
        ticker = Finders.ticker_finder(file)

        min_date = criteria['Period End Date']
        min_date = datetime.datetime.strptime(str(min_date), '%Y-%m-%d')

        for ident, data in new_file.items():
            if 'reit' in data['Name'].lower() and str(data[ticker]).startswith('43'):
                universe[ident] = data
            else:
                if str(data['Period End Date']).strip() == '' or data['Period End Date'] == np.nan:
                    data['Comments'] = 'Failing Period End Date'
                    excluded[ident] = data
                else:
                    current_date = data['Period End Date']
                    current_date = datetime.datetime.strptime(str(current_date), '%Y-%m-%d')
                    if current_date < (min_date - dateutil.relativedelta.relativedelta(days=30)):
                        data['Comments'] = 'Failing Period End Date'
                        excluded[ident] = data
                    else:
                        universe[ident] = data

        return universe, excluded

    def period_end_date_saudireits(new_file, criteria, file, **kwargs):
        universe = {}
        excluded = {}
        ba = Finders.BA_finder(file)
        nation = Finders.nation_finder(file)

        min_date = criteria['Period End Date']
        min_date = datetime.datetime.strptime(str(min_date), '%Y-%m-%d')

        for ident, data in new_file.items():
            if ba:
                if 'reit' in str(data[ba]).strip().lower() and str(
                        data[nation]).lower() == 'saudi arabia':
                    universe[ident] = data
                else:
                    if str(data['Period End Date']).strip() == '' or data['Period End Date'] == np.nan or pd.isnull(data['Period End Date']):
                        data['Comments'] = "Failing Period End Date"
                        excluded[ident] = data
                    else:
                        current_date = data['Period End Date']
                        current_date = str(current_date).split()[0]
                        current_date = datetime.datetime.strptime(str(current_date), '%Y-%m-%d')
                        if current_date < (min_date - dateutil.relativedelta.relativedelta(days=30)):
                            data['Comments'] = "Failing Period End Date"
                            excluded[ident] = data
                        else:
                            universe[ident] = data
            else:
                if str(data['Ticker']).startswith('43') and 'reits' in str(data['Name']).lower().strip() and \
                        'saudi arabia' in str(data['Nation']).lower().strip():
                    universe[ident] = data
                else:
                    if str(data['Period End Date']).strip() == '' or data['Period End Date'] == np.nan or pd.isnull(
                            data['Period End Date']):
                        data['Comments'] = "Failing Period End Date"
                        excluded[ident] = data
                    else:
                        current_date = data['Period End Date']
                        current_date = str(current_date).split()[0]
                        current_date = datetime.datetime.strptime(str(current_date), '%Y-%m-%d')
                        if current_date < (min_date - dateutil.relativedelta.relativedelta(days=30)):
                            data['Comments'] = "Failing Period End Date"
                            excluded[ident] = data
                        else:
                            universe[ident] = data

        return universe, excluded


    def fiscal_period_to_period_end_date(new_file: dict, file: pd.DataFrame,**kwargs):
        x = Finders.fiscal_period_finder(file)

        for ident, data in new_file.items():

            if x in data and 'Period End Date' not in data:
                if str(data[x]).strip() == '' or data[x] == np.nan or data[x] == 'N/A':
                    data['Period End Date'] = " "
                else:
                    Quarter = str(data[x])[5:]

                    Year = str(data[x])[0:4]

                    if Quarter == 'Q 1':
                        Date = '03-31'
                    elif Quarter == 'Q 2' or Quarter == 'S 1':
                        Date = '06-30'
                    elif Quarter == 'Q 3':
                        Date = '09-30'
                    elif Quarter == 'Q 4' or Quarter == 'A 1':
                        Date = '12-31'
                    else:
                        Date = "12-31"

                    data['Period End Date'] = Year + '-' + Date
                    # data['Period End Date'] = pd.to_datetime(data['Period End Date'], format= '%Y-%m-%d')
            else:
                pass
        return new_file

    def adjust_date(new_file:dict, file,**kwargs):
        dates_cols = Finders.date_finder(file)
        for date_col in dates_cols:
            for ident, data in new_file.items():
                if data[date_col] == pd.NaT or str(data[date_col]).strip() == "" or data[date_col] == np.nan or data[date_col] == 'N/A' or str(data[date_col]) == 'NaT' or pd.isnull(data[date_col]) or pd.isna(data[date_col]):
                    data[date_col] = ""

                else:
                    try:
                        data[date_col] = pd.to_datetime(str(data[date_col]), errors='coerce').strftime('%Y-%m-%d')
                    except:
                        data[date_col] = datetime.datetime.strptime(str(data[date_col]), '%Y-%m-%d')
        return new_file

    def date_of_current_price(new_file:dict, criteria, file:pd.DataFrame, **kwargs):
        universe = {}
        excluded = {}
        date_current_price = Finders.date_current_price_finder(file)

        date_value_raw = criteria['Date Of Current Price']
        date_value_raw = datetime.datetime.strptime(str(date_value_raw), '%Y-%m-%d')
        for ident, data in new_file.items():
            current_date = data[date_current_price]
            # current_date = data['Date Of Current Price']
            if str(current_date).strip() == '' or current_date == np.nan:
                data['Comments'] = 'Failing Date of Current Price'
                excluded[ident] = data
            else:
                current_date = datetime.datetime.strptime(str(current_date), '%Y-%m-%d')
                new_date = date_value_raw - timedelta(days=90)
                if current_date < new_date:
                    data['Comments'] = 'Failing Date of Current Price'
                    excluded[ident] = data
                else:
                    universe[ident] = data

        return universe, excluded

    def date_of_current_price_saudireits(new_file, criteria, file, **kwargs):
        universe = {}
        excluded = {}
        ba = Finders.BA_finder(file)
        nation = Finders.nation_finder(file)

        date_value_raw = criteria['Date Of Current Price']
        date_value_raw = datetime.datetime.strptime(str(date_value_raw), '%Y-%m-%d')
        for ident, data in new_file.items():
            current_date = data['Date Of Current Price']
            if ba:
                if 'reit' in str(data[ba]).strip().lower() and str(
                        data[nation]).lower() == 'saudi arabia':
                    universe[ident] = data
                else:
                    if str(current_date).strip() == '' or current_date == np.nan:
                        data['Comments'] = 'Failing Date of current Price'
                        excluded[ident] = data
                    else:
                        current_date = pd.to_datetime(str(current_date), format='%Y-%m-%d', errors='coerce')
                        new_date = date_value_raw - timedelta(days=90)
                        if current_date < new_date:
                            data['Comments'] = 'Failing Date of Current Price'
                            excluded.update({ident: data})
                        else:
                            universe.update({ident: data})
            else:
                if str(data['Ticker']).startswith('43') and 'reits' in str(data['Name']).lower().strip() and\
                        'saudi arabia' in str(data['Nation']).lower().strip():
                    universe[ident] = data
                else:
                    if str(current_date).strip() == '' or current_date == np.nan:
                        data['Comments'] = 'Failing Date of Current Price'
                        excluded[ident] = data
                    else:
                        current_date = pd.to_datetime(str(current_date), format='%Y-%m-%d', errors='coerce')
                        new_date = date_value_raw - timedelta(days=90)
                        if current_date < new_date:
                            data['Comments'] = 'Failing Date of Current Price'
                            excluded.update({ident: data})
                        else:
                            universe.update({ident: data})
        return universe, excluded

    def passing_reits_filter(new_file:dict, file:pd.DataFrame, rb:str, **kwargs):
        universe = {}
        excluded = {}

        Ticker = Finders.ticker_finder(file)
        # Adib = Finders.adib_finder(file)

        for ident, data in new_file.items():
            if str(data[Ticker]).startswith('43') and 'reit' in str(data['Name']).lower():
                if str(data[rb]).strip().upper() == 'FAIL':
                    data[rb] = 'PASS'
                    #excluded[ident] = data
                    universe[ident] = data
                else:
                    universe[ident] = data
            else:
                universe[ident] = data

        return universe, excluded

    def ticker_null(new_file: dict, file: pd.DataFrame, **kwargs):
        universe = {}
        excluded = {}

        Ticker = Finders.ticker_finder(file)

        for ident, data in new_file.items():
            if str(data[Ticker]).strip() == '' or data[Ticker] == np.nan:
                data['Comments'] = "Blank Ticker"
                excluded[ident] = data
            else:
                universe[ident] = data

        return universe, excluded

    def blank_identifiers(new_file:dict, **kwargs):
        universe = {}
        excluded = {}

        for wid, data in new_file.items():
            if data['ISIN'] in ['', ' ', np.nan] and data['SEDOL'] in ['', ' ', np.nan] and data['Ticker'] in ['', ' ', np.nan]:
                data['Comments'] = "Failing for blank identifier"
                excluded[wid] = data
            else:
                universe[wid] = data

        return universe, excluded

    def reits_filter(new_file: dict, file:pd.DataFrame,**kwargs):
        universe = {}
        excluded = {}

        Ticker = Finders.ticker_finder(file)
        Albilad = Finders.albilad_finder(file)
        Aaoifi = Finders.aaoifi_albilad_finder(file)

        for ident, data in new_file.items():
            if str(data[Ticker]).startswith('43') and 'reit' in str(data['Name']).lower():
                if data[Albilad] == 'FAIL' or data[Aaoifi] == 'FAIL':
                    data['Comments'] = "Failing REITs"
                    excluded[ident] = data
                else:
                    universe[ident] = data
            else:
                universe[ident] = data
        return universe, excluded

    def filter_blacklist_albilad(new_file: dict, blacklist_dic: dict, **kwargs):
        universe = {}

        for ident, data in new_file.items():
            if ident in blacklist_dic.keys():
                data['AAOIFI (Albilad )'] = 'FAIL'
                data['Albilad'] = 'FAIL'

            universe[ident] = data

        return universe

    def adjust_ratios_alrajhi(new_file:dict, file:pd.DataFrame,**kwargs):
        ratio_cols = Finders.finder_ratios_alrajhi(file)

        for col in ratio_cols:
            for ident, data in new_file.items():
                if str(data[col]).strip() == "" or data[col] == np.nan or data[col] == 'N/A':
                    data[col] = ""
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                else:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
        return new_file

    def new_compliance_check_df(new_file_df: pd.DataFrame,
                                new_compliance_df: pd.DataFrame,
                                rb: str,
                                **kwargs) -> pd.DataFrame:
        """
        Creates a compliance-adjusted DataFrame by:
        - Keeping all original rows from new_file_df
        - Updating values where compliance data exists and differs
        - Adding any new rows from compliance data that weren't in original
        """
        # Make copies to avoid modifying originals
        universe_df = new_file_df.copy()
        compliance_df = new_compliance_df.copy()

        # Ensure rb column exists in both DataFrames
        if rb not in universe_df.columns or rb not in compliance_df.columns:
            raise ValueError(f"Column '{rb}' not found in both DataFrames")

        # Convert both DataFrames to dictionaries for easier processing
        original_dict = universe_df.to_dict('index')
        compliance_dict = compliance_df.to_dict('index')

        # Process each item (similar to original dictionary logic)
        for ident, data in original_dict.items():
            if ident in compliance_dict:
                original_val = str(data.get(rb, '')).strip().upper()
                compliance_val = str(compliance_dict[ident].get(rb, '')).strip().upper()
                if original_val != compliance_val:
                    data['Comments'] = 'Changed Compliance'
                    data[rb] = compliance_dict[ident][rb]

        # Add any compliance items that weren't in original
        # for ident, data in compliance_dict.items():
        #     if ident not in original_dict:
        #         original_dict[ident] = data

        # Convert back to DataFrame
        universe_df = pd.DataFrame.from_dict(original_dict, orient='index')

        return universe_df

    def adjust_ratios_alrayan(new_file:dict, file:pd.DataFrame,**kwargs):
        ratio_cols = Finders.finder_ratios_alrayan(file)

        for col in ratio_cols:
            for ident, data in new_file.items():
                if str(data[col]).strip() == "" or data[col] == np.nan or data[col] == 'N/A':
                    data[col] = ""
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                else:
                    if '%' in str(data[col]):
                        data[col] = str(data[col]).strip('%')
                        data[col] = pd.to_numeric(data[col], errors='coerce')
                        data[col] = float(data[col])/100
                    else:
                        data[col] = pd.to_numeric(data[col], errors='coerce')

        return new_file

    def adjust_ratios_fransi(new_file:dict, file:pd.DataFrame,**kwargs):
        ratio_cols = Finders.finder_ratios_fransi(file)

        for col in ratio_cols:
            for ident, data in new_file.items():
                if str(data[col]).strip() == "" or data[col] == np.nan or data[col] == 'N/A':
                    data[col] = " "
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                else:
                    data[col] = pd.to_numeric(data[col], errors='coerce')

        return new_file

    def removing_reits(new_file, **kwargs):
        universe = {}
        excluded = {}
        universe = {}

        for ident, data in new_file.items():
            if 'reit' in str(data['Business Activities']).lower().strip():
                data['Comments'] = 'Failing REITs'
                excluded[ident] = data
            else:
                universe[ident] = data

        return universe, excluded

    def clean_ratios_fransi(new_file: dict, file: pd.DataFrame, **kwargs):

        ratio_cols = Finders.finder_ratios_fransi(file)

        for col in ratio_cols:
            for ident, data in new_file.items():
                if str(data[col]).strip() == "" or data[col] == np.nan or data[col] == " ":
                    pass
                else:
                    if round(float(data[col]), 2) > 1.00:
                        data[col] = '1'
                    elif str(data[col]).strip().lower() == 'inf':
                        data[col] = '1'

        return new_file

    def safe_format_currency_us(x):
        try:
            return format_currency(float(x), currency="USD", locale="en_US") if pd.notna(x) and x != "" else x
        except (ValueError, TypeError, decimal.InvalidOperation):
            return x

    def safe_format_currency_sar(x):
        try:
            return format_currency(float(x), currency="SAR", locale="en_US") if pd.notna(x) and x != "" else x
        except (ValueError, TypeError, decimal.InvalidOperation):
            return x


def excluded_included_compliance(file:pd.DataFrame, rb:str, **kwargs):
    included = {}
    excluded = {}

    new_file = file.to_dict(orient='index')

    for ident, data in new_file.items():
        if str(data[rb]).strip().upper() == 'FAIL':
            excluded[ident] = data
        elif str(data[rb]).strip().upper() == 'PASS':
            included[ident] = data

    if not included:
        included['Inclusion'] = {'Comments': 'No companies in Inclusion list'}

    if not excluded:
        excluded['Exclusion'] = {'Comments': 'No companies in Exclusion list'}

    included_df = pd.DataFrame.from_dict(included, orient='index')
    excluded_df = pd.DataFrame.from_dict(excluded, orient='index')

    return included_df, excluded_df





