import datetime
import json
import pandas as pd
import traceback
from abc import ABC, abstractmethod
import io
from pathlib import Path

from myproject.functions import *
from myproject.basicfunctions import *

class Deliveries_Processing:

    def __init__(self, raw_file=None, final_old_file=None, raw_old_file=None, rb=None, delivery_name=None, us_file=None,
                 gcc_file=None, old_file_us=None, old_file_gcc=None):
            if delivery_name not in ['AlBilad', 'AlRajhi', 'Saudi Fransi']:
                self.raw_file = raw_file
                if final_old_file is not None:
                    self.final_old_file = final_old_file
                    self.final_old_file.columns = self.final_old_file.columns.str.strip()
                else:
                    self.final_old_file = None
                self.raw_dict = self.raw_file.to_dict(orient='index')
                self.raw_dict = Functions.fiscal_period_to_period_end_date(new_file=self.raw_dict, file=self.raw_file)
                self.raw_dict = Functions.adjust_date(new_file=self.raw_dict, file=self.raw_file)
                if delivery_name == 'AlRayan GCC':
                    self.raw_dict = Functions.adjust_ratios_alrayan(new_file=self.raw_dict, file=self.raw_file)
                self.old_dict = self.final_old_file.to_dict(orient='index') if self.final_old_file is not None else None
            elif delivery_name == 'AlBilad':
                self.raw_albilad_file = read_new_function(raw_file)
                self.raw_albilad_file.columns = self.raw_albilad_file.columns.str.strip()
                date_current_price = Finders.date_current_price_finder(self.raw_albilad_file)
                self.raw_albilad_file = self.raw_albilad_file.rename(columns={date_current_price: 'Date Of Current Price'})
                self.raw_albilad_file['Name'] = self.raw_albilad_file['Name'].str.strip()
                self.raw_albilad_dict = self.raw_albilad_file.to_dict(orient='index')
                self.blacklist = read_excel_sheet_func(raw_file, 'Blacklisted AAOIFI')
                self.blacklist_dict = self.blacklist.to_dict(orient='index') if self.blacklist is not None else None
                self.raw_albilad_dict = Functions.filter_blacklist_albilad(self.raw_albilad_dict, self.blacklist_dict)
                self.sheet_aaoifi = read_excel_sheet_func(raw_old_file, sheet='AAOIFI')
                self.sheet_albilad = read_excel_sheet_func(raw_old_file, sheet='Albilad')
                self.sheet_albiladpure = read_excel_sheet_func(raw_old_file, sheet='Albilad Pure')
                raw_old_file = read_new_function(raw_old_file)
                old_albilad_file = self.sheet_aaoifi.merge(self.sheet_albilad, how='outer')
                old_file1 = old_albilad_file.merge(self.sheet_albiladpure, how='outer')
                wid_col_old = Finders.index_column(old_file1)
                old_file1 = old_file1.rename(old_file1[wid_col_old])
                old_file1 = old_file1.drop_duplicates(keep='first')
                self.old_albilad_file = old_file1
                self.old_albilad_dict = old_file1.to_dict(orient='index')
            elif delivery_name == 'Saudi Fransi':
                self.raw_file = read_new_function(raw_file) if raw_file is not None else None
                self.raw_file.columns = self.raw_file.columns.str.strip()
                self.raw_dict = self.raw_file.to_dict(orient='index')
                self.old_file = final_old_file if final_old_file is not None else None
                self.old_dict = self.old_file.to_dict(orient='index') if self.old_file is not None else None

                # self.raw_file_fransi_us = read_new_function(us_file) if us_file is not None else None
                # self.raw_dict_us = self.raw_file_fransi_us.to_dict(orient='index')
                # self.old_file_fransi_us = read_new_function(old_file_us) if old_file_us is not None else None
                # self.old_dict_us = self.old_file_fransi_us.to_dict(orient='index')
                #
                # self.raw_file_fransi_ggc = read_new_function(gcc_file) if gcc_file is not None else None
                # self.raw_dict_gcc = self.raw_file_fransi_ggc.to_dict(orient='index')
                # self.old_file_fransi_ggc = read_new_function(old_file_gcc) if old_file_gcc is not None else None
                # self.old_dict_gcc = self.old_file_fransi_ggc.to_dict(orient='index')

            if raw_old_file is not None:
                self.raw_old_file = raw_old_file
                self.raw_old_file.columns = self.raw_old_file.columns.str.strip()
            else:
                self.raw_old_file = None

            self.raw_old_dict = self.raw_old_file.to_dict(orient='index') if self.raw_old_file is not None else None
            self.delivery_name = delivery_name
            self.rb = rb
            # old_file = sheet_aaoifi.merge(sheet_albilad, how='outer')
            # old_file1 = old_file.merge(sheet_albiladpure_old, how='outer')
            # wid_col_old = Finders.index_column(old_file1)
            # old_file1 = old_file1.rename(old_file1[wid_col_old])
            # old_file1 = old_file1.drop_duplicates(keep='first')
            # old_file_dict = old_file1.to_dict(orient='index')

            # self.wid_column = Finders.index_column(self.raw_file)
            # self.parent_col = Finders.parent_finder(self.raw_file)
            # self.name_col = Finders.name_finder(self.raw_file);
            # self.isin_col = Finders.isin_finder(self.raw_file)
            # self.sedol_col = Finders.sedol_finder(self.raw_file);
            # self.country = Finders.country_finder(self.raw_file)
            # self.ba_cols = Finders.BA_finder(self.raw_file);
            # self.mc_cols = Finders.market_cap_finder(self.raw_file)
            # self.ticker_col = Finders.ticker_finder(self.raw_file);
            # self.dates_cols = Finders.date_finder(self.raw_file)
            # self.price_date = Finders.price_date_finder(self.raw_file);
            # self.Issue_active_col = Finders.issue_active_finder(self.raw_file)
            # self.rev_col = Finders.revenue_finder(self.raw_file);
            # self.fiscal_period_col = Finders.fiscal_period_finder(self.raw_file)


   #  delivery_a = GeneralFunctions.create_delivery_json(
   #      name="Saudi_Delivery",
   #      input_files=2,
   #      filters=[
   #          {"column": "Market Cap", "value": "", "operation": "equals"},
   #          {"column": "Amount", "value": 100, "operation": "greater_than"}
   #      ],
   #      comparisons=[
   #          {"file1_column": "Order_ID", "file2_column": "Order_ID", "operation": "match"}
   #      ],
   #      output_columns=["Order_ID", "Status", "Amount"],
   #      # table_data=[
   #      #     {"Order_ID": 101, "Status": "Pending", "Amount": 150},
   #      #     {"Order_ID": 102, "Status": "Completed", "Amount": 200},
   #      #     {"Order_ID": 103, "Status": "Pending", "Amount": 75}
   #      # ]
   #  )
   #
   #
   #  # Combine deliveries into a final JSON structure
   #  final_json = {
   #      "deliveries": [delivery_a]
   #  }
   #
   #  # Convert to JSON string
   #  json_output = json.dumps(final_json, indent=4)
   #  print(json_output)
   #
   # with open("deliveries_config.json", "w") as f:
   #      f.write(json_output)

    # def functions_applications(self, function_map, filters, delivery_config):
    #     universe_df = pd.DataFrame
    #     excluded_df = pd.DataFrame
    #     for filter in filters:
    #         if filter:
    #             for func_config in delivery_config["functions"]:
    #                 func_name = func_config["name"]
    #                 if func_name in function_map:
    #                     universe, excluded = function_map[func_name](self.raw_dict, func_config["args"], self.raw_file)
    #                     universe_df = pd.DataFrame.from_dict(universe, orient="index")
    #                     excluded_df = pd.DataFrame.from_dict(excluded, orient="index")
    #
    #     return universe_df, excluded_df
    # function_map, filters,
    # def functions_applications(self, delivery_config):
    #
    #     current_universe = self.raw_dict.copy()
    #     excluded_dict = {}
    #     excluded_records = []
    #
    #     # for filter_active, func_config in zip(filters, delivery_config["filters"]):
    #     #     if not filter_active:
    #     #         continue
    #     for func_config in delivery_config["filters"]:
    #         # print(func_config)
    #
    #         func_name = func_config["function"]
    #         # if func_name not in function_map:
    #         #     print(f"Warning: Function '{func_name}' not found in function_map")
    #         #     continue
    #         print(func_name)
    #         try:
    #             func = getattr(Functions, func_name)
    #             new_universe, excluded = func(new_file=current_universe, criteria=func_config["args"], file=self.raw_file)
    #
    #             excluded_df = pd.DataFrame.from_dict(excluded, orient='index')
    #             excluded_dict[func_name] = excluded_df
    #             excluded_records.append(excluded_df)
    #
    #             current_universe = new_universe
    #
    #         except Exception as e:
    #             print(f"Error applying {func_name}: {str(e)}")
    #             print(traceback.format_exc())
    #             continue
    #
    #     final_universe_df = pd.DataFrame.from_dict(current_universe, orient='index')
    #
    #     final_excluded_df = pd.concat(excluded_records) if excluded_records else pd.DataFrame()
    #
    #     return final_universe_df, final_excluded_df

    def functions_applications(self, delivery_config):
        if self.delivery_name == 'AlBilad':
            current_universe = self.raw_albilad_dict.copy()
            file = self.raw_albilad_file
        else:
            current_universe = self.raw_dict.copy()
            file = self.raw_file
        excluded_dict = {}
        excluded_records = []

        for func_config in delivery_config["filters"]:
            print(f"filters are {delivery_config['filters']}")
            func_name = func_config["function"]

            try:
                func = getattr(Functions, func_name)
                new_universe, excluded = func(new_file=current_universe, criteria=func_config["args"], file=file, rb=self.rb)

                excluded_df = pd.DataFrame.from_dict(excluded, orient='index')
                excluded_dict[func_name] = excluded_df
                excluded_records.append(excluded_df)

                current_universe = new_universe

            except Exception as e:
                print(f"Error applying {func_name}: {str(e)}")
                print(traceback.format_exc())
                continue

        final_universe_df = pd.DataFrame.from_dict(current_universe, orient='index')

        final_excluded_df = pd.concat(excluded_records) if excluded_records else pd.DataFrame()

        return final_universe_df, final_excluded_df

    def included_and_excluded_error(self, **kwargs):
        # if self.delivery_name == 'AlBilad':
        #     new_file = self.raw_albilad_dict
        #     old_file = self.old_albilad_dict
        # else:
        new_file = self.raw_dict.copy()
        old_file = self.old_dict.copy()
        included_dict = {}
        excluded_dict = {}
        error = []

        for ident, data in new_file.items():
            if ident not in old_file.keys():
                data['Status'] = 'Included'
                included_dict[ident] = data

        for wid, data1 in old_file.items():
            if wid not in new_file.keys():
                data1['Status'] = 'Excluded'
                excluded_dict[wid] = data1

        if not included_dict:
            included_dict['Included'] = {'Comments': 'No Included Companies'}

        if not excluded_dict:
            excluded_dict['Excluded'] = {'Comments': 'No Excluded Companies'}

        if len(included_dict.keys()) / len(old_file.keys()) > 0.1:
            error.append("The increase in the universe is more than 10%")
        if len(excluded_dict.keys()) / len(new_file.keys()) > 0.1:
            error.append("The decrease in the universe is more than 10%")

        included_df = pd.DataFrame.from_dict(included_dict, orient='index')
        excluded_df = pd.DataFrame.from_dict(excluded_dict, orient='index')
        error_df = pd.DataFrame(error, columns=['Comment'])

        return excluded_df, included_df, error_df

    def comparison_rb(self, **kwargs):
        raw = {}
        new_file = self.raw_dict.copy()
        old_file = self.raw_old_dict.copy()
        rb = self.rb

        for ident, data in new_file.items():
            if ident in old_file.keys():
                value = old_file[ident]
                if str(data[rb]).strip().lower() != str(value[rb]).strip().lower():
                    data[f"old_{rb}"] = value[rb]
                    raw[ident] = data
        if len(raw) == 0:
            raw = pd.DataFrame(['No Change in Compliance'], columns=['Comments'])
        else:
            raw = pd.DataFrame.from_dict(raw, orient='index')
        return raw

    def comparison_alrajhi(self, **kwargs):
        raw = {}
        new_file = self.raw_dict
        old_file = self.raw_old_dict
        client=self.delivery_name

        for ident, data in new_file.items():
            if ident in old_file.keys():
                value = old_file[ident]
            # for wid, value in old_file.items():
            #     if ident == wid:
                if str(data['Al-Rajhi']).lower().strip() != str(value['Al-Rajhi']).lower().strip():
                    data['Previous'] = value['Al-Rajhi']
                    if str(data['[Al-Rajhi] NPIN Status']).lower().strip() != str(value['[Al-Rajhi] NPIN Status']).lower().strip():
                        if float(data['[Al-Rajhi] Non Permissible Income Ratio']) > float(value['[Al-Rajhi] Non Permissible Income Ratio']):
                            data['Comment'] = 'The company became non-compliant due to an increase in the non-permissible income ratio'
                        else:
                            data['Comment'] = 'The company became sahriah compliant due to a decrease in the non-permissible income ratio'

                    elif str(data['[Al-Rajhi] Financial Status']).lower().strip() != str(value['[Al-Rajhi] Financial Status']).lower().strip():
                        if float(data['[Al-Rajhi] Debt Ratio']) > float(value['[Al-Rajhi] Debt Ratio']):
                            data['Comment'] = 'The company became non-compliant due to an increase in the debt ratio'
                        else:
                            data['Comment'] = 'The company became sahriah compliant due to a decrease  in the debt ratio'
                    raw[ident] = data

        if client not in ['AlRajhi GCC']:
            if len(raw) == 0:
                raw['Change Compliance'] = {'Comment': 'No Change in Compliance'}
            else:
                raw = raw
        raw_df = pd.DataFrame.from_dict(raw, orient='index')

        return raw_df

    def compliance_rb(self, **kwargs):
        universe = {}
        new_file = self.raw_dict.copy()
        rb = self.rb

        for ident, data in new_file.items():
            if str(data[rb]).strip().upper() == 'PASS':
                universe[ident] = data

        universe_df = pd.DataFrame.from_dict(universe, orient='index')

        return universe_df

    def compliance_nbk_mena(self, **kwargs):
        universe = {}
        new_file = self.raw_dict

        for ident, data in new_file.items():
            if data['Nation'] == 'Saudi Arabia' and str(data['ASRHC Saudi']).strip().upper() == 'PASS':
                universe[ident] = data
            elif data['Nation'] != 'Saudi Arabia' and str(data['ASRHC Global']).strip().upper() == 'PASS':
                universe[ident] = data

        universe_df = pd.DataFrame.from_dict(universe, orient='index')
        return universe_df

    def included_comments_nbk(self, cc_dict:dict, **kwargs):
        included_dict = {}
        new_file = self.raw_dict

        for ident, data in new_file.items():
            if ident in cc_dict.keys():
                data['Comments'] = 'CC'
                included_dict[ident] = data
            else:
                data['Comments'] = 'Skipping criteria'
                included_dict[ident] = data
        included = pd.DataFrame.from_dict(included_dict, orient='index')

        return included

    def divide_nations(self, **kwargs):
        saudi = {}
        mena = {}
        new_file = self.raw_dict

        for ident, data in new_file.items():
            if data['Nation'] == 'Saudi Arabia':
                saudi[ident] = data
            else:
                mena[ident] = data

        saudi_df = pd.DataFrame.from_dict(saudi, orient='index')
        mena_df = pd.DataFrame.from_dict(mena, orient='index')

        return saudi_df, mena_df

    def comparison_albilad(self, **kwargs):
        cc_aaoifi = {}
        cc_albilad = {}
        cc_albilad_pure = {}
        file = self.raw_file.copy()
        file2 = self.raw_old_file.copy()
        new_file = self.raw_dict.copy()
        old_file = self.raw_old_dict.copy()

        Albilad = Finders.albilad_finder(file)
        Aaoifi = Finders.aaoifi_albilad_finder(file)
        albiladpure = Finders.albilad_pure_finder(file)

        Albilad_old = Finders.albilad_finder(file2)
        Aaoifi_old = Finders.aaoifi_albilad_finder(file2)
        albiladpure_old = Finders.albilad_pure_finder(file2)

        for ident, data in new_file.items():
            if ident in old_file.keys():
                value = old_file[ident]
                if str(data[Albilad]).strip().lower() != str(value[Albilad_old]).strip().lower():
                    data['Old_Albilad'] = value[Albilad_old]
                    cc_albilad[ident] = data

                if str(data[Aaoifi]).strip().lower() != str(value[Aaoifi_old]).strip().lower():
                    data['Old_AAOIFI'] = value[Aaoifi_old]
                    cc_aaoifi[ident] = data

                if str(data[albiladpure]).strip().lower() != str(value[albiladpure_old]).strip().lower():
                    data['Old_Albilad Pure'] = value[albiladpure_old]
                    cc_albilad_pure[ident] = data

        if len(cc_albilad) == 0:
            cc_albilad['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_albilad = cc_albilad

        if len(cc_aaoifi) == 0:
            cc_aaoifi['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_aaoifi = cc_aaoifi

        if len(cc_albilad_pure) == 0:
            cc_albilad_pure['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_albilad_pure = cc_albilad_pure

        cc_albilad_df = pd.DataFrame.from_dict(cc_albilad, orient='index')
        cc_aaoifi_df = pd.DataFrame.from_dict(cc_aaoifi, orient='index')
        cc_albilad_pure_df = pd.DataFrame.from_dict(cc_albilad_pure, orient='index')

        return cc_albilad_df, cc_aaoifi_df, cc_albilad_pure_df

    def pure_companies(self, **kwargs):
        pure = {}
        new_file = self.raw_dict

        for ident, data in new_file.items():
            if str(data['Purification Status']).strip().upper() == 'PURE':
                pure[ident] = data
            else:
                pass
        pure_df = pd.DataFrame.from_dict(pure, orient='index')

        return pure_df

    def compliance_status_albilad(self, **kwargs):
        AAOIFI = {}
        ALBILAD = {}
        albilad_pure = {}
        file = self.raw_file.copy()
        new_file = self.raw_dict.copy()

        albilad = Finders.albilad_finder(file)
        aaoifi = Finders.aaoifi_albilad_finder(file)
        albiladpure = Finders.albilad_pure_finder(file)

        for ident, data in new_file.items():
            if str(data[aaoifi]).strip().upper() == 'PASS':
                AAOIFI[ident] = data

            if str(data[albilad]).strip().upper() == 'PASS':
                ALBILAD[ident] = data

            if str(data[albiladpure]).strip().upper() == 'PASS':
                albilad_pure[ident] = data

        AAOIFI_df = pd.DataFrame.from_dict(AAOIFI, orient='index')
        ALBILAD_df = pd.DataFrame.from_dict(ALBILAD, orient='index')
        albilad_pure_df = pd.DataFrame.from_dict(albilad_pure, orient='index')

        return AAOIFI_df, ALBILAD_df, albilad_pure_df

    def comparison_fransi(self, **kwargs):
        raw = {}
        new_file = self.raw_dict
        old_file = self.old_dict

        for ident, data in new_file.items():
            if ident in old_file.keys():
                value = old_file[ident]
                if data['FransiCapital'] != value['FransiCapital']:
                    data['Old_FransiCapital'] = value['FransiCapital']

                    if float(data['[FransiCapital] Debt']) > 0.3333 or float(value['[FransiCapital] Debt']) > 0.3333:
                        if float(data['[FransiCapital] Debt']) > float(value['[FransiCapital] Debt']):
                            data['Comment'] = 'The company\'s Debt ratio increased'
                        elif float(data['[FransiCapital] Debt']) < float(value['[FransiCapital] Debt']):
                            data['Comment'] = 'The company\'s Debt ratio decreased'
                        elif (data['[FransiCapital] Debt'] == "" or data['[FransiCapital] Debt'] == np.nan) and (
                                value['[FransiCapital] Debt'] != "" or value['[FransiCapital] Debt'] != np.nan):
                            data['Comment'] = 'Debt ratio is null'
                        elif (data['[FransiCapital] Debt'] != "" or data['[FransiCapital] Debt'] != np.nan) and (
                                value['[FransiCapital] Debt'] == "" or value['[FransiCapital] Debt'] == np.nan):
                            data['Comment'] = 'Debt ratio is null in old file'
                    elif float(data['[FransiCapital] Interest bearing Investments']) > 0.3333 or float(value[
                                                                                                           '[FransiCapital] Interest bearing Investments']) > 0.3333:
                        if float(data['[FransiCapital] Interest bearing Investments']) > float(value[
                                                                                                   '[FransiCapital] Interest bearing Investments']):
                            data['Comment'] = 'The company\'s Interest bearing Investments ratio increased'
                        elif float(data['[FransiCapital] Interest bearing Investments']) < float(value[
                                                                                                     '[FransiCapital] Interest bearing Investments']):
                            data['Comment'] = 'The company\'s Interest-bearing Investments ratio decreased'
                        elif (data['[FransiCapital] Interest bearing Investments'] == "" or data[
                            '[FransiCapital] Interest bearing Investments'] == np.nan) and (
                                value['[FransiCapital] Interest bearing Investments'] != "" or value[
                            '[FransiCapital] Interest bearing Investments'] != np.nan):
                            data['Comment'] = 'Interest-bearing Investments ratio is null'
                        elif (data['[FransiCapital] Interest bearing Investments'] != "" or data[
                            '[FransiCapital] Interest bearing Investments'] != np.nan) and (
                                value['[FransiCapital] Interest bearing Investments'] == "" or value[
                            '[FransiCapital] Interest bearing Investments'] == np.nan):
                            data['Comment'] = 'Interest bearing Investments ratio is null in old file'
                    else:
                        if float(data['[FransiCapital] Non permissible Income']) > float(value[
                                                                                             '[FransiCapital] Non permissible Income']):
                            data['Comment'] = 'The company\'s Non permissible income ratio increased'
                        elif float(data['[FransiCapital] Non permissible Income']) < float(value[
                                                                                               '[FransiCapital] Non permissible Income']):
                            data['Comment'] = 'The company\'s Non permissible income ratio decreased'
                        elif (data['[FransiCapital] Non permissible Income'] == "" or data[
                            '[FransiCapital] Non permissible Income'] == np.nan) and (
                                value['[FransiCapital] Non permissible Income'] != "" or value[
                            '[FransiCapital] Non permissible Income'] != np.nan):
                            data['Comment'] = 'Non permissible Income is null'
                        elif (data['[FransiCapital] Non permissible Income'] != "" or data[
                            '[FransiCapital] Non permissible Income'] != np.nan) and (
                                value['[FransiCapital] Non permissible Income'] == "" or value[
                            '[FransiCapital] Non permissible Income'] == np.nan):
                            data['Comment'] = 'Non permissible Income is null in old file'

                    raw[ident] = data

        if len(raw) == 0:
            raw = pd.DataFrame(['No Change in Compliance'], columns=['Comment'])
        else:
            raw = pd.DataFrame.from_dict(raw, orient='index')
        return raw

    def alrayan_purif(self, **kwargs):
        new_file = self.raw_dict
        file = self.raw_file
        non_income = Finders.nonperm_income_finder(file)

        for ident, data in new_file.items():
            if str(data[non_income]).strip() == '' or data[non_income] == np.nan or pd.notna(data[non_income]) == False:
                data[non_income] = 0
                data['[Al Rayan] Purification Factor'] = (1 - float(data[non_income])) * 100
            else:
                data[non_income] = data[non_income]
                data['[Al Rayan] Purification Factor'] = (1 - float(data[non_income])) * 100

            if float(data['[Al Rayan] Purification Factor']) < 90.0:
                data['[Al Rayan] Purification Factor'] = 0

        universe = pd.DataFrame.from_dict(new_file, orient='index')

        return universe

    def purification_ratio_check(self, **kwargs):
        universe = {}
        new_file = self.raw_dict
        file = self.raw_file

        purific_ratios = Finders.purification_finder(file)

        for ident, data in new_file.items():
            if str(data[purific_ratios[0]]) in ['', " ", np.nan] and str(data[purific_ratios[1]]) in ['', " ", np.nan] \
                    and str(data[purific_ratios[2]]) in ['', " ", np.nan]:
                pass
            else:
                if float(data[purific_ratios[0]]) < 0:
                    universe[ident] = data
                elif float(data[purific_ratios[1]]) < 0:
                    universe[ident] = data
                elif float(data[purific_ratios[2]]) < 0:
                    universe[ident] = data
                else:
                    pass
        universe_df = pd.DataFrame.from_dict(universe, orient='index')
        return universe_df

    def comparison_albiladgcc(self, **kwargs):
        cc_albilad_oldm = {}
        cc_albilad = {}
        cc_albilad_pure = {}
        cc_albilad_amc = {}
        file = self.raw_file
        file2 = self.raw_old_file
        new_file = self.raw_dict
        old_file_dict = self.raw_old_dict

        albilad = Finders.albilad_finder(file)
        albilad_oldm = Finders.albilad_old_finder(file)
        albiladpure = Finders.albilad_pure_finder(file)
        albilad_amc = Finders.albilad_amc_finder(file)

        albilad_old = Finders.albilad_finder(file2)
        albilad_oldm_old = Finders.albilad_old_finder(file2)
        albiladpure_old = Finders.albilad_pure_finder(file2)
        albilad_amc_old = Finders.albilad_amc_finder(file2)

        for ident, data in new_file.items():
            if ident in old_file_dict.keys():
                value = old_file_dict[ident]
                if str(data[albilad]).strip().lower() != str(value[albilad_old]).strip().lower():
                    data['Old_Albilad'] = value[albilad_old]
                    cc_albilad[ident] = data

                if str(data[albilad_oldm]).strip().lower() != str(value[albilad_oldm_old]).strip().lower():
                    data[f'Old_{albilad_oldm}'] = value[albilad_oldm_old]
                    cc_albilad_oldm[ident] = data

                if str(data[albiladpure]).strip().lower() != str(value[albiladpure_old]).strip().lower():
                    data['Old_Albilad Pure'] = value[albiladpure_old]
                    cc_albilad_pure[ident] = data

                if str(data[albilad_amc]).strip().lower() != str(value[albilad_amc_old]).strip().lower():
                    data[f'Old_{albilad_amc}'] = value[albilad_amc_old]
                    cc_albilad_amc[ident] = data

        if len(cc_albilad) == 0:
            cc_albilad['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_albilad = cc_albilad

        if len(cc_albilad_oldm) == 0:
            cc_albilad_oldm['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_albilad_oldm = cc_albilad_oldm

        if len(cc_albilad_pure) == 0:
            cc_albilad_pure['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_albilad_pure = cc_albilad_pure

        if len(cc_albilad_amc) == 0:
            cc_albilad_amc['Change Compliance'] = {'Comments': 'No Change in Compliance'}
        else:
            cc_albilad_amc = cc_albilad_amc

        cc_albilad_df = pd.DataFrame.from_dict(cc_albilad, orient='index')
        cc_albilad_oldm_df = pd.DataFrame.from_dict(cc_albilad_oldm, orient='index')
        cc_albilad_pure_df = pd.DataFrame.from_dict(cc_albilad_pure, orient='index')
        cc_albilad_amc_df = pd.DataFrame.from_dict(cc_albilad_amc, orient='index')

        return cc_albilad_df, cc_albilad_oldm_df, cc_albilad_pure_df, cc_albilad_amc_df

    def purification_status(self, **kwargs):
        new_file = self.raw_dict

        for ident, data in new_file.items():
            if str(data['Albilad Pure']).lower() == 'pass':
                data['Purification Status'] = 'PURE'
            else:
                if str(data['Albilad']).lower() == 'pass':
                    data['Purification Status'] = 'MIXED'
                else:
                    data['Purification Status'] = 'FAIL'

        file = pd.DataFrame.from_dict(new_file, orient='index')
        return file

    def duplicated_names(self, duplicates:pd.DataFrame, **kwargs):
        excluded = {}
        universe = {}
        new_file = self.raw_dict
        duplicates_dict = duplicates.to_dict(orient='index')

        for ident, data in new_file.items():
            for key, value in duplicates_dict.items():
                if str(data['ISIN']).strip() == str(value['ISIN']).strip():
                    if data['Market Cap'] == value['Market Cap']:
                        if str(data['Introspect']).lower().strip() != str(value['Introspect']).lower().strip():
                            excluded[key] = value
                            excluded[ident] = data
                            universe[key] = value
                        else:
                            universe[key] = value
                    else:
                        excluded[key] = value
                        excluded[ident] = data

        return universe, excluded

    def remove_duplicate_isin(self, **kwargs):

        removed = []
        file = self.raw_file
        wid = Finders.index_column(file)

        universe = file.sort_index(ascending=True)

        blank_isin = universe[universe['ISIN'].isin(['', ' ', np.nan])]

        new_univ = universe.drop_duplicates(subset=['ISIN'], keep='first')
        new_univ = pd.concat([new_univ, blank_isin]).drop_duplicates(keep='first')

        for i in universe.index:
            if i not in new_univ.index.values:
                removed.append(i)

        removed_df = pd.DataFrame(removed, columns=[wid])
        removed_df = removed_df.rename(removed_df[wid]).drop(columns=[wid])
        removed_df = removed_df.join(universe)

        return new_univ, removed_df

    def comparison_introspect(self, **kwargs):
        raw = {}
        new_file = self.raw_dict
        old_file = self.old_dict

        for ident, data in new_file.items():
            if ident in old_file.keys():
                value = old_file[ident]
                if data['Introspect'] != value['Introspect']:
                    data['Old_Introspect'] = value['Introspect']
                    if float(data['[Introspect] Interest-bearing Debts']) > 0.3333 or float(
                            value['[Introspect] Interest-bearing Debts']) > 0.3333:
                        if float(data['[Introspect] Interest-bearing Debts']) > float(
                                value['[Introspect] Interest-bearing Debts']):
                            data['Comment'] = 'The company changed compliance due to an increase in the interest bearing debts ratio'
                        elif float(data['[Introspect] Interest-bearing Debts']) < float(
                                value['[Introspect] Interest-bearing Debts']):
                            data['Comment'] = 'The company changed compliance due to a decrease in the interest bearing debts ratio'
                        elif (data['[Introspect] Interest-bearing Debts'] == "" or data[
                            '[Introspect] Interest-bearing Debts'] == np.nan) and (
                                value['[Introspect] Interest-bearing Debts'] != "" or value[
                            '[Introspect] Interest-bearing Debts'] != np.nan):
                            data['Comment'] = 'Debt Ratio is null'
                        elif (data['[Introspect] Interest-bearing Debts'] != "" or data[
                            '[Introspect] Interest-bearing Debts'] != np.nan) and (
                                value['[Introspect] Interest-bearing Debts'] == "" or value[
                            '[Introspect] Interest-bearing Debts'] == np.nan):
                            data['Comment'] = 'Debt ratio is null in old file'
                    elif float(data['[Introspect] Interest-bearing Investments']) > 0.3333 or float(value[
                                                                                                        '[Introspect] Interest-bearing Investments']) > 0.3333:
                        if float(data['[Introspect] Interest-bearing Investments']) > float(value[
                                                                                                '[Introspect] Interest-bearing Investments']):
                            data['Comment'] = 'The company changed compliance due to an increase in the Interest bearing Investments ratio'
                        elif float(data['[Introspect] Interest-bearing Investments']) < float(value[
                                                                                                  '[Introspect] Interest-bearing Investments']):
                            data['Comment'] = 'The company changed compliance due to a decrease in the Interest bearing Investments ratio'
                        elif (data['[Introspect] Interest-bearing Investments'] == "" or data[
                            '[Introspect] Interest-bearing Investments'] == np.nan) and (
                                value['[Introspect] Interest-bearing Investments'] != "" or value[
                            '[Introspect] Interest-bearing Investments'] != np.nan):
                            data['Comment'] = 'Interest-bearing Investments ratio is null'
                        elif (data['[Introspect] Interest-bearing Investments'] != "" or data[
                            '[Introspect] Interest-bearing Investments'] != np.nan) and (
                                value['[Introspect] Interest-bearing Investments'] == "" or value[
                            '[Introspect] Interest-bearing Investments'] == np.nan):
                            data['Comment'] = 'Interest bearing Investments ratio is null in old file'
                    else:
                        if str(data['[Introspect] NPIN Status']).lower().strip() == 'false':
                            data['Comment'] = 'The company changed compliance due to an increase in Non permissible income ratio'
                        elif str(data['[Introspect] NPIN Status']).lower().strip() == 'true':
                            data['Comment'] = 'The company changed compliance due to a decrease in Non permissible income ratio'

                    raw[ident] = data

        if len(raw) == 0:
            raw['Change Compliance'] = {'Comment': 'No Change in Compliance'}
        else:
            raw = raw

        raw_df = pd.DataFrame.from_dict(raw, orient='index')

        return raw_df

    def compliance_ratio_check_introspect(self, **kwargs):

        excluded = {}
        new_file=self.raw_dict

        for ident, data in new_file.items():
            if (str(data['[Introspect] NPIN Status']).strip() == "" or data[
                '[Introspect] NPIN Status'] == np.nan or pd.notna(
                data['[Introspect] NPIN Status']) == False) and (
                    float(data['[Introspect] Interest-bearing Debts']) <= 0.3333 and float(
                data['[Introspect] Interest-bearing Investments']) <= 0.3333):
                if data['Introspect'] == 'FAIL':
                    excluded[ident] = data
            elif (str(data['[Introspect] NPIN Status']).strip() == "" or data[
                '[Introspect] NPIN Status'] == np.nan or pd.notna(
                data['[Introspect] NPIN Status']) == False) and (
                    float(data['[Introspect] Interest-bearing Debts']) > 0.3333 or float(
                data['[Introspect] Interest-bearing Investments']) > 0.3333):
                if data['Introspect'] == 'PASS':
                    excluded[ident] = data
            elif (str(data['[Introspect] Interest-bearing Debts']).strip() == "" or data[
                '[Introspect] Interest-bearing Debts'] == np.nan) and (str(
                    data['[Introspect] NPIN Status'])) == True and float(
                data['[Introspect] Interest-bearing Investments']) <= 0.3333:  # or pd.notna(
                #             data['[Introspect] Interest-bearing Debts']) == False) and (float(int(
                #             data['[Introspect] Non-permissible Income'])) <= 0.05) and float(
                #             data['[Introspect] Interest-bearing Investments']) <= 0.3333:
                if data['Introspect'] == 'FAIL':
                    excluded[ident] = data
            elif (str(data['[Introspect] Interest-bearing Debts']).strip() == "" or data[
                '[Introspect] Interest-bearing Debts'] == np.nan or pd.notna(
                    data['[Introspect] Interest-bearing Debts']) == False) and (
                    str(data['[Introspect] NPIN Status']) == False or float(
                data['[Introspect] Interest-bearing Investments']) > 0.3333):
                if data['Introspect'] == 'PASS':
                    excluded[ident] = data
            elif (str(data['[Introspect] Interest-bearing Investments']).strip() == "" or data[
                '[Introspect] Interest-bearing Investments'] == np.nan or pd.notna(
                data['[Introspect] Interest-bearing Investments']) == False) and float(
                data['[Introspect] Interest-bearing Debts']) <= 0.3333 and str(
                data['[Introspect] NPIN Status']) == True:
                if data['Introspect'] == 'FAIL':
                    excluded[ident] = data
            elif (str(data['[Introspect] Interest-bearing Investments']).strip() == "" or data[
                '[Introspect] Interest-bearing Investments'] == np.nan or pd.notna(
                data['[Introspect] Interest-bearing Investments']) == False) or float(
                data['[Introspect] Interest-bearing Debts']) > 0.3333 or str(
                data['[Introspect] NPIN Status']) == False:
                if data['Introspect'] == 'PASS':
                    excluded[ident] = data
            elif float(data['[Introspect] Interest-bearing Debts']) <= 0.3333 and str(
                    data['[Introspect] NPIN Status']) == True and float(
                data['[Introspect] Interest-bearing Investments']) <= 0.3333:
                if data['Introspect'] == 'FAIL':
                    excluded[ident] = data
            elif float(data['[Introspect] Interest-bearing Debts']) > 0.3333 or str(
                    data['[Introspect] NPIN Status']) == False or float(
                data['[Introspect] Interest-bearing Investments']) > 0.3333:
                if data['Introspect'] == 'PASS':
                    excluded[ident] = data
            else:
                pass
        excluded_df = pd.DataFrame.from_dict(excluded, orient='index')

        return excluded_df

    # def functions_applications(self, delivery_config):
    #     current_universe = self.raw_dict.copy()
    #     excluded_dict = {}
    #     excluded_records = []
    #
    #     # Create an instance of the Functions class
    #     functions_instance = Functions()  # Add any required initialization parameters if needed
    #     print(f"functions_instance:{Functions}")
    #     for func_config in delivery_config["filters"]:
    #         func_name = func_config["function"]
    #         print(f"Applying function: {func_name}")
    #
    #         try:
    #             # Get the method from the Functions instance
    #             func = getattr(functions_instance, func_name)
    #
    #             # Call the method with the required parameters
    #             new_universe, excluded = func(
    #                 new_file=current_universe,
    #                 criteria=func_config["args"],
    #                 file=self.raw_file
    #             )
    #
    #             # Process excluded items
    #             excluded_df = pd.DataFrame.from_dict(excluded, orient='index')
    #             excluded_dict[func_config] = excluded_df
    #             excluded_records.append(excluded_df)
    #
    #             # Update current universe
    #             current_universe = new_universe
    #
    #         except AttributeError as e:
    #             print(f"Error: Function '{func_name}' not found in Functions class")
    #             print(f"Available methods: {[m for m in dir(functions_instance) if not m.startswith('_')]}")
    #             continue
    #         except Exception as e:
    #             print(f"Error applying {func_name}: {str(e)}")
    #             print(traceback.format_exc())
    #             continue
    #
    #     # Convert results to DataFrames
    #     final_universe_df = pd.DataFrame.from_dict(current_universe, orient='index')
    #     final_excluded_df = pd.concat(excluded_records) if excluded_records else pd.DataFrame()
    #
    #     return final_universe_df, final_excluded_df

    # If the checkbox is checked, apply the market_cap function
    # if apply_market_cap_filter and apply_issue_active_filter:
    #     for func_config in delivery_config["functions"]:
    #         func_name = func_config["name"]
    #         if func_name in FUNCTION_MAP:
    #             try:
    #                 universe, excluded = FUNCTION_MAP[func_name](new_file, func_config["args"], new_df)
    #                 universe_df = pd.DataFrame.from_dict(universe, orient="index")
    #                 excluded_df = pd.DataFrame.from_dict(excluded, orient="index")
    #             except Exception as e:
    #                 st.error(f"Error applying {func_name}: {e}")
    #                 return

# def process_delivery(file_paths, delivery_config):
#     # Load files
#     files = [pd.read_excel(fp) for fp in file_paths]
#
#     # Apply filters
#     for file, filters in zip(files, delivery_config.get("filters", [])):
#         for filter in filters:
#             column = filter["column"]
#             value = filter["value"]
#             operation = filter["operation"]
#             if operation == "equals":
#                 file = file[file[column] == value]
#             elif operation == "greater_than":
#                 file = file[file[column] > value]
#             # Add more operations as needed
#
#     # Apply comparisons
#     comparisons = delivery_config.get("comparisons", [])
#     for comp in comparisons:
#         file1_col = comp["file1_column"]
#         file2_col = comp["file2_column"]
#         operation = comp["operation"]
#         if operation == "match":
#             files[0] = files[0][files[0][file1_col].isin(files[1][file2_col])]
#         elif operation == "greater_than":
#             files[0] = files[0][files[0][file1_col] > files[1][file2_col]]
#         # Add more comparison operations as needed
#
#     # Generate output
#     output_columns = delivery_config.get("output_columns", [])
#     output_file = "output.xlsx"
#     files[0][output_columns].to_excel(output_file, index=False)
#     return output_file



# def prepare_final_output(universe, delivery_config, new_universe_df):
#     """
#     Prepares the final output files based on the delivery configuration.
#
#     Args:
#         universe (dict): Dictionary containing all processed dataframes
#         delivery_config (dict): Delivery configuration dictionary
#         new_universe_df (DataFrame): The processed universe dataframe
#
#     Returns:
#         dict: Dictionary of DataFrames ready for writing to Excel
#     """
#     # Initialize the delivery file dictionary
#     delivery_file = {}
#
#     # Get client configuration
#     client_config = delivery_config["final_client"]
#     client_name = client_config["name"]
#     sheets_mapping = client_config["sheets_names"]
#
#     # Get output columns configuration
#     output_config = delivery_config["output_columns"]
#     output_columns = output_config["columns"]
#     default_values = output_config.get("defaults", {})
#
#     # Prepare each required sheet
#     for sheet_type, source_key in sheets_mapping.items():
#         # Get the source dataframe
#         if source_key in universe:
#             source_df = universe[source_key].copy()
#         else:
#             # Create empty dataframe if source not found
#             source_df = pd.DataFrame(columns=output_columns)
#
#         # Ensure all required columns exist
#         for col in output_columns:
#             if col not in source_df.columns:
#                 source_df[col] = default_values.get(col, None)
#
#         # Add any default columns that weren't in output_columns
#         for col, default_val in default_values.items():
#             if col not in source_df.columns:
#                 source_df[col] = default_val
#
#         # Select only the desired columns (output_columns + defaults)
#         final_columns = list(set(output_columns + list(default_values.keys())))
#         delivery_file[sheet_type] = source_df[final_columns]
#
#     # Special handling for the main "Final" sheet
#     if "Final" in sheets_mapping.values():
#         # Ensure the main universe has all required columns
#         final_df = new_universe_df.copy()
#         for col in output_columns:
#             if col not in final_df.columns:
#                 final_df[col] = default_values.get(col, None)
#
#         # Add defaults
#         for col, default_val in default_values.items():
#             if col not in final_df.columns:
#                 final_df[col] = default_val
#
#         # Find the sheet name that maps to "Final"
#         final_sheet_name = [k for k, v in sheets_mapping.items() if v == "Final"][0]
#         delivery_file[final_sheet_name] = final_df[output_columns + list(default_values.keys())]
#
#     return delivery_file

def resolve_date(value):
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    year = datetime.date.today().year
    if value == "first_day_month":
        return datetime.datetime(int(year), int(next_month), int(1)).strftime('%d-%m-%Y')
    #     # return new_value
    # else:
    #     new_value = "Value is None"
    return value
#
#
# def resolve_sheet_name(client_name: str, template: str) -> str:
#     """
#     Resolve dynamic sheet names with {month} and {year} placeholders
#     Example: Shariah_List_01{month}{year} -> Shariah_List_01052025
#     """
#     today = datetime.date.today()
#     current_month = today.month
#     current_year = today.year
#
#     # Use next month if we're past the 15th of current month
#
#     next_month = today + dateutil.relativedelta.relativedelta(months=1)
#     month = next_month.month
#     year = next_month.year
#     month_l = datetime.datetime(year, int(current_month)+1, 1).strftime("%B")
#
#     # Format as two digits (05 for May)
#     month_str = f"{month:02d}"
#     year_str = str(year)
#     month_letters = str(month_l)
#
#     return template.format(month=month_str, year=year_str, month_l=month_letters)


# def prepare_final_output(universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
#     """
#     Prepare final output files with proper sheet naming and column handling
#     """
#     delivery_file = {}
#     client_config = delivery_config["final_client"]
#     output_config = delivery_config["output_columns"]
#
#     # Handle Shariah List case
#     if client_config["name"] == "Shariah_List":
#         sheet_template = client_config["sheets_names"][0]
#         sheet_name = resolve_sheet_name(client_config["name"], sheet_template)
#
#         # Get columns and defaults
#         columns = output_config.get(sheet_template, [])
#         defaults = output_config.get("defaults", {})
#
#         # Prepare output dataframe
#         output_df = new_universe_df.copy()
#
#         # Apply defaults for missing columns
#         for col in columns:
#             if col not in output_df.columns:
#                 output_df[col] = defaults.get(col, None)
#
#         # Handle special date formatting
#         if "Date" in defaults and defaults["Date"] == "first_day_month":
#             today = datetime.date.today()
#             output_df["Date"] = today.replace(day=1).strftime('%Y-%m-%d')
#
#         # Apply fixed defaults
#         if "Weight" in defaults:
#             output_df["Weight"] = defaults["Weight"]
#         if "Code" in defaults:
#             output_df["Code"] = defaults["Code"]
#
#         # Select and order columns
#         final_columns = [col for col in columns if col in output_df.columns]
#         output_df = output_df[final_columns]
#
#         delivery_file[sheet_name] = output_df
#
#     # Handle other cases (Inclusion/Exclusion lists)
#     else:
#         sheets_mapping = client_config["sheets_names"]
#         for sheet_name, source_key in sheets_mapping.items():
#             # Get source data
#             source_df = new_universe_df if source_key == "Final" else universe.get(source_key, pd.DataFrame())
#
#             # Get required columns
#             required_columns = output_config.get(sheet_name, [])
#             defaults = output_config.get("defaults", {})
#
#             # Prepare output
#             if len(source_df) == 1 and 'Comments' in source_df.columns:
#                 output_df = pd.DataFrame({
#                     'Comments': [source_df.iloc[0]['Comments']]
#                 })
#             else:
#                 output_df = pd.DataFrame()
#                 for col in required_columns + list(defaults.keys()):
#                     if col in source_df.columns:
#                         output_df[col] = source_df[col]
#                     elif col in defaults:
#                         output_df[col] = defaults[col]
#
#             delivery_file[sheet_name] = output_df
#
#     return delivery_file

# def prepare_final_output(universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
#     """
#     Prepare final output files with:
#     - Dynamic sheet naming (Shariah_List_01052025)
#     - Proper date handling (first_day_month)
#     - Empty list comments
#     - Default values
#     - Sorting by specified columns
#     """
#     delivery_file = {}
#     client_config = delivery_config["final_client"]
#     output_config = delivery_config["output_columns"]
#
#     # Get sorting column if specified
#     sort_column = output_config.get("sort")
#     ascending = output_config.get("ascending", True)
#
#     # Handle Shariah List case
#     # if client_config["name"] == "Shariah_List":
#     if len(client_config['sheets_names']) == 1:
#         sheet_template = client_config["sheets_names"][0]
#         sheet_name = resolve_sheet_name(client_config["sheets_names"], sheet_template)
#
#         columns = output_config.get(sheet_template, [])
#         defaults = output_config.get("defaults", {})
#
#         output_df = new_universe_df.copy()
#
#         # Apply defaults and special values
#         for col in columns:
#             print(col, columns)
#             if col not in output_df.columns:
#                 print(f"columns of output are : {output_df.columns}")
#                 # print(f"if col not in output_df.columns: {col}, {output_df.columns}")
#                 if col in defaults:
#                     # print(f"if col in defaults: {col}, {defaults}")
#                     output_df[col] = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                 else:
#                     output_df[col] = None
#
#         # Apply sorting if specified and column exists
#         if sort_column and sort_column in output_df.columns:
#             output_df = output_df.sort_values(by=sort_column, ascending=ascending, key=lambda col: col.str.lower())
#
#         # Ensure correct column order
#         output_df = output_df[columns]
#         delivery_file[sheet_name] = output_df
#
#     # Handle other cases (Inclusion/Exclusion)
#     else:
#         sheets_mapping = client_config["sheets_names"]
#         print(f"sheet mapping is {sheets_mapping}")
#         for sheet_name, source_key in sheets_mapping.items():
#             source_df = new_universe_df if source_key == "Final" else universe.get(source_key, pd.DataFrame())
#
#             required_columns = output_config.get(sheet_name, [])
#             defaults = output_config.get("defaults", {})
#
#             # Handle empty lists with comments
#             if len(source_df) == 1 and 'Comments' in source_df.columns:
#                 output_df = pd.DataFrame({'Comments': [source_df.iloc[0]['Comments']]})
#             else:
#                 output_df = pd.DataFrame()
#                 for col in required_columns + list(defaults.keys()):
#                     if type(col) == str:
#                         if col in source_df.columns:
#                             output_df[col] = source_df[col]
#                         elif col in defaults:
#                             output_df[col] = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                     else:
#                         for key, value in col.items():
#                             output_df[key] = source_df[value]
#
#                 # Apply sorting if specified and column exists
#                 if sort_column and sort_column in output_df.columns:
#                     output_df = output_df.sort_values(by=sort_column, ascending=ascending, key=lambda col: col.str.lower())
#
#             delivery_file[sheet_name] = output_df
#
#     return delivery_file


# def prepare_final_output(universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
#     """
#     Prepare final output files with:
#     - Dynamic sheet naming (Shariah_List_01052025)
#     - Proper date handling (first_day_month)
#     - Empty list comments
#     - Default values
#     - Sorting by specified columns
#     - Complex column mappings
#     """
#     delivery_file = {}
#     client_config = delivery_config["final_client"]
#     output_config = delivery_config["output_columns"]
#
#     # Get sorting configuration
#     sort_column = output_config.get("sort")
#     ascending = output_config.get("ascending", True)
#
#     # Handle single sheet case (like Riyad Capital, Derayah ASRHC)
#     if isinstance(client_config["sheets_names"], list):
#         sheet_template = client_config["sheets_names"][0]
#         sheet_name = resolve_sheet_name(sheet_template)  # Implement this function to handle {month}, {year} etc.
#
#         columns = output_config.get(sheet_template, [])
#         defaults = output_config.get("defaults", {})
#
#         output_df = new_universe_df.copy()
#
#         # Process each required column
#         processed_columns = []
#         for col in columns:
#             if isinstance(col, str):
#                 if col in output_df.columns:
#                     processed_columns.append(col)
#                 elif col in defaults:
#                     default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                     output_df[col] = default_value
#                     processed_columns.append(col)
#                 else:
#                     # Column not in dataframe or defaults - add as empty
#                     output_df[col] = None
#                     processed_columns.append(col)
#             elif isinstance(col, dict):
#                 # Handle complex column mappings (like {"[Derraya] Debt Ratio": "[Derraya Proposed] Debt Ratio"})
#                 for new_col, source_col in col.items():
#                     if source_col in output_df.columns:
#                         output_df[new_col] = output_df[source_col]
#                         processed_columns.append(new_col)
#                     else:
#                         output_df[new_col] = None
#                         processed_columns.append(new_col)
#
#         # Apply sorting if specified
#         if sort_column and sort_column in output_df.columns:
#             output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                               key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#         # Ensure correct column order
#         output_df = output_df[processed_columns]
#         delivery_file[sheet_name] = output_df
#
#     # Handle multiple sheet case (like ADIB, BMK)
#     elif isinstance(client_config["sheets_names"], dict):
#         sheets_mapping = client_config["sheets_names"]
#         # sheet_mapping = {}
#         for sheet_name, source_key in sheets_mapping.items():
#             # Get source data - either from filtered universe or specific sheets
#             if source_key == "Final":
#                 source_df = new_universe_df
#             else:
#                 source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_name, [])))
#
#             # Handle empty DataFrames with comments
#             if len(source_df) == 1 and 'Comments' in source_df.columns:
#                 delivery_file[sheet_name] = source_df
#                 continue
#
#             required_columns = output_config.get(sheet_name, [])
#             defaults = output_config.get("defaults", {})
#
#             output_df = pd.DataFrame()
#
#             # Process each required column
#             for col in required_columns:
#                 if isinstance(col, str):
#                     if col in source_df.columns:
#                         output_df[col] = source_df[col]
#                     elif col in defaults:
#                         default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                         output_df[col] = default_value
#                     else:
#                         output_df[col] = None
#                 elif isinstance(col, dict):
#                     # Handle column mappings
#                     for new_col, source_col in col.items():
#                         if source_col in source_df.columns:
#                             output_df[new_col] = source_df[source_col]
#                         else:
#                             output_df[new_col] = None
#
#             # Apply sorting if specified
#             if sort_column and sort_column in output_df.columns:
#                 output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                                   key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#             delivery_file[sheet_name] = output_df
#
#     return delivery_file

SHEET_MAPPING = {
    "Included": "Included",
    "Excluded": "Excluded",
    # "Inclusion": "Included CC",
    # "Included CC": "Inclusion",
    # "Exclusion": "Excluded CC",
    # "Excluded CC": "Exclusion",
    # "Change Compliance": "CC",
    "Change Compliance": "Change Compliance",
    "Inclusion": "Inclusion",
    "Exclusion": "Exclusion",
    "AAOIFI": "AAOIFI",
    "Albilad": "Albilad",
    "Albilad Pure": "Albilad Pure",
    "Change Compliance Albilad": "Change Compliance_Albilad",
    # "Change Compliance_Albilad": "Change Compliance Albilad",
    "Change Compliance Albilad Pure": "Change Compliance_Albilad Pure",
    # "Change Compliance_Albilad Pure": "Change Compliance Albilad Pure",
    "AlRajhi Pure List": "AlRajhi Pure List",
    "Saudi (SAR)": "Saudi (SAR)",
    'Saudi (USD)': 'Saudi (USD)',
    'GCC ex-Saudi': 'GCC ex-Saudi',
    'Albilad-Albilad Old':'Albilad-Albilad Old',
    'Max (Assets-Market Cap)': 'Max (Assets-Market Cap)'
}


# def prepare_final_output(universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
#     """
#     Prepare final output files where:
#     - sheets_names is always a list in JSON config
#     - First sheet always uses "Final" source and has its name resolved
#     - Subsequent sheets use sources from SHEET_MAPPING
#     """
#     delivery_file = {}
#     client_config = delivery_config["final_client"]
#     output_config = delivery_config["output_columns"]
#
#     # Get sorting configuration
#     sort_column = output_config.get("sort")
#     ascending = output_config.get("ascending", True)
#
#     # sheets_names is now always a list in the config
#     sheet_templates = client_config["sheets_names"]  # These may contain placeholders
#
#     # Process first sheet - always uses "Final"
#     first_sheet_template = sheet_templates[0]
#     first_sheet_name = resolve_sheet_name(first_sheet_template)  # Resolve dynamic name
#     first_sheet_columns = output_config.get(first_sheet_template, [])  # Use original template as key
#
#     # Create output DataFrame for first sheet
#     output_df = new_universe_df.copy()
#
#     # Process columns with defaults and mappings
#     processed_columns = []
#     for col in first_sheet_columns:
#         if isinstance(col, str):
#             if col in output_df.columns:
#                 processed_columns.append(col)
#             elif col in output_config.get("defaults", {}):
#                 default_value = resolve_date(output_config["defaults"][col]) if col == "Date" else \
#                     output_config["defaults"][col]
#                 output_df[col] = default_value
#                 processed_columns.append(col)
#             else:
#                 output_df[col] = None
#                 processed_columns.append(col)
#         elif isinstance(col, dict):
#             for new_col, source_col in col.items():
#                 if source_col in output_df.columns:
#                     output_df[new_col] = output_df[source_col]
#                     processed_columns.append(new_col)
#                 else:
#                     output_df[new_col] = None
#                     processed_columns.append(new_col)
#
#     # Apply sorting
#     if sort_column and sort_column in output_df.columns:
#         output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                           key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#     # Ensure correct column order
#     output_df = output_df[processed_columns]
#     delivery_file[first_sheet_name] = output_df  # Use resolved name as key
#
#     # Process remaining sheets using the mapping
#     for sheet_template in sheet_templates[1:]:
#         # Resolve sheet name (in case it has placeholders)
#         sheet_name = resolve_sheet_name(sheet_template)
#
#         # Get source from mapping, default to sheet_name if not found
#         source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
#         source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
#
#         # Handle empty DataFrames with comments
#         if len(source_df) == 1 and 'Comments' in source_df.columns:
#             delivery_file[sheet_name] = source_df
#             continue
#
#         required_columns = output_config.get(sheet_template, [])
#         defaults = output_config.get("defaults", {})
#
#         output_df = pd.DataFrame()
#
#         # Process each required column
#         for col in required_columns:
#             if isinstance(col, str):
#                 if col in source_df.columns:
#                     output_df[col] = source_df[col]
#                 elif col in defaults:
#                     default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                     output_df[col] = default_value
#                 else:
#                     output_df[col] = None
#             elif isinstance(col, dict):
#                 for new_col, source_col in col.items():
#                     if source_col in source_df.columns:
#                         output_df[new_col] = source_df[source_col]
#                     else:
#                         output_df[new_col] = None
#
#         # Apply sorting if specified
#         if sort_column and sort_column in output_df.columns:
#             output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                               key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#         delivery_file[sheet_name] = output_df
#
#     return delivery_file


# def resolve_sheet_name(template: str) -> str:
#     """
#     Resolve dynamic sheet names with placeholders like {month}, {year}, {month_l}
#     """
#     # Get current date info
#     now = datetime.datetime.now()
#     today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
#     next_month = f'{today.month:02d}'
#     month = datetime.datetime(2023, int(next_month), 1).strftime("%m")
#     # month = now.strftime("%m")
#     year = now.strftime("%Y")
#     month_l = datetime.datetime(2023, int(next_month), 1).strftime("%B")  # Full month name
#     quarter = (now.month - 1) // 3 + 1  # Current quarter (1-4)
#
#     # Replace placeholders
#     sheet_name = template
#     sheet_name = sheet_name.replace("{month}", month)
#     sheet_name = sheet_name.replace("{year}", year)
#     sheet_name = sheet_name.replace("{month_l}", month_l)
#     sheet_name = sheet_name.replace("{quarter}", str(quarter))
#
#     return sheet_name


# class DeliveryStrategy(ABC):
#     """Base strategy interface for all delivery processors"""
#
#     @abstractmethod
#     def prepare_output(self, universe: dict, config: dict, new_universe_df: pd.DataFrame) -> dict:
#         pass
#
#
# class DefaultDeliveryStrategy(DeliveryStrategy):
#     """Default strategy for most deliveries"""

    # def prepare_output(self, universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
    #     # delivery_file = {}
    #     # client_config = config["final_client"]
    #     # output_config = config["output_columns"]
    #     #
    #     # for sheet_template in client_config["sheets_names"]:
    #     #     sheet_name = self._resolve_sheet_name(sheet_template)
    #     #     source_df = universe.get(SHEET_MAPPING.get(sheet_name, sheet_name), new_universe_df)
    #     #
    #     #     delivery_file[sheet_name] = self._process_sheet(
    #     #         source_df,
    #     #         output_config.get(sheet_template, []),
    #     #         output_config.get("defaults", {}),
    #     #         output_config.get("sort"),
    #     #         output_config.get("ascending", True)
    #     #     )
    #     #
    #     # return delivery_file
    #     """
    #    #     Prepare final output files where:
    #    #     - sheets_names is always a list in JSON config
    #    #     - First sheet always uses "Final" source and has its name resolved
    #    #     - Subsequent sheets use sources from SHEET_MAPPING
    #    #     """
    #
    #     delivery_file = {}
    #     client_config = delivery_config["final_client"]
    #     output_config = delivery_config["output_columns"]
    #
    #     # Get sorting configuration
    #     sort_column = output_config.get("sort")
    #     ascending = output_config.get("ascending", True)
    #
    #     # sheets_names is now always a list in the config
    #     sheet_templates = client_config["sheets_names"]  # These may contain placeholders
    #
    #     is_albilad = delivery_config.get('name') == 'Albilad'
    #
    #     # Process first sheet - always uses "Final"
    #     first_sheet_template = sheet_templates[0]
    #     first_sheet_name = self._resolve_sheet_name(first_sheet_template)  # Resolve dynamic name
    #     first_sheet_columns = output_config.get(first_sheet_template, [])  # Use original template as key
    #
    #     # Create output DataFrame for first sheet
    #     output_df = new_universe_df.copy()
    #
    #     # Process columns with defaults and mappings
    #     processed_columns = []
    #     if not is_albilad:
    #         for col in first_sheet_columns:
    #             if isinstance(col, str):
    #                 if col in output_df.columns:
    #                     processed_columns.append(col)
    #                 elif col in output_config.get("defaults", {}):
    #                     default_value = resolve_date(output_config["defaults"][col]) if col == "Date" else \
    #                         output_config["defaults"][col]
    #                     output_df[col] = default_value
    #                     processed_columns.append(col)
    #                 else:
    #                     output_df[col] = None
    #                     processed_columns.append(col)
    #             elif isinstance(col, dict):
    #                 for new_col, source_col in col.items():
    #                     if source_col in output_df.columns:
    #                         output_df[new_col] = output_df[source_col]
    #                         processed_columns.append(new_col)
    #                     else:
    #                         output_df[new_col] = None
    #                         processed_columns.append(new_col)
    #
    #     # Apply sorting
    #     if sort_column and sort_column in output_df.columns:
    #         output_df = output_df.sort_values(by=sort_column, ascending=ascending,
    #                                           key=lambda col: col.str.lower() if col.dtype == 'object' else col)
    #
    #     # Ensure correct column order
    #     output_df = output_df[processed_columns]
    #     delivery_file[first_sheet_name] = output_df  # Use resolved name as key
    #
    #     # Process remaining sheets using the mapping
    #     for sheet_template in sheet_templates[1:]:
    #         # Resolve sheet name (in case it has placeholders)
    #         sheet_name = self._resolve_sheet_name(sheet_template)
    #
    #         # Get source from mapping, default to sheet_name if not found
    #         source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
    #         source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
    #
    #         # Handle empty DataFrames with comments
    #         if len(source_df) == 1 and 'Comments' in source_df.columns:
    #             delivery_file[sheet_name] = source_df
    #             continue
    #
    #         required_columns = output_config.get(sheet_template, [])
    #         defaults = output_config.get("defaults", {})
    #
    #         output_df = pd.DataFrame()
    #
    #         # Process each required column
    #         for col in required_columns:
    #             if isinstance(col, str):
    #                 if col in source_df.columns:
    #                     output_df[col] = source_df[col]
    #                 elif col in defaults:
    #                     default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
    #                     output_df[col] = default_value
    #                 else:
    #                     output_df[col] = None
    #             elif isinstance(col, dict):
    #                 for new_col, source_col in col.items():
    #                     if source_col in source_df.columns:
    #                         output_df[new_col] = source_df[source_col]
    #                     else:
    #                         output_df[new_col] = None
    #
    #         # Apply sorting if specified
    #         if sort_column and sort_column in output_df.columns:
    #             output_df = output_df.sort_values(by=sort_column, ascending=ascending,
    #                                               key=lambda col: col.str.lower() if col.dtype == 'object' else col)
    #
    #         delivery_file[sheet_name] = output_df
    #
    #     return delivery_file

    # def prepare_output(self, universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
    #     delivery_file = {}
    #     client_config = delivery_config["final_client"]
    #     output_config = delivery_config["output_columns"]
    #
    #     # Get sorting configuration
    #     sort_column = output_config.get("sort")
    #     ascending = output_config.get("ascending", True)
    #
    #     # sheets_names is now always a list in the config
    #     sheet_templates = client_config["sheets_names"]  # These may contain placeholders
    #
    #     # Check if this is Albilad special case
    #     is_albilad = delivery_config.get('name') == 'AlBilad'
    #
    #     if is_albilad:
    #         # For Albilad, process ALL sheets with the same logic
    #         for sheet_template in sheet_templates:
    #             sheet_name = self._resolve_sheet_name(sheet_template)
    #             source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
    #             source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
    #
    #             # Handle empty DataFrames with comments
    #             if len(source_df) == 1 and 'Comments' in source_df.columns:
    #                 delivery_file[sheet_name] = source_df
    #                 continue
    #
    #             required_columns = output_config.get(sheet_template, [])
    #             defaults = output_config.get("defaults", {})
    #
    #             output_df = pd.DataFrame()
    #
    #             # Process each required column
    #             for col in required_columns:
    #                 if isinstance(col, str):
    #                     if col in source_df.columns:
    #                         output_df[col] = source_df[col]
    #                     elif col in defaults:
    #                         default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
    #                         output_df[col] = default_value
    #                     else:
    #                         output_df[col] = None
    #                 elif isinstance(col, dict):
    #                     for new_col, source_col in col.items():
    #                         if source_col in source_df.columns:
    #                             output_df[new_col] = source_df[source_col]
    #                         else:
    #                             output_df[new_col] = None
    #
    #             # Apply sorting if specified
    #             if sort_column and sort_column in output_df.columns:
    #                 output_df = output_df.sort_values(by=sort_column, ascending=ascending,
    #                                                   key=lambda col: col.str.lower() if col.dtype == 'object' else col)
    #
    #             delivery_file[sheet_name] = output_df
    #
    #     else:
    #         # Original logic for non-Albilad clients
    #         # Process first sheet - always uses "Final"
    #         first_sheet_template = sheet_templates[0]
    #         first_sheet_name = self._resolve_sheet_name(first_sheet_template)
    #         first_sheet_columns = output_config.get(first_sheet_template, [])
    #
    #         # Create output DataFrame for first sheet
    #         output_df = new_universe_df.copy()
    #
    #         # Process columns with defaults and mappings
    #         processed_columns = []
    #         for col in first_sheet_columns:
    #             if isinstance(col, str):
    #                 if col in output_df.columns:
    #                     processed_columns.append(col)
    #                 elif col in output_config.get("defaults", {}):
    #                     default_value = resolve_date(output_config["defaults"][col]) if col == "Date" else \
    #                         output_config["defaults"][col]
    #                     output_df[col] = default_value
    #                     processed_columns.append(col)
    #                 else:
    #                     output_df[col] = None
    #                     processed_columns.append(col)
    #             elif isinstance(col, dict):
    #                 for new_col, source_col in col.items():
    #                     if source_col in output_df.columns:
    #                         output_df[new_col] = output_df[source_col]
    #                         processed_columns.append(new_col)
    #                     else:
    #                         output_df[new_col] = None
    #                         processed_columns.append(new_col)
    #
    #         # Apply sorting
    #         if sort_column and sort_column in output_df.columns:
    #             output_df = output_df.sort_values(by=sort_column, ascending=ascending,
    #                                               key=lambda col: col.str.lower() if col.dtype == 'object' else col)
    #
    #         # Ensure correct column order
    #         output_df = output_df[processed_columns]
    #         delivery_file[first_sheet_name] = output_df
    #
    #         # Process remaining sheets using the mapping
    #         for sheet_template in sheet_templates[1:]:
    #             sheet_name = self._resolve_sheet_name(sheet_template)
    #             source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
    #             source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
    #
    #             # Handle empty DataFrames with comments
    #             if len(source_df) == 1 and 'Comments' in source_df.columns:
    #                 delivery_file[sheet_name] = source_df
    #                 continue
    #
    #             required_columns = output_config.get(sheet_template, [])
    #             defaults = output_config.get("defaults", {})
    #
    #             output_df = pd.DataFrame()
    #
    #             # Process each required column
    #             for col in required_columns:
    #                 if isinstance(col, str):
    #                     if col in source_df.columns:
    #                         output_df[col] = source_df[col]
    #                     elif col in defaults:
    #                         default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
    #                         output_df[col] = default_value
    #                     else:
    #                         output_df[col] = None
    #                 elif isinstance(col, dict):
    #                     for new_col, source_col in col.items():
    #                         if source_col in source_df.columns:
    #                             output_df[new_col] = source_df[source_col]
    #                         else:
    #                             output_df[new_col] = None
    #
    #             # Apply sorting if specified
    #             if sort_column and sort_column in output_df.columns:
    #                 output_df = output_df.sort_values(by=sort_column, ascending=ascending,
    #                                                   key=lambda col: col.str.lower() if col.dtype == 'object' else col)
    #
    #             delivery_file[sheet_name] = output_df
    #
    #     return delivery_file

#     def prepare_output(self, universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
#         delivery_file = {}
#
#         # Determine if we're using the old format (final_client) or new format (output_files)
#         if "output_files" in delivery_config:
#             # New format with multiple output files
#             for output_file_config in delivery_config["output_files"]:
#                 client_config = output_file_config["final_client"]
#                 output_config = output_file_config["output_columns"]
#
#                 # Process this output file
#                 file_delivery = self._process_output_file(universe, new_universe_df, client_config, output_config)
#                 delivery_file.update(file_delivery)
#         else:
#             # Old format with single output
#             client_config = delivery_config["final_client"]
#             output_config = delivery_config["output_columns"]
#             delivery_file = self._process_output_file(universe, new_universe_df, client_config, output_config)
#
#         return delivery_file
#
#     def _process_output_file(self, universe: dict, new_universe_df: pd.DataFrame,
#                              client_config: dict, output_config: dict) -> dict:
#         """Helper method to process a single output file configuration"""
#         delivery_file = {}
#
#         # Get sorting configuration
#         sort_column = output_config.get("sort")
#         ascending = output_config.get("ascending", True)
#
#         # sheets_names is now always a list in the config
#         sheet_templates = client_config["sheets_names"]  # These may contain placeholders
#
#         # Check if this is Albilad special case
#         is_albilad = client_config.get('name') == 'AlBilad'
#         # is_alrajhi = client_config.get('name') == 'AlRajhi'
#
#         if is_albilad:
#             # For Albilad, process ALL sheets with the same logic
#             for sheet_template in sheet_templates:
#                 sheet_name = self._resolve_sheet_name(sheet_template)
#                 source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
#                 source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
#
#                 # Handle empty DataFrames with comments
#                 if len(source_df) == 1 and 'Comments' in source_df.columns:
#                     delivery_file[sheet_name] = source_df
#                     continue
#
#                 required_columns = output_config.get(sheet_template, [])
#                 defaults = output_config.get("defaults", {})
#
#                 output_df = pd.DataFrame()
#
#                 # Process each required column
#                 for col in required_columns:
#                     if isinstance(col, str):
#                         if col in source_df.columns:
#                             output_df[col] = source_df[col]
#                         elif col in defaults:
#                             default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                             output_df[col] = default_value
#                         else:
#                             output_df[col] = None
#                     elif isinstance(col, dict):
#                         for new_col, source_col in col.items():
#                             if source_col in source_df.columns:
#                                 output_df[new_col] = source_df[source_col]
#                             else:
#                                 output_df[new_col] = None
#
#                 # Apply sorting if specified
#                 if sort_column and sort_column in output_df.columns:
#                     output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                                       key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#                 delivery_file[sheet_name] = output_df
#         else:
#             # Original logic for non-Albilad clients
#             # Process first sheet - always uses "Final"
#             first_sheet_template = sheet_templates[0]
#             first_sheet_name = self._resolve_sheet_name(first_sheet_template)
#             first_sheet_columns = output_config.get(first_sheet_template, [])
#
#             # Create output DataFrame for first sheet
#             output_df = new_universe_df.copy()
#
#             # Process columns with defaults and mappings
#             processed_columns = []
#             for col in first_sheet_columns:
#                 if isinstance(col, str):
#                     if col in output_df.columns:
#                         processed_columns.append(col)
#                     elif col in output_config.get("defaults", {}):
#                         default_value = resolve_date(output_config["defaults"][col]) if col == "Date" else \
#                             output_config["defaults"][col]
#                         output_df[col] = default_value
#                         processed_columns.append(col)
#                     else:
#                         output_df[col] = None
#                         processed_columns.append(col)
#                 elif isinstance(col, dict):
#                     for new_col, source_col in col.items():
#                         if source_col in output_df.columns:
#                             output_df[new_col] = output_df[source_col]
#                             processed_columns.append(new_col)
#                         else:
#                             output_df[new_col] = None
#                             processed_columns.append(new_col)
#
#             # Apply sorting
#             if sort_column and sort_column in output_df.columns:
#                 output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                                   key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#             # Ensure correct column order
#             output_df = output_df[processed_columns]
#             delivery_file[first_sheet_name] = output_df
#
#             # Process remaining sheets using the mapping
#             for sheet_template in sheet_templates[1:]:
#                 sheet_name = self._resolve_sheet_name(sheet_template)
#                 source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
#                 source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
#
#                 # Handle empty DataFrames with comments
#                 if len(source_df) == 1 and 'Comments' in source_df.columns:
#                     delivery_file[sheet_name] = source_df
#                     continue
#
#                 required_columns = output_config.get(sheet_template, [])
#                 defaults = output_config.get("defaults", {})
#
#                 output_df = pd.DataFrame()
#
#                 # Process each required column
#                 for col in required_columns:
#                     if isinstance(col, str):
#                         if col in source_df.columns:
#                             output_df[col] = source_df[col]
#                         elif col in defaults:
#                             default_value = resolve_date(defaults[col]) if col == "Date" else defaults[col]
#                             output_df[col] = default_value
#                         else:
#                             output_df[col] = None
#                     elif isinstance(col, dict):
#                         for new_col, source_col in col.items():
#                             if source_col in source_df.columns:
#                                 output_df[new_col] = source_df[source_col]
#                             else:
#                                 output_df[new_col] = None
#
#                 # Apply sorting if specified
#                 if sort_column and sort_column in output_df.columns:
#                     output_df = output_df.sort_values(by=sort_column, ascending=ascending,
#                                                       key=lambda col: col.str.lower() if col.dtype == 'object' else col)
#
#                 delivery_file[sheet_name] = output_df
#
#         return delivery_file
#
#     def _process_sheet(self, source_df, columns_config, defaults, sort_column, ascending):
#         output_df = pd.DataFrame()
#
#         for col in columns_config:
#             if isinstance(col, str):
#                 if col in source_df.columns:
#                     output_df[col] = source_df[col]
#                 elif col in defaults:
#                     output_df[col] = defaults[col]
#             elif isinstance(col, dict):
#                 for new_col, source_col in col.items():
#                     output_df[new_col] = source_df.get(source_col, defaults.get(new_col))
#
#         if sort_column and sort_column in output_df.columns:
#             output_df = output_df.sort_values(
#                 by=sort_column,
#                 ascending=ascending,
#                 key=lambda col: col.str.lower() if col.dtype == 'object' else col
#             )
#
#         return output_df
#
#
#     def _resolve_default_value(self, value, column_name):
#         """Helper to resolve default values, especially dates"""
#         if column_name == "Date":
#             return self._resolve_date(value)
#         return value
#
#     def _sort_dataframe(self, df, sort_column, ascending):
#         """Helper to sort dataframe with case-insensitive handling for strings"""
#         return df.sort_values(
#             by=sort_column,
#             ascending=ascending,
#             key=lambda col: col.str.lower() if col.dtype == 'object' else col
#         )
#
#     def _resolve_date(self, date_str):
#         """Helper to resolve date strings (implementation depends on your date handling)"""
#         # Implement your date resolution logic here
#         if date_str == "today":
#             return datetime.date.today().strftime("%Y-%m-%d")
#         return date_str
#
#     def _resolve_sheet_name(self, template):
#         """
#            Resolve dynamic sheet names with placeholders like {month}, {year}, {month_l}
#            """
#         # Get current date info
#         now = datetime.datetime.now()
#         today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
#         next_month = f'{today.month:02d}'
#         month = datetime.datetime(2023, int(next_month), 1).strftime("%m")
#         # month = now.strftime("%m")
#         year = now.strftime("%Y")
#         month_l = datetime.datetime(2023, int(next_month), 1).strftime("%B")  # Full month name
#         quarter = (now.month - 1) // 3 + 1  # Current quarter (1-4)
#
#         # Replace placeholders
#         sheet_name = template
#         sheet_name = sheet_name.replace("{month}", month)
#         sheet_name = sheet_name.replace("{year}", year)
#         sheet_name = sheet_name.replace("{month_l}", month_l)
#         sheet_name = sheet_name.replace("{quarter}", str(quarter))
#
#         return sheet_name
#
#


# class DeliveryStrategy(ABC):
#     """Abstract base class for delivery strategies"""
#
#     @abstractmethod
#     def prepare_output(self, universe: dict, config: dict, new_universe_df: pd.DataFrame) -> dict:
#         pass


# class DefaultDeliveryStrategy(DeliveryStrategy):
#     """Default strategy for most deliveries"""
#
#     def prepare_output(self, universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
#         """
#         Prepare output files based on the new format delivery configuration with multiple output files.
#         Each output file will contain only its specified sheets.
#
#         Args:
#             universe: Dictionary containing all input data sheets
#             delivery_config: The delivery configuration dictionary (new format with output_files)
#             new_universe_df: The processed DataFrame after filters
#
#         Returns:
#             Dictionary with filename patterns as keys and dictionaries of sheets as values
#         """
#         delivery_files = {}
#
#         # Process each output file configuration
#         for output_file_config in delivery_config["output_files"]:
#             filename_pattern = output_file_config["filename_pattern"]
#             client_config = output_file_config["final_client"]
#             output_config = {
#                 **output_file_config.get("output_columns", {}),
#                 "sort": output_file_config.get("sort"),
#                 "ascending": output_file_config.get("ascending", True),
#                 "defaults": output_file_config.get("defaults", {}),
#                 "csv": output_file_config.get("csv", False)
#             }
#
#             # Process this output file's sheets
#             file_sheets = self._process_output_file(
#                 universe=universe,
#                 new_universe_df=new_universe_df,
#                 client_config=client_config,
#                 output_config=output_config
#             )
#
#             # Add metadata to each sheet in this file
#             for sheet_name, sheet_df in file_sheets.items():
#                 sheet_df.attrs['filename_pattern'] = filename_pattern
#                 sheet_df.attrs['generate_csv'] = output_config.get("csv", False)
#                 sheet_df.attrs['output_config'] = output_config
#
#             # Store with filename pattern as key and its sheets as value
#             delivery_files[filename_pattern] = file_sheets
#
#         return delivery_files
#
# #########last used the below
#     def _process_output_file(self, universe: dict, new_universe_df: pd.DataFrame,
#                              client_config: dict, output_config: dict) -> dict:
#         """
#         Process a single output file configuration using the new format.
#         Handles both standard cases and AlBilad special requirements.
#         """
#         delivery_file = {}
#         sheet_templates = client_config["sheets_names"]
#         sort_column = output_config.get("sort")
#         ascending = output_config.get("ascending", True)
#         defaults = output_config.get("defaults", {})
#
#         # Check for AlBilad special case
#         is_albilad = client_config.get('name') == 'AlBilad'
#
#         if is_albilad:
#             # AlBilad processing - all sheets come from universe
#             for sheet_template in sheet_templates:
#                 sheet_name = self._resolve_sheet_name(sheet_template)
#                 source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
#                 source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
#
#                 # Handle comment-only DataFrames
#                 if len(source_df) == 1 and 'Comments' in source_df.columns:
#                     delivery_file[sheet_name] = source_df
#                     continue
#
#                 # Get columns for this sheet (use template name as key)
#                 required_columns = output_config.get(sheet_template, [])
#                 output_df = self._process_sheet_columns(
#                     source_df=source_df,
#                     columns_config=required_columns,
#                     defaults=defaults
#                 )
#
#                 # Apply AlBilad-specific formatting
#                 output_df = self._apply_albilad_formatting(output_df, sheet_name)
#
#                 # Apply sorting if specified
#                 if sort_column and sort_column in output_df.columns:
#                     output_df = self._sort_dataframe(output_df, sort_column, ascending)
#
#                 delivery_file[sheet_name] = output_df
#         else:
#             # Standard processing
#             if len(sheet_templates) == 1:
#                 # Single sheet output - use filtered universe
#                 sheet_name = self._resolve_sheet_name(sheet_templates[0])
#                 required_columns = output_config.get(sheet_templates[0], [])
#
#                 output_df = new_universe_df.copy()
#                 processed_columns = []
#
#                 # Process each column
#                 for col in required_columns:
#                     if isinstance(col, str):
#                         if col in output_df.columns:
#                             processed_columns.append(col)
#                         elif col in defaults:
#                             output_df[col] = self._resolve_default_value(defaults[col], col)
#                             processed_columns.append(col)
#                         else:
#                             output_df[col] = None
#                             processed_columns.append(col)
#                     elif isinstance(col, dict):
#                         for new_col, source_col in col.items():
#                             if source_col in output_df.columns:
#                                 output_df[new_col] = output_df[source_col]
#                             elif new_col in defaults:
#                                 output_df[new_col] = self._resolve_default_value(defaults[new_col], new_col)
#                             else:
#                                 output_df[new_col] = None
#                             processed_columns.append(new_col)
#
#                 # Apply sorting and column order
#                 if sort_column and sort_column in output_df.columns:
#                     output_df = self._sort_dataframe(output_df, sort_column, ascending)
#                 output_df = output_df[processed_columns]
#
#                 delivery_file[sheet_name] = output_df
#             else:
#                 # Multiple sheets - process each from universe
#                 for sheet_template in sheet_templates:
#                     sheet_name = self._resolve_sheet_name(sheet_template)
#                     source_key = SHEET_MAPPING.get(sheet_name, sheet_name)
#                     source_df = universe.get(source_key, pd.DataFrame(columns=output_config.get(sheet_template, [])))
#
#                     # Handle comment-only DataFrames
#                     if len(source_df) == 1 and 'Comments' in source_df.columns:
#                         delivery_file[sheet_name] = source_df
#                         continue
#
#                     required_columns = output_config.get(sheet_template, [])
#                     output_df = self._process_sheet_columns(
#                         source_df=source_df,
#                         columns_config=required_columns,
#                         defaults=defaults
#                     )
#
#                     # Apply sorting if specified
#                     if sort_column and sort_column in output_df.columns:
#                         output_df = self._sort_dataframe(output_df, sort_column, ascending)
#
#                     delivery_file[sheet_name] = output_df
#
#         return delivery_file

class DeliveryStrategy(ABC):
    """Abstract base class for delivery strategies"""

    @abstractmethod
    def prepare_output(self, universe: dict, config: dict, new_universe_df: pd.DataFrame) -> dict:
        pass
class DefaultDeliveryStrategy(DeliveryStrategy):

    def prepare_output(self, universe: dict, delivery_config: dict,
                       primary_universe_df: pd.DataFrame = None) -> dict:
        """
        Args:
            universe: Dictionary of all available DataFrames
            delivery_config: Delivery configuration
            primary_universe_df: Optional main filtered DataFrame (defaults to Saudi (SAR))
        """
        delivery_files = {}

        # Process each output file configuration
        for output_file_config in delivery_config["output_files"]:
            filename_pattern = output_file_config["filename_pattern"]
            client_config = output_file_config["final_client"]
            output_config = output_file_config.get("output_columns", {})

            # Process each sheet in this output file
            file_sheets = {}
            for sheet_template in client_config["sheets_names"]:
                sheet_name = self._resolve_sheet_name(sheet_template)

                # Get source DataFrame using SHEET_MAPPING with fallback to 'Final'
                source_key = SHEET_MAPPING.get(sheet_name, 'Final')  # Default to 'Final' if not found
                source_df = universe.get(source_key, pd.DataFrame())

                # # Handle empty DataFrames with comments
                # if len(source_df) == 1 and 'Comments' in source_df.columns:
                #     source_df = universe.get(sheet_name, pd.DataFrame())

                # If no data and this is the primary sheet, use the filtered DataFrame
                if source_df.empty and sheet_name == "Saudi (SAR)" and primary_universe_df is not None:
                    source_df = primary_universe_df

                # Special case: If DataFrame has exactly 1 row with Comments column, use as-is
                if len(source_df) == 1 and 'Comments' in source_df.columns:
                    sheet_df = source_df.copy()  # Use the DataFrame exactly as is
                else:
                # Process the sheet columns
                    sheet_df = self._process_sheet(
                        source_df=source_df,
                        columns_config=output_config.get(sheet_template, []),
                        defaults=output_config.get("defaults", {}),
                        sort_column=output_config.get("sort"),
                        ascending=output_config.get("ascending", True)
                    )

                # Add metadata
                sheet_df.attrs.update({
                    'filename_pattern': filename_pattern,
                    'generate_csv': output_config.get("csv", False)
                })

                file_sheets[sheet_name] = sheet_df

            delivery_files[filename_pattern] = file_sheets

        return delivery_files

    def _process_sheet(self, source_df, columns_config, defaults, sort_column, ascending):
        """Process a single sheet's DataFrame"""
        output_df = pd.DataFrame()

        for col in columns_config:
            if isinstance(col, str):
                if col in source_df.columns:
                    output_df[col] = source_df[col]
                elif col in defaults:
                    output_df[col] = self._resolve_default(defaults[col], source_df)
                else:
                    output_df[col] = None
            elif isinstance(col, dict):
                # Handle column mappings like {"New Name": "Original Name"}
                for new_col, source_col in col.items():
                    if source_col in source_df.columns:
                        output_df[new_col] = source_df[source_col]
                    elif new_col in defaults:
                        output_df[new_col] = self._resolve_default(defaults[new_col], source_df)
                    else:
                        output_df[new_col] = None

        # Apply sorting
        if sort_column and sort_column in output_df.columns:
            output_df = output_df.sort_values(
                by=sort_column,
                ascending=ascending,
                key=lambda x: x.str.lower() if x.dtype == 'object' else x
            )

        return output_df

    def _process_sheet_columns(self, source_df: pd.DataFrame, columns_config: list, defaults: dict) -> pd.DataFrame:
        """Process columns for a sheet according to configuration"""
        output_df = pd.DataFrame()

        for col in columns_config:
            if isinstance(col, str):
                if col in source_df.columns:
                    output_df[col] = source_df[col]
                elif col in defaults:
                    # output_df[col] = self._resolve_default(defaults[col])
                    default_val = self._resolve_default(defaults[col], source_df)
                    output_df[col] = default_val
                    print(f"Using default value for column '{col}': {default_val}")
                else:
                    output_df[col] = None
                    print(f"Column '{col}' not found in source and no default provided")
            elif isinstance(col, dict):
                for new_col, source_col in col.items():
                    if source_col in source_df.columns:
                        output_df[new_col] = source_df[source_col]
                    elif new_col in defaults:
                        # output_df[new_col] = self._resolve_default(defaults[new_col])
                        default_val = self._resolve_default(defaults[new_col], source_df)
                        output_df[new_col] = default_val
                        print(f"Using default value for column '{new_col}': {default_val}")
                    else:
                        output_df[new_col] = None
                        print(f"Column mapping '{source_col}'->'{new_col}' not found and no default provided")
            else:
                print(f"Warning: Unsupported column config type {type(col)} - {col}")

        return output_df

    def _apply_albilad_formatting(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Apply AlBilad-specific formatting rules"""
        if "AAOIFI" in sheet_name or "AlBilad" in sheet_name:
            if "Compliance" in df.columns:
                df["Compliance"] = df["Compliance"].apply(
                    lambda x: "PASS" if x == "PASS" else "FAIL"
                )
            if "AAOIFI_Compliance" in df.columns:
                df["AAOIFI_Compliance"] = df["AAOIFI_Compliance"].str.upper()

        return df

    # def _resolve_default(self, value, source_df):
    #     """Resolve special default values"""
    #     if value == "first_day_month":
    #         today = datetime.date.today()
    #         return datetime.date.today().strftime("%d-%m-%Y")
    #
    #     return value
    def _resolve_default(self, value, source_df):
        """Resolve special default values and handle column name changes"""
        # Handle special default values
        if value == "first_day_month":
            today = datetime.date.today()
            return today.replace(day=1).strftime("%d-%m-%Y")  # Fixed to return first day of month

        # Handle column name changes from defaults
        if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
            # Extract the column reference
            col_ref = value[1:-1]

            # Check if the column exists in source_df
            if col_ref in source_df.columns:
                return source_df[col_ref]
            else:
                actual_col = value
            # Handle the [Al Rayan] cases
            # if col_ref == "[Al Rayan] NPIN Status":
            #     # You might want to map this to an actual column name
            #     actual_col = "Al Rayan Sector Status"  # Example mapping
                if actual_col in source_df.columns:
                    return source_df[actual_col]

        return value

    def _resolve_default_value(self, value, column_name):
        """Resolve default values with special handling for dates"""
        if column_name == "Date":
            return self._resolve_date(value)
        return value

    def _sort_dataframe(self, df, sort_column, ascending):
        """Sort dataframe with case-insensitive handling for strings"""
        return df.sort_values(
            by=sort_column,
            ascending=ascending,
            key=lambda col: col.str.lower() if col.dtype == 'object' else col
        )

    def _resolve_date(self, date_str):
        """Resolve date strings (replace 'today' with current date)"""
        if date_str == "today":
            return datetime.date.today().strftime("%Y-%m-%d")
        elif date_str == "first_day_month":
            return datetime.date.today().strftime("%d-%m-%Y")
        return date_str

    def _resolve_sheet_name(self, template):
        """
           Resolve dynamic sheet names with placeholders like {month}, {year}, {month_l}
           """
        # Get current date info
        now = datetime.datetime.now()
        year = datetime.date.today().year
        today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
        next_month0 = f'{today.month:02d}'
        next_month = datetime.datetime(2023, int(next_month0), 1).strftime("%m")
        # month = now.strftime("%m")
        next_month_l = datetime.datetime(int(year), int(next_month0), 1).strftime("%B")  # Full month name
        quarter = (now.month - 1) // 3 + 1  # Current quarter (1-4)

        # Replace placeholders
        sheet_name = template
        sheet_name = sheet_name.replace("{next_month}", next_month)
        sheet_name = sheet_name.replace("{year}", str(year))
        sheet_name = sheet_name.replace("{next_month_letters}", str(next_month_l))
        sheet_name = sheet_name.replace("{quarter}", str(quarter))

        # sheet_name = sheet_name.replace("{next_month}", next_month)

        return sheet_name



class AlphaCapitalStrategy(DeliveryStrategy):
    """Special strategy for Alpha Capital delivery"""

    def prepare_output(self, universe: dict, config: dict, primary_universe_df: pd.DataFrame=None) -> dict:
        delivery_file = {}

        # Process Compliant List
        compliant_list = universe['Final'][['Name', 'ISIN', 'Ticker']].sort_values(
            by='Name', key=lambda col: col.str.lower())
        delivery_file['Compliant List'] = compliant_list

        # Process Included/Excluded
        included = self._prepare_compliance_sheet(universe['Included'])
        excluded = self._prepare_compliance_sheet(universe['Excluded'])

        # Create Change Compliance sheet
        empty = pd.DataFrame([" ", " ", " "], columns=['Name'])
        delivery_file['Change Compliance'] = pd.concat([included, empty, excluded]).fillna(" ")

        return delivery_file

    def _prepare_compliance_sheet(self, df):
        if 'Comment' not in df:
            df['Comment'] = " "
        return df[['Name', 'ISIN', 'Comment']].sort_values(
            by='Name', key=lambda col: col.str.lower())


class IntrospectStrategy(DeliveryStrategy):
    """Special strategy for Introspect Saudi delivery"""

    def prepare_output(self, universe: dict, config: dict, primary_universe_df: pd.DataFrame = None) -> dict:
        # Use default processing first
        default_strategy = DefaultDeliveryStrategy()
        delivery_files = default_strategy.prepare_output(universe, config)

        # Add Introspect-specific modifications
        for filename_pattern, file_sheets in delivery_files.items():
            for sheet_name, df in file_sheets.items():
                # More flexible matching for Introspect sheets
                if 'Delivery' in sheet_name or 'Introspect' in filename_pattern:
                    # Add Introspect-specific columns
                    df['Date'] = datetime.date.today().strftime("%d-%m-%Y")
                    # df['Introspect'] = "Introspect"

                    # Insert empty columns at specific positions if needed
                    df.insert(12, ' ', " ")
                    # df.insert(2, '  ', " ")

                    # Ensure we maintain the metadata
                    df.attrs.update(file_sheets[sheet_name].attrs)

        return delivery_files

class IntrospectMenaStrategy(DeliveryStrategy):
    """Special strategy for Introspect Saudi delivery"""

    def prepare_output(self, universe: dict, config: dict, primary_universe_df: pd.DataFrame = None) -> dict:
        # Use default processing first
        default_strategy = DefaultDeliveryStrategy()
        delivery_files = default_strategy.prepare_output(universe, config)

        # Add Introspect-specific modifications
        for filename_pattern, file_sheets in delivery_files.items():
            for sheet_name, df in file_sheets.items():
                # More flexible matching for Introspect sheets
                if sheet_name == 'Compliance' and 'Introspect' in filename_pattern:
                    # Add Introspect-specific columns
                    df['Date'] = datetime.date.today().strftime("%d-%m-%Y")
                    # df['Introspect'] = "Introspect"

                    # Insert empty columns at specific positions if needed
                    df.insert(11, ' ', " ")
                    # df.insert(2, '  ', " ")

                    # Ensure we maintain the metadata
                    df.attrs.update(file_sheets[sheet_name].attrs)

        return delivery_files


# class SaudiFransiStrategy(DefaultDeliveryStrategy):
#     """Special handling for Saudi Fransi's multi-universe delivery"""
#
#     def prepare_output(self, universe: dict, delivery_config: dict, primary_universe_df: pd.DataFrame = None) -> dict:
#         # Let parent class handle the basic processing
#         delivery_files = super().prepare_output(universe, delivery_config, primary_universe_df)
#
#         # Add Saudi Fransi-specific processing if needed
#         for file_sheets in delivery_files.values():
#             for sheet_name, sheet_df in file_sheets.items():
#                 if sheet_name == "Saudi (SAR)":
#                     # Example: Add special columns for SAR sheet
#                     if "Special Column" not in sheet_df.columns:
#                         sheet_df["Special Column"] = "SAR Value"
#
#         return delivery_files


class DeliveryStrategyFactory:
    """Creates the appropriate strategy based on delivery name"""

    _strategies = {
        'Alpha Capital': AlphaCapitalStrategy(),
        'Introspect Saudi': IntrospectStrategy(),
        'Introspect MENA': IntrospectMenaStrategy()
        # Add more special cases here
    }

    @classmethod
    def get_strategy(cls, delivery_name: str) -> DeliveryStrategy:
        return cls._strategies.get(delivery_name, DefaultDeliveryStrategy())


def prepare_final_output(universe: dict, delivery_config: dict, new_universe_df: pd.DataFrame) -> dict:
    """Now just delegates to the appropriate strategy"""
    strategy = DeliveryStrategyFactory.get_strategy(delivery_config['name'])
    return strategy.prepare_output(universe, delivery_config)


def save_df_to_excel(sheets: dict) -> bytes:
    """Convert dictionary of DataFrames to Excel bytes"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return output.getvalue()

def generate_excel_file(dataframes):
    """Generate Excel file from dict of DataFrames"""
    output = BytesIO()
    with pd.ExcelWriter(output) as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name[:31])
    return output  # Returns BytesIO

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to CSV bytes"""
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    return output.getvalue()

def generate_csv_file(dataframe):
    """Generate CSV file from DataFrame"""
    output = BytesIO()
    dataframe.to_csv(output, index=False)
    return output.getvalue()  # Returns bytes


# def generate_delivery_files(delivery_name, delivery_config, universe, raw_file):
#     strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
#     outputs = strategy.prepare_output(universe, delivery_config)
#
#     zip_files = []
#     zip_filenames = []
#
#     # Add raw file to ZIP
#     zip_files.append(raw_file)
#     zip_filenames.append(f'{delivery_name}_Raw_File_{datetime.datetime.now().strftime("%d%m%Y")}.xlsx')
#
#     for pattern, data in outputs.items():
#         # Generate Excel file
#         excel_bytes = generate_excel_file(data)
#         zip_files.append(excel_bytes)
#         zip_filenames.append(pattern)
#
#         # Generate CSV files if needed
#         for sheet_name in data["csv_sheets"]:
#             csv_bytes = generate_csv_file(data)
#             csv_filename = f"{Path(pattern).stem}_{sheet_name}.csv"
#             zip_files.append(csv_bytes)
#             zip_filenames.append(csv_filename)
#
#     return create_zip(zip_files, zip_filenames)




# def resolve_date(date_spec: str):
#     """
#     Handle special date values like 'first_day_month'
#     """
#     today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
#     next_month = f'{today.month:02d}'
#     year = datetime.date.today().year
#     if date_spec == "first_day_month":
#         today = datetime.date.today()
#         return datetime.date(today.year, {next_month}, 1).strftime("%d-%m-%Y")
#     return date_spec