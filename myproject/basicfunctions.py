import csv
import datetime
import dateutil
import xlsxwriter
import io
import pandas as pd
import numpy as np
import re
import zipfile
import arabic_reshaper
from bidi.algorithm import get_display
from deep_translator import GoogleTranslator
import urllib.parse
from urllib.parse import quote
import calendar
import os

from urllib.parse import unquote
import deep_translator


def writing(lst):
    output = io.BytesIO()

    test_wb = xlsxwriter.Workbook(output, {'nan_inf_to_errors': True})
    headers = test_wb.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black',
                                  'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
    cells = test_wb.add_format({'bold': False, 'font_color': 'black',
                                'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})

    # Writing Field Group Tabs:
    empty_lst = {}
    for tab, sheet in lst.items():
        if sheet is not None and len(sheet) > 0:
            s = test_wb.add_worksheet(tab)
            for col_no, data in enumerate(sheet, 0):
                s.write(0, col_no, data, headers)  # row adjusted
                row_count = 1
                for d in enumerate(sheet[data], 0):
                    s.write(row_count, col_no, d[1], cells)
                    row_count += 1

                count2 = 0
                for i in sheet.columns:
                    column = []
                    for c in sheet[i]:
                        column.append(len(str(c)))
                        column.append(len(str(i)))
                    s.set_column(count2, count2, max(column) + 1)
                    count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

    test_wb.close()
    output.seek(0)

    return output


def df_to_csv_bytes(lst):
    output = io.BytesIO()
    for tab, sheet in lst.items():
        print(lst)
        sheet.to_csv(output, index=False)
        output.seek(0)
    return output


def read_new_function(raw_file):
    try:
        raw_file = pd.read_excel(raw_file, keep_default_na=False, dtype=object)
    except:
        raw_file = pd.read_csv(raw_file, keep_default_na=False, low_memory=False, encoding_errors='ignore'
                               , on_bad_lines='skip')

    # Define a function to check if a row can be a valid header based on specific keywords
    def is_valid_header(row):
        keywords = ["WID", "ISIN", "Name", "IR WID"]
        return any(keyword in row.values for keyword in keywords)

    # Iterate through the rows until a valid header is found
    header_row = None
    for idx, row in raw_file.iterrows():
        if is_valid_header(row):
            header_row = idx
            break

    # If a valid header row is found, set it as the header and remove rows above it
    if header_row is not None:
        raw_file.columns = raw_file.iloc[header_row]
        raw_file = raw_file.drop(range(header_row + 1)).reset_index(drop=True)

    wid_col = Finders.index_column(raw_file)
    raw_file = raw_file.rename(raw_file[wid_col])

    raw_file = raw_file.drop_duplicates(keep='first')
    # raw_file.columns = [str(re.sub(' +',' ',x)).strip() for x in raw_file.columns]
    raw_file.index = [str(x).strip() for x in raw_file.index]

    return raw_file


def read_excel_sheet_func(raw_file, sheet:str):

    raw_file = pd.read_excel(raw_file, sheet_name=sheet, keep_default_na=False, parse_dates=True,dtype=object)

    def is_valid_header(row):
        keywords = ["WID", "ISIN", "Name"]
        return any(keyword in row.values for keyword in keywords)

    # Iterate through the rows until a valid header is found
    header_row = None
    for idx, row in raw_file.iterrows():
        if is_valid_header(row):
            header_row = idx
            break

    # If a valid header row is found, set it as the header and remove rows above it
    if header_row is not None:
        raw_file.columns = raw_file.iloc[header_row]
        raw_file = raw_file.drop(range(header_row + 1)).reset_index(drop=True)

    # if client not in ['NBK_MENA']:
    wid_col = Finders.index_column(raw_file)
    raw_file = raw_file.rename(raw_file[wid_col])

    raw_file = raw_file.drop_duplicates(keep='first')
    # raw_file.columns = [str(re.sub(' +',' ',x)).strip() for x in raw_file.columns]
    raw_file.index = [str(x).strip() for x in raw_file.index]

    return raw_file


def prepare_filenames(delivery_config: dict, format_vars: dict) -> dict:
    """
    Prepare filenames for all output files based on their patterns.

    Args:
        delivery_config: The delivery configuration dictionary
        format_vars: Dictionary of variables for filename formatting

    Returns:
        Dictionary mapping original filename patterns to resolved filenames
    """
    filename_mapping = {}

    if 'output_files' not in delivery_config:
        # Handle legacy single-file case
        pattern = delivery_config.get('filename_pattern', '')
        try:
            filename = f"{pattern.format(**format_vars)}.xlsx" if pattern else ''
            filename_mapping[pattern] = filename
        except KeyError as e:
            print(f"Invalid placeholder in filename pattern: {e}")
            filename_mapping[pattern] = ''
        return filename_mapping

    # Process each output file's pattern
    for output_file in delivery_config['output_files']:
        pattern = output_file['filename_pattern']
        try:
            filename = f"{pattern.format(**format_vars)}.xlsx"
            filename_mapping[pattern] = filename
        except KeyError as e:
            print(f"Invalid placeholder in filename pattern '{pattern}': {e}")
            # Generate fallback filename with timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_mapping[pattern] = f"delivery_{timestamp}.xlsx"

    return filename_mapping

# def writing_delivery(lst, client, delivery_config):
#     # Preparations
#     output = BytesIO()
#     filename= None
#     year = datetime.date.today().year
#     day = f'{datetime.date.today().day:02d}'
#     month_f = f'{datetime.date.today().month:02d}'
#     today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
#     next_month = f'{today.month:02d}'
#     last_day = calendar.monthrange(year, int(month_f))[1]
#     quarter = pd.Timestamp(datetime.date.today()).quarter
#     last_day = calendar.monthrange(year, int(month_f))[1]
#     month = datetime.datetime(int(year), int(next_month), 1).strftime("%B")
#     month_l = datetime.datetime(int(year), int(month_f), 1).strftime("%B")
#
#     try:
#         month_translated = GoogleTranslator(source='auto', target='arabic').translate(text=month)
#     except:
#         # Handle the error, e.g., by setting a default translation or logging the error.
#         month_translated = month  # using the original month name as a fallback
#         # Alternatively, you might log the error or take some other action.
#     month_translated = quote(month_translated)
#     arabic_name = 'قائمة السوق السعودي'
#     arabic_client= quote(arabic_name)
#
#     arabic = '۰١٢٣٤٥٦٧٨٩'
#     english = '0123456789'
#
#     translation_table = str.maketrans(english, arabic)
#     translated_year = str(year).translate(translation_table)
#     translated_year = quote(translated_year)
#     next_year = int(year)+1
#     translated_next_year = str(next_year).translate(translation_table)
#     translated_next_year = quote(translated_next_year)
#
#     format_vars = {
#         'client': client,
#         'year': year,
#         'next_month_letters': month,
#         'current_month': month_f,
#         'month_letters': month_l,
#         'month_translated': month_translated,
#         'day': day,
#         'quarter': quarter,
#         'next_month': next_month,
#         'translated_year': translated_year,
#         'translated_next_year': translated_next_year,
#         'arabic_client': arabic_client,
#         'last_day': last_day
#     }
#
#     book = xlsxwriter.Workbook(output, {'nan_inf_to_errors': True})
#     if client == 'Arabesque_Delivery_File':
#         headers = book.add_format({'bold': False, 'font_color': 'black',
#                                    'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': '', 'border': True})
#     else:
#         headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black',
#                                    'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': '', 'border': True})
#     cells = book.add_format({'bold': False, 'font_color': 'black',
#                              'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
#     format1 = book.add_format({'font_color': 'red', 'num_format': '0.00%'})
#     format2 = book.add_format({'font_color': 'red'})
#     format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})
#     percent_format = book.add_format({'font_color': 'black', 'text_wrap': False, 'bottom': True,
#                                       'border': True, 'num_format': '0.00%'})
#     cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
#                                    'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
#                                    'font_size': '11'})
#     cell_format_font = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
#                                    'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
#                                    'font_size': '12'})
#
#     cell_format2 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False, 'bg_color': '#ED7D31',
#                                     'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
#                                     'font_size': '11'})
#     # writing Field Group Tabs:
#     empty_lst = {}
#     # for s in lst:
#     #     print(f"the list in writing is: {lst}")
#     #     print(f"in writing s is : {s}")
#     for tab, sheet in lst.items():
#         if sheet is not None and len(sheet) > 0:
#             s = book.add_worksheet(tab)
#             for col_no, data in enumerate(sheet, 0):
#                 if client == 'Derayah':
#                     s.write(0, 0, "Report Type", cell_format2)
#                     s.write(0, 1, 'Compliance List', cell_format)
#                     s.write(1, 0, 'Customer Name', cell_format2)
#                     s.write(1, 1, 'Derayah', cell_format)
#                     s.write(2, 0, 'Rulebook', cell_format2)
#                     s.write(2, 1, "Derayah", cell_format)
#                     s.write(3, 0, 'Prepared By', cell_format2)
#                     s.write(3, 1, 'IdealRatings Support Team', cell_format)
#                     s.write(4, 0, 'Date', cell_format2)
#                     s.write(4, 1, f'{month} 1, {year+1 if month=="January" else year}', cell_format)
#                     s.write(6, col_no, data, headers)  # row adjusted
#                     row_count = 7
#                 elif client == 'Saudi Fransi':
#                     s.write(0, 0, "Report Type", cell_format2)
#                     s.write(0, 1, 'Compliance List', cell_format)
#                     s.write(1, 0, 'Customer Name', cell_format2)
#                     s.write(1, 1, 'Saudi Fransi Capital', cell_format)
#                     s.write(2, 0, 'Prepared By', cell_format2)
#                     s.write(2, 1, 'IdealRatings Support Team', cell_format)
#                     s.write(3, 0, 'Date', cell_format2)
#                     s.write(3, 1, f'{month} 1, {year+1 if month=="January" else year}', cell_format)
#                     s.write(6, col_no, data, headers)  # row adjusted
#                     row_count = 7
#                 elif client == 'Egypt Universe':
#                     s.write(0, 0, "Shariah & Non-Shariah Universe", cell_format_font)
#                     s.write(1, 0, 'Scope: Egypt', cell_format_font)
#                     s.write(2, 0, f'Results Incorporated Q{quarter - 1} {year}', cell_format_font)
#                     s.write(3, 0, f'Effective From: {month_l} {day}th, {year}', cell_format_font)
#                     s.write(6, col_no, data, headers)  # row adjusted
#                     row_count = 7
#                 else:
#                     s.write(0, col_no, data, headers)  # row adjusted
#                     row_count = 1
#
#                 for d in enumerate(sheet[data], 0):
#                     if 'date' in str(data).lower():
#                         s.write(row_count, col_no, d[1], format3)
#                     elif '[fransicapital]' in str(data).lower() and 'npin' not in str(data).lower():
#                         s.write(row_count, col_no, d[1], percent_format)
#                     elif 'ratio' in str(data).lower():
#                         s.write(row_count, col_no, d[1], percent_format)
#                     elif '[s&p shariah-based]' in str(data).lower().strip() and client == 'SRB_GCC_List':
#                         s.write(row_count, col_no, d[1], percent_format)
#                     else:
#                         s.write(row_count, col_no, d[1], cells)
#                     row_count += 1
#                 count2 = 0
#                 for i in sheet.columns:
#                     column = []
#                     for c in sheet[i]:
#                         column.append(len(str(c)))
#                         column.append(len(str(i)))
#                     s.set_column(count2, count2, max(column) + 1)
#                     count2 += 1
#         else:
#             empty_lst[tab] = 'No Changes'
#
#         # Get the first output file configuration
#     # output_file_config = delivery_config.get("output_files", [{}])[0]
#     #
#     # # Prepare filename
#     # filename_mapping = prepare_filenames(delivery_config, format_vars)
#     # pattern = output_file_config.get('filename_pattern', '')
#     # filename = filename_mapping.get(pattern, 'delivery.xlsx')
#     if delivery_config and 'filename_pattern' in delivery_config:
#         try:
#             filename = f"{delivery_config['filename_pattern'].format(**format_vars)}.xlsx"
#         except KeyError as e:
#             print(f"Invalid placeholder in filename pattern: {e}")
#             filename = f"{client}_{year}{month_f}{day}.xlsx"
#     else:
#         # Fallback to default pattern if no pattern is specified
#         filename = f"{client}_{year}{month_f}{day}.xlsx"
#
#     book.close()
#     output.seek(0)
#     return output, filename

    # filename_mapping = prepare_filenames(delivery_config=delivery_config, format_vars=format_vars)
    #
    # for pattern in delivery_config['filename_pattern']:
    #     filename = filename_mapping.get(pattern, f"{client}_{year}{month_f}{day}.xlsx")

    # if str(client).strip() in ['BMK']:
    #     filename=f"Saudi Universe_{client}_{year+1 if str(month_f)=='12' else year}{month_f}25.xlsx"
    #
    # elif str(client).strip() == 'Riyad Capital':
    #
    #    filename=f"Shariah_List_01{next_month}{year+1 if str(next_month)=='01' else year}.xlsx"
    #
    # elif str(client).strip() in ['Saudi_Compliant_List_AlBilad', 'Saudi Universe_Jadwa', 'NUQI_Global_Universe',
    #                              'SEDCO Global Small & Mid Cap Compliant Universe', 'Raw Indonesia OJK']:
    #     filename=f"{client}_{month}_{year+1 if str(month)=='January' else year}.xlsx"
    #
    # elif str(client).strip() == 'AlRajhi':
    #     filename = f"{year}{month_f}{last_day}_Saudi Universe_AlRajhi_Q{quarter}_{year}.xlsx"
    #
    # elif str(client).strip() in ['Rassanah', 'Jadwa', 'ADIB']:
    #     filename=f"Saudi_Universe_{client}_{month}_{year+1 if month=='January' else year}.xlsx"
    #
    # elif str(client).strip() == 'Alinma':
    #    filename=f"{client} Compliance List_{month}_{year+1 if month=='January' else year}.xlsx"
    #
    # elif str(client).strip() in ['TCW_US Universe', 'Ashmore - Saudi Universe', 'IdealRatings_Russell_Universe', 'IdealRatings_Russell_Universe_REITs',
    #                              'Foreign_NVDR_ThaiStocks', 'IdealRatings_Axiom_AAOIFI Compliance', 'IdealRatings_Axiom_AAOIFI Compliance_1B',
    #                              'IdealRatings_Russell_Australia_Universe',"KFH_Global List", "Compliant Global REITs_Rasameel",'ARGA_Emerging_Markets_Delivery',
    #                              'IdealRatings AM_Manulife_Delivery', "AAOIFI_Manulife_Delivery", 'NAIS_Global Universe', 'IdealRatings_ANB_Saudi List',
    #                              "IdealRatings_ANB_US List"]:
    #    filename=f"{year+1 if str(next_month)=='01' else year}{next_month}01_{client}.xlsx"
    #
    # elif str(client).strip() == 'Alinma Shariah':
    #     filename=f"{arabic_client} - {month_translated} {translated_next_year if str(month) =='January' else translated_year}.xlsx"
    #
    # elif str(client).strip() == 'Derayah Saudi (Derayah Proposed RB)':
    #    filename=f'{year+1 if next_month=="01" else year}{next_month}01_SaudiArabia List_Q{quarter}_Derayah.xlsx'
    #
    # elif client in ['Derayah ASRHC']:
    #     filename = f'Saudi Compliant Universe_{month} {year+1 if month=="January" else year}.xlsx'
    #
    # elif client == "Symbols":
    #     filename= f'{client}_01{next_month}{year+1 if str(next_month)=="01" else year}_080000.xlsx'
    #
    # elif client == 'Morgan Saudi':
    #     filename= f'{year+1 if str(month_f)=="12" else year}{month_f}25_Saudi Arabia List_Q{"4" if str(quarter)=="1" else quarter-1}_Morgan Stanley.xlsx'
    #
    #
    # else:
    #     if delivery_config and 'filename_pattern' in delivery_config:
    #         try:
    #             filename = f"{delivery_config['filename_pattern'].format(**format_vars)}.xlsx"
    #         except KeyError as e:
    #             print(f"Invalid placeholder in filename pattern: {e}")
    #             filename = f"{client}_{year}{month_f}{day}.xlsx"
    #     else:
    #         # Fallback to default pattern if no pattern is specified
    #         filename = f"{client}_{year}{month_f}{day}.xlsx"

    # return output, filename

# def writing_delivery(delivery_data: dict, client: str, delivery_config: dict):
#     """
#     Write a single delivery file based on the first matching configuration,
#     using sheet names and filename pattern from delivery_config.
#
#     Args:
#         delivery_data: Dictionary with sheet names as keys and DataFrames as values
#         client: Client name
#         delivery_config: Delivery configuration dictionary with output_files
#
#     Returns:
#         Tuple of (BytesIO object containing the Excel file, filename)
#     """
#     # Preparations
#     year = datetime.date.today().year
#     day = f'{datetime.date.today().day:02d}'
#     month_f = f'{datetime.date.today().month:02d}'
#     today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
#     next_month = f'{today.month:02d}'
#     quarter = pd.Timestamp(datetime.date.today()).quarter
#     month = datetime.datetime(int(year), int(next_month), 1).strftime("%B")
#     month_l = datetime.datetime(int(year), int(month_f), 1).strftime("%B")
#     last_day = calendar.monthrange(year, int(month_f))[1]
#
#     # Prepare format variables
#     format_vars = {
#         'client': client,
#         'year': year,
#         'next_month_letters': month,
#         'current_month': month_f,
#         'month_letters': month_l,
#         'day': day,
#         'quarter': quarter,
#         'next_month': next_month,
#         'last_day': last_day
#     }
#
#     # Get the first output file configuration
#     output_file_config = delivery_config.get("output_files", [{}])[0]
#
#     # Prepare filename
#     filename_mapping = prepare_filenames(delivery_config, format_vars)
#     pattern = output_file_config.get('filename_pattern', '')
#     filename = filename_mapping.get(pattern, 'delivery.xlsx')
#
#     # Get the sheets for this output file
#     sheet_names = output_file_config.get('final_client', {}).get('sheets_names', [])
#     sheets_to_write = {name: delivery_data[name] for name in sheet_names if name in delivery_data}
#
#     # Create workbook
#     output = BytesIO()
#     book = xlsxwriter.Workbook(output, {'nan_inf_to_errors': True})
#
#     # Set up formats
#     if client == 'Arabesque_Delivery_File':
#         headers = book.add_format({'bold': False, 'font_color': 'black',
#                                    'text_wrap': False, 'align': 'left', 'bottom': True,
#                                    'bottom_color': '', 'border': True})
#     else:
#         headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black',
#                                    'text_wrap': False, 'align': 'left', 'bottom': True,
#                                    'bottom_color': '', 'border': True})
#
#     cells = book.add_format({'bold': False, 'font_color': 'black',
#                              'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
#     format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})
#     percent_format = book.add_format({'font_color': 'black', 'text_wrap': False, 'bottom': True,
#                                       'border': True, 'num_format': '0.00%'})
#     cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
#                                    'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
#                                    'font_size': '11'})
#     cell_format_font = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
#                                         'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
#                                         'font_size': '12'})
#     cell_format2 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False, 'bg_color': '#ED7D31',
#                                     'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
#                                     'font_size': '11'})
#
#     # Write each sheet
#     for sheet_name, sheet_df in sheets_to_write.items():
#         if sheet_df is not None and len(sheet_df) > 0:
#             try:
#                 worksheet = book.add_worksheet(sheet_name)
#             except xlsxwriter.exceptions.DuplicateWorksheetName:
#                 # Handle duplicate sheet names by adding a suffix
#                 sheet_name = f"{sheet_name}_{len(book.worksheets())}"
#                 worksheet = book.add_worksheet(sheet_name)
#
#             row_count = 0
#
#             # Write client-specific headers
#             if client == 'Derayah':
#                 worksheet.write(0, 0, "Report Type", cell_format2)
#                 worksheet.write(0, 1, 'Compliance List', cell_format)
#                 worksheet.write(1, 0, 'Customer Name', cell_format2)
#                 worksheet.write(1, 1, 'Derayah', cell_format)
#                 worksheet.write(2, 0, 'Rulebook', cell_format2)
#                 worksheet.write(2, 1, "Derayah", cell_format)
#                 worksheet.write(3, 0, 'Prepared By', cell_format2)
#                 worksheet.write(3, 1, 'IdealRatings Support Team', cell_format)
#                 worksheet.write(4, 0, 'Date', cell_format2)
#                 worksheet.write(4, 1, f'{month} 1, {year + 1 if month == "January" else year}', cell_format)
#                 row_count = 6
#             elif client == 'Saudi Fransi':
#                 worksheet.write(0, 0, "Report Type", cell_format2)
#                 worksheet.write(0, 1, 'Compliance List', cell_format)
#                 worksheet.write(1, 0, 'Customer Name', cell_format2)
#                 worksheet.write(1, 1, 'Saudi Fransi Capital', cell_format)
#                 worksheet.write(2, 0, 'Prepared By', cell_format2)
#                 worksheet.write(2, 1, 'IdealRatings Support Team', cell_format)
#                 worksheet.write(3, 0, 'Date', cell_format2)
#                 worksheet.write(3, 1, f'{month} 1, {year + 1 if month == "January" else year}', cell_format)
#                 row_count = 6
#             elif client == 'Egypt Universe':
#                 worksheet.write(0, 0, "Shariah & Non-Shariah Universe", cell_format_font)
#                 worksheet.write(1, 0, 'Scope: Egypt', cell_format_font)
#                 worksheet.write(2, 0, f'Results Incorporated Q{quarter - 1} {year}', cell_format_font)
#                 worksheet.write(3, 0, f'Effective From: {month_l} {day}th, {year}', cell_format_font)
#                 row_count = 6
#
#             # Write column headers
#             for col_no, col_name in enumerate(sheet_df.columns):
#                 worksheet.write(row_count, col_no, col_name, headers)
#             row_count += 1
#
#             # Write data rows
#             for _, row in sheet_df.iterrows():
#                 for col_no, col_name in enumerate(sheet_df.columns):
#                     value = row[col_name]
#                     if 'date' in str(col_name).lower():
#                         worksheet.write(row_count, col_no, value, format3)
#                     elif '[fransicapital]' in str(col_name).lower() and 'npin' not in str(col_name).lower():
#                         worksheet.write(row_count, col_no, value, percent_format)
#                     elif 'ratio' in str(col_name).lower():
#                         worksheet.write(row_count, col_no, value, percent_format)
#                     elif '[s&p shariah-based]' in str(col_name).lower().strip() and client == 'SRB_GCC_List':
#                         worksheet.write(row_count, col_no, value, percent_format)
#                     else:
#                         worksheet.write(row_count, col_no, value, cells)
#                 row_count += 1
#
#             # Adjust column widths
#             for col_no, col_name in enumerate(sheet_df.columns):
#                 max_len = max(sheet_df[col_name].astype(str).apply(len).max(), len(col_name))
#                 worksheet.set_column(col_no, col_no, max_len + 1)
#
#     book.close()
#     output.seek(0)
#
#     return output, filename

def writing_delivery(delivery_data: dict, client: str, delivery_config: dict):
    """
    Write delivery files based on the delivery configuration, ensuring each output file
    contains only its specified sheets.

    Args:
        delivery_data: Dictionary with sheet names as keys and DataFrames as values
        client: Client name
        delivery_config: Delivery configuration dictionary with output_files

    Returns:
        Dictionary mapping filenames to BytesIO objects containing the Excel files
    """
    # Preparations
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    quarter = pd.Timestamp(datetime.date.today()).quarter
    month = datetime.datetime(int(year), int(next_month), 1).strftime("%B")
    month_l = datetime.datetime(int(year), int(month_f), 1).strftime("%B")
    last_day = calendar.monthrange(year, int(month_f))[1]

    # Prepare format variables
    format_vars = {
        'client': client,
        'year': year,
        'next_month_letters': month,
        'current_month': month_f,
        'month_letters': month_l,
        'day': day,
        'quarter': quarter,
        'next_month': next_month,
        'last_day': last_day
    }
    print("Starting writing_delivery function")  # Debug
    print(f"Delivery data keys: {delivery_data.keys()}")  # Debug

    # Prepare filename mapping
    filename_mapping = prepare_filenames(delivery_config, format_vars)
    output_files = {}
    print(f"Filename mapping: {filename_mapping}")  # Debug

    # Process each output file configuration
    for output_file_config in delivery_config.get("output_files", []):
        print(f"Processing output file config: {output_file_config}")  # Debug
        pattern = output_file_config['filename_pattern']
        filename = filename_mapping.get(pattern)
        print(f"Pattern: {pattern}, Filename: {filename}")  # Debug
        if not filename:
            print(f"No filename matched for pattern: {pattern}")  # Debug
            continue

        # Get the sheets for this output file
        names = []
        sheet_names = output_file_config['final_client']['sheets_names']
        for sheet in sheet_names:
            sheet = resolve_sheet_name(sheet)
            names.append(sheet)
        print(f"Looking for sheets: {names}")  # Debug

        # Find the matching delivery pattern in delivery_data
        matching_delivery = delivery_data.get(pattern, {})
        print(f"Matching delivery data: {matching_delivery.keys()}")  # Debug

        # Get sheets from the matching delivery pattern
        sheets_to_write = {name: matching_delivery[name]
                           for name in names
                           if name in matching_delivery}
        print(f"Found sheets to write: {sheets_to_write.keys()}")  # Debug
        # sheets_to_write = {name: delivery_data[name] for name in sheet_names if name in delivery_data.values()}
        # print(f"Found sheets to write: {sheets_to_write.keys()}, {delivery_data.values()}")  # Debug

        if not sheets_to_write:
            print("No sheets to write for this file")  # Debug
            continue

        # Create a new workbook for this output file
        output = io.BytesIO()
        book = xlsxwriter.Workbook(output, {'nan_inf_to_errors': True})

        # Set up formats (move inside the loop)
        if client == 'Arabesque_Delivery_File':
            headers = book.add_format({'bold': False, 'font_color': 'black',
                                     'text_wrap': False, 'align': 'left', 'bottom': True,
                                     'bottom_color': '', 'border': True})
        else:
            headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black',
                                     'text_wrap': False, 'align': 'left', 'bottom': True,
                                     'bottom_color': '', 'border': True})

        cells = book.add_format({'bold': False, 'font_color': 'black',
                               'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
        format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})
        percent_format = book.add_format({'font_color': 'black', 'text_wrap': False, 'bottom': True,
                                        'border': True, 'num_format': '0.00%'})
        cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                     'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
                                     'font_size': '11'})
        cell_format_font = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                          'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
                                          'font_size': '12'})
        cell_format2 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False, 'bg_color': '#ED7D31',
                                      'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
                                      'font_size': '11'})

        # Write each sheet for this file
        for sheet_name, sheet_df in sheets_to_write.items():
            print(f"Sheet: {sheet_name}, Shape: {sheet_df.shape}")
            if sheet_df is not None and len(sheet_df) > 0:
                try:
                    worksheet = book.add_worksheet(sheet_name)
                except xlsxwriter.exceptions.DuplicateWorksheetName:
                    # Handle duplicate sheet names by adding a suffix
                    sheet_name = f"{sheet_name}_{len(book.worksheets())}"
                    worksheet = book.add_worksheet(sheet_name)

                row_count = 0

                # Write client-specific headers
                if client == 'Derayah Saudi (Derayah Proposed RB)':
                    worksheet.write(0, 0, "Report Type", cell_format2)
                    worksheet.write(0, 1, 'Compliance List', cell_format)
                    worksheet.write(1, 0, 'Customer Name', cell_format2)
                    worksheet.write(1, 1, 'Derayah', cell_format)
                    worksheet.write(2, 0, 'Rulebook', cell_format2)
                    worksheet.write(2, 1, "Derayah", cell_format)
                    worksheet.write(3, 0, 'Prepared By', cell_format2)
                    worksheet.write(3, 1, 'IdealRatings Support Team', cell_format)
                    worksheet.write(4, 0, 'Date', cell_format2)
                    worksheet.write(4, 1, f'{month} 1, {year + 1 if month == "January" else year}', cell_format)
                    row_count = 6
                elif client == 'Saudi Fransi':
                    worksheet.write(0, 0, "Report Type", cell_format2)
                    worksheet.write(0, 1, 'Compliance List', cell_format)
                    worksheet.write(1, 0, 'Customer Name', cell_format2)
                    worksheet.write(1, 1, 'Saudi Fransi Capital', cell_format)
                    worksheet.write(2, 0, 'Prepared By', cell_format2)
                    worksheet.write(2, 1, 'IdealRatings Support Team', cell_format)
                    worksheet.write(3, 0, 'Date', cell_format2)
                    worksheet.write(3, 1, f'{month} 1, {year + 1 if month == "January" else year}', cell_format)
                    row_count = 6
                elif client == 'Egypt Universe':
                    worksheet.write(0, 0, "Shariah & Non-Shariah Universe", cell_format_font)
                    worksheet.write(1, 0, 'Scope: Egypt', cell_format_font)
                    worksheet.write(2, 0, f'Results Incorporated Q{quarter - 1} {year}', cell_format_font)
                    worksheet.write(3, 0, f'Effective From: {month_l} {day}th, {year}', cell_format_font)
                    row_count = 6

                # Write column headers
                for col_no, col_name in enumerate(sheet_df.columns):
                    worksheet.write(row_count, col_no, col_name, headers)
                row_count += 1

                # Write data rows
                for _, row in sheet_df.iterrows():
                    for col_no, col_name in enumerate(sheet_df.columns):
                        value = row[col_name]
                        if 'date' in str(col_name).lower():
                            worksheet.write(row_count, col_no, value, format3)
                        elif '[fransicapital]' in str(col_name).lower() and 'npin' not in str(col_name).lower():
                            worksheet.write(row_count, col_no, value, percent_format)
                        elif 'ratio' in str(col_name).lower():
                            worksheet.write(row_count, col_no, value, percent_format)
                        elif '[s&p shariah-based]' in str(col_name).lower().strip() and client == 'SRB_GCC_List':
                            worksheet.write(row_count, col_no, value, percent_format)
                        else:
                            worksheet.write(row_count, col_no, value, cells)
                    row_count += 1

                # Adjust column widths
                for col_no, col_name in enumerate(sheet_df.columns):
                    max_len = max(sheet_df[col_name].astype(str).apply(len).max(), len(col_name))
                    worksheet.set_column(col_no, col_no, max_len + 1)
        book.close()
        output.seek(0)
        output_files[filename] = output

        # Generate CSV files if needed
        for sheet_name, sheet_df in sheets_to_write.items():
            if sheet_df.attrs.get('generate_csv', False):
                csv_output = io.BytesIO()
                sheet_df.to_csv(csv_output, index=False)
                csv_output.seek(0)
                csv_filename = filename.replace('.xlsx', f'_{sheet_name}.csv')
                output_files[csv_filename] = csv_output
    print(f"output files are : {output_files}")
    return output_files

def resolve_sheet_name(template):
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
def writing_introspect_url(delivery_data: dict, client: str, delivery_config: dict):
    # Preparations
    output = io.BytesIO()
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    month = datetime.datetime(2023, int(next_month), 1).strftime("%B")
    quarter = pd.Timestamp(datetime.date.today()).quarter
    month_l = datetime.datetime(int(year), int(month_f), 1).strftime("%B")
    last_day = calendar.monthrange(year, int(month_f))[1]

    # Prepare format variables
    format_vars = {
        'client': client,
        'year': year,
        'next_month_letters': month,
        'current_month': month_f,
        'month_letters': month_l,
        'day': day,
        'quarter': quarter,
        'next_month': next_month,
        'last_day': last_day
    }
    print("Starting writing_delivery function")  # Debug
    print(f"Delivery data keys: {delivery_data.keys()}")  # Debug

    # Prepare filename mapping
    filename_mapping = prepare_filenames(delivery_config, format_vars)
    output_files = {}
    print(f"Filename mapping: {filename_mapping}")  # Debug

    # Process each output file configuration
    for output_file_config in delivery_config.get("output_files", []):
        print(f"Processing output file config: {output_file_config}")  # Debug
        pattern = output_file_config['filename_pattern']
        filename = filename_mapping.get(pattern)
        print(f"Pattern: {pattern}, Filename: {filename}")  # Debug
        if not filename:
            print(f"No filename matched for pattern: {pattern}")  # Debug
            continue

        # Get the sheets for this output file
        names = []
        sheet_names = output_file_config['final_client']['sheets_names']
        for sheet in sheet_names:
            sheet = resolve_sheet_name(sheet)
            names.append(sheet)
        print(f"Looking for sheets: {names}")  # Debug

        # Find the matching delivery pattern in delivery_data
        matching_delivery = delivery_data.get(pattern, {})
        print(f"Matching delivery data: {matching_delivery.keys()}")  # Debug

        # Get sheets from the matching delivery pattern
        sheets_to_write = {name: matching_delivery[name]
                           for name in names
                           if name in matching_delivery}
        print(f"Found sheets to write: {sheets_to_write.keys()}")  # Debug
        # sheets_to_write = {name: delivery_data[name] for name in sheet_names if name in delivery_data.values()}
        # print(f"Found sheets to write: {sheets_to_write.keys()}, {delivery_data.values()}")  # Debug

        if not sheets_to_write:
            print("No sheets to write for this file")  # Debug
            continue

        book = xlsxwriter.Workbook(output,{'nan_inf_to_errors': True})
        if client == 'Arabesque_Delivery_File':
            headers = book.add_format({'bold': False, 'font_color': 'black',
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        else:
            headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black', 'border': True,
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        cells = book.add_format({'bold': False, 'font_color': 'black',
                                 'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
        format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})

        format1 = book.add_format({'font_color': 'red', 'num_format': '0.00%'})

        format2 = book.add_format({'font_color': 'red'})

        percent_format = book.add_format({'font_color': 'black', 'num_format': '0.00%'})

        # writing Field Group Tabs:
        empty_lst = {}
        # for s in lst:
        for tab, sheet in sheets_to_write.items():
            if sheet is not None and len(sheet) > 0:
                s = book.add_worksheet(tab)
                for col_no, data in enumerate(sheet, 0):
                    s.write(0, col_no, data, headers)  # row adjusted
                    s.conditional_format('I2:J1048576', {'type': 'cell',
                                                         'criteria': 'greater than',
                                                         'value': '0.3333',
                                                         'format': format1})

                    s.conditional_format('I2:J1048576', {'type': 'cell',
                                                         'criteria': 'less than',
                                                         'value': '0.3333',
                                                         'format': percent_format})

                    s.conditional_format('H2:H1048576', {'type': 'cell',
                                                         'criteria': 'equal to',
                                                         'value': 'FALSE',
                                                         'format': format2})

                    s.conditional_format('K2:K1048576', {'type': 'cell',
                                                         'criteria': 'greater than',
                                                         'value': '0.05',
                                                         'format': format1})

                    s.conditional_format('K2:K1048576', {'type': 'cell',
                                                         'criteria': 'less than',
                                                         'value': '0.05',
                                                         'format': percent_format})


                    # s.conditional_format('J6:K1048576', {'type': 'cell',
                    #                                      'criteria': 'equal to',
                    #                                      'value': "",
                    #                                      'format': empty_format})
                    row_count = 1
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        s.set_column(0, 0, 32)
                        #s.set_column('G:H', max(column) + 1, format1)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

        book.close()
        output.seek(0)
        output_files[filename] = output

        # Generate CSV files if needed
        for sheet_name, sheet_df in sheets_to_write.items():
            if sheet_df.attrs.get('generate_csv', False):
                csv_output = io.BytesIO()
                sheet_df.to_csv(csv_output, index=False)
                csv_output.seek(0)
                csv_filename = filename.replace('.xlsx', f'_{sheet_name}.csv')
                output_files[csv_filename] = csv_output
    print(f"output files are : {output_files}")

    # filename= f'Introspect_IdealRatings_Saudi Compliance List_{month}_{year}.xlsx'

    return output_files


def writing_delivery_alpha_url(lst, client):
    # Preparations
    output = io.BytesIO()
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    month = datetime.datetime(2023, int(next_month), 1).strftime("%B")
    month_l = datetime.datetime(2023, int(month_f), 1).strftime("%B")
    quarter = pd.Timestamp(datetime.date.today()).quarter

    book = xlsxwriter.Workbook(output,{'nan_inf_to_errors': True})
    if client == 'Arabesque_Delivery_File':
        headers = book.add_format({'bold': False, 'font_color': 'black',
                                   'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
    else:
        headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black', 'border': True,
                                   'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
    cells = book.add_format({'bold': False, 'font_color': 'black',
                             'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
    format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})

    cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                   'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
                                   'font_size': '12'})

    cell_format2 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False, 'bg_color': '#ED7D31',
                                   'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True, 'font_size': '12'})

    c_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                'align': 'left', 'bottom': True, 'font_size': '12'})

    format1 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,'border': True,
                               'align': 'center', 'bottom': True, 'font_size': '12'})

    format_1 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                               'align': 'center', 'bottom': True, 'font_size': '12', 'right': True})

    format2 = book.add_format({'text_wrap': False,'bottom': False, 'border':False})

    percent_format = book.add_format({'font_color': 'black', 'num_format': '0.00%', 'border': True, 'align': 'left'})

    # writing Field Group Tabs:
    empty_lst = {}
    # for s in lst:
    for tab, sheet in lst.items():
        if sheet is not None and len(sheet) > 0 and tab == 'Compliant List':
            s = book.add_worksheet(tab)
            for col_no, data in enumerate(sheet, 0):
                s.write(0, col_no, data, headers)  # row adjusted
                row_count = 1
                for d in enumerate(sheet[data], 0):
                    if 'date' in str(data).lower():
                        s.write(row_count, col_no, d[1], format3)
                    elif 'ratio' in str(data).lower():
                        s.write(row_count, col_no, d[1], percent_format)
                    else:
                        s.write(row_count, col_no, d[1], cells)
                    row_count += 1

                count2 = 0
                for i in sheet.columns:
                    column = []
                    for c in sheet[i]:
                        column.append(len(str(c)))
                        column.append(len(str(i)))
                    s.set_column(count2, count2, max(column) + 1)
                    # s.set_column(0, 0, 18)
                    # s.set_column(1, 1, 30)
                    #s.set_column('G:H', max(column) + 1, format1)
                    count2 += 1
        else:
            empty_lst[tab] = 'No Changes'

        if sheet is not None and len(sheet) > 0 and tab == 'Change Compliance':
            s = book.add_worksheet(tab)
            s.merge_range(1, 0, 1, 2, None, format1)
            for col_no, data in enumerate(sheet, 0):
                s.write(0, 0, "Universe Update", c_format)
                s.write(1, 0, 'Included', format1)
                s.write(2, col_no, data, headers)  # row adjusted
                row_count = 3

                for d in enumerate(sheet[data], 0):
                    if 'date' in str(data).lower():
                        s.write(row_count, col_no, d[1], format3)
                    elif 'name' in str(data).lower() and (d[1] == np.nan or str(d[1]) == " " or d[1] is None):
                        count = row_count
                    else:
                        s.write(row_count, col_no, d[1], cells)
                    row_count += 1

            s.merge_range(count-1, 0, count-1, 2, None, format_1)
            for col_no, data in enumerate(sheet, 0):
                s.write(count-2, 0, " ", format2)
                s.write(count-2, 1, " ", format2)
                s.write(count-2, 2, " ", format2)
                s.write(count - 3, 0, " ", format2)
                s.write(count - 3, 1, " ", format2)
                s.write(count - 3, 2, " ", format2)

                s.write(count-1, 0, 'Excluded', format_1)
                s.write(count, 0, 'Name', headers)
                s.write(count, 1, 'ISIN', headers)
                s.write(count, 2, 'Comment', headers)

                count2 = 0
                for i in sheet.columns:
                    column = []

                    for c in sheet[i]:
                        column.append(len(str(c)))
                        column.append(len(str(i)))

                    s.set_column(count2, count2, max(column) + 1)
                    s.set_column(0, 0, 20)
                    count2 += 1
        else:
            empty_lst[tab] = 'No Changes'

    book.close()
    output.seek(0)

    filename = f'{str(client).replace(" ", "")} Compliant List_{month} {year+1 if month == "January" else year}.xlsx'

    return output, filename

def writing_pure_alrajhi_url(lst, client, delivery_config):
    # Preparations
    # Preparations
    output = io.BytesIO()
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    quarter = pd.Timestamp(datetime.date.today()).quarter

    book = xlsxwriter.Workbook(output, {'nan_inf_to_errors': True})
    if client == 'Arabesque_Delivery_File':
        headers = book.add_format({'bold': False, 'font_color': 'black',
                                   'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
    else:
        headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black', 'border': True,
                                   'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
    cells = book.add_format({'bold': False, 'font_color': 'black',
                             'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
    format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})

    percent_format = book.add_format({'font_color': 'black', 'bottom': True, 'border': True, 'num_format': '0.00%'})

    format_vars = {
        'client': client,
        'year': year,
        'current_month': month_f,
        'day': day,
        'quarter': quarter,
        'next_month': next_month,
    }
    # writing Field Group Tabs:
    empty_lst = {}
    for tab, sheet in lst.items():
        if sheet is not None and len(sheet) > 0:
            s = book.add_worksheet(tab)
            for col_no, data in enumerate(sheet, 0):
                s.write(0, col_no, data, headers)  # row adjusted
                row_count = 1
                for d in enumerate(sheet[data], 0):
                    if 'date' in str(data).lower():
                        s.write(row_count, col_no, d[1], format3)
                    elif 'ratio' in str(data).lower():
                        s.write(row_count, col_no, d[1], percent_format)
                    else:
                        s.write(row_count, col_no, d[1], cells)
                    row_count += 1

                count2 = 0
                for i in sheet.columns:
                    column = []
                    for c in sheet[i]:
                        column.append(len(str(c)))
                        column.append(len(str(i)))
                    s.set_column(count2, count2, max(column) + 1)
                    count2 += 1
        else:
            empty_lst[tab] = 'No Changes'

    book.close()
    output.seek(0)

    filename = f"Saudi Pure List_AlRajhi_Q{quarter}_{year}.xlsx"

    return output, filename


def writing_delivery_alphagcc_url(delivery_data: dict, client: str, delivery_config: dict):
    # Preparations
    output = io.BytesIO()
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    month = datetime.datetime(2023, int(next_month), 1).strftime("%B")
    month_l = datetime.datetime(2023, int(month_f), 1).strftime("%B")
    quarter = pd.Timestamp(datetime.date.today()).quarter

    # Prepare format variables
    format_vars = {
        'client': client,
        'year': year,
        'next_month_letters': month,
        'current_month': month_f,
        'month_letters': month_l,
        'day': day,
        'quarter': quarter,
        'next_month': next_month
    }
    print("Starting writing_delivery function")  # Debug
    print(f"Delivery data keys: {delivery_data.keys()}")  # Debug

    # Prepare filename mapping
    filename_mapping = prepare_filenames(delivery_config, format_vars)
    output_files = {}
    print(f"Filename mapping: {filename_mapping}")  # Debug

    # Process each output file configuration
    for output_file_config in delivery_config.get("output_files", []):
        print(f"Processing output file config: {output_file_config}")  # Debug
        pattern = output_file_config['filename_pattern']
        filename = filename_mapping.get(pattern)
        print(f"Pattern: {pattern}, Filename: {filename}")  # Debug
        if not filename:
            print(f"No filename matched for pattern: {pattern}")  # Debug
            continue

        # Get the sheets for this output file
        names = []
        sheet_names = output_file_config['final_client']['sheets_names']
        for sheet in sheet_names:
            sheet = resolve_sheet_name(sheet)
            names.append(sheet)
        print(f"Looking for sheets: {names}")  # Debug

        # Find the matching delivery pattern in delivery_data
        matching_delivery = delivery_data.get(pattern, {})
        print(f"Matching delivery data: {matching_delivery.keys()}")  # Debug

        # Get sheets from the matching delivery pattern
        sheets_to_write = {name: matching_delivery[name]
                           for name in names
                           if name in matching_delivery}
        print(f"Found sheets to write: {sheets_to_write.keys()}")  # Debug
        # sheets_to_write = {name: delivery_data[name] for name in sheet_names if name in delivery_data.values()}
        # print(f"Found sheets to write: {sheets_to_write.keys()}, {delivery_data.values()}")  # Debug

        if not sheets_to_write:
            print("No sheets to write for this file")  # Debug
            continue

        book = xlsxwriter.Workbook(output,{'nan_inf_to_errors': True})
        if client == 'Arabesque_Delivery_File':
            headers = book.add_format({'bold': False, 'font_color': 'black',
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        else:
            headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black', 'border': True,
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        cells = book.add_format({'bold': False, 'font_color': 'black',
                                 'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
        format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})

        cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                       'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
                                       'font_size': '12'})

        cell_format2 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False, 'bg_color': '#ED7D31',
                                       'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True, 'font_size': '12'})

        c_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                    'align': 'left', 'bottom': True, 'font_size': '12'})

        format1 = book.add_format({'bg_color': '#FFFF00', 'font_color': 'black', 'num_format': '0.00%'})

        format2 = book.add_format({'text_wrap': False,'bottom': False, 'border':False})

        percent_format = book.add_format({'font_color': 'black', 'num_format': '0.00%', 'border': True, 'align': 'left'})

        merge_format = book.add_format({"border": False, 'text_wrap': False, "align": "center", "valign": "vcenter"})

        # writing Field Group Tabs:
        empty_lst = {}
        # for s in lst:
        for tab, sheet in sheets_to_write.items():
            if sheet is not None and len(sheet) > 0 and tab == 'Compliant List':
                s = book.add_worksheet(tab)
                for col_no, data in enumerate(sheet, 0):
                    s.write(0, col_no, data, headers)  # row adjusted
                    row_count = 1
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        elif 'ratio' in str(data).lower():
                            s.write(row_count, col_no, d[1], percent_format)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        # s.set_column(0, 0, 18)
                        # s.set_column(1, 1, 30)
                        #s.set_column('G:H', max(column) + 1, format1)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

            if sheet is not None and len(sheet) > 0 and tab == 'Change Compliance':
                s = book.add_worksheet(tab)
                for col_no, data in enumerate(sheet, 0):
                    s.write(0, 0, "Change Compliance", c_format)
                    s.write(1, col_no, data, headers)  # row adjusted
                    row_count = 2
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        s.set_column(0, 0, 20)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

        book.close()
        output.seek(0)

        output_files[filename] = output

        # Generate CSV files if needed
        for sheet_name, sheet_df in sheets_to_write.items():
            if sheet_df.attrs.get('generate_csv', False):
                csv_output = io.BytesIO()
                sheet_df.to_csv(csv_output, index=False)
                csv_output.seek(0)
                csv_filename = filename.replace('.xlsx', f'_{sheet_name}.csv')
                output_files[csv_filename] = csv_output

    # filename = f'Alpha Capital_GCC Ex-Saudi_Compliant List_{month} {year + 1 if month == "January" else year}.xlsx'

    return output_files

def writing_rayangcc_delivery_url(delivery_data: dict, client: str, delivery_config: dict):
    # Preparations
    output = io.BytesIO()
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    month = datetime.datetime(2023, int(next_month), 1).strftime("%B")
    # Prepare format variables
    format_vars = {
        'client': client,
        'year': year,
        'next_month_letters': month,
        'current_month': month_f,
        # 'month_letters': month_l,
        'day': day,
        # 'quarter': quarter,
        'next_month': next_month
    }
    print("Starting writing_delivery function")  # Debug
    print(f"Delivery data keys: {delivery_data.keys()}")  # Debug

    # Prepare filename mapping
    filename_mapping = prepare_filenames(delivery_config, format_vars)
    output_files = {}
    print(f"Filename mapping: {filename_mapping}")  # Debug

    # Process each output file configuration
    for output_file_config in delivery_config.get("output_files", []):
        print(f"Processing output file config: {output_file_config}")  # Debug
        pattern = output_file_config['filename_pattern']
        filename = filename_mapping.get(pattern)
        print(f"Pattern: {pattern}, Filename: {filename}")  # Debug
        if not filename:
            print(f"No filename matched for pattern: {pattern}")  # Debug
            continue

        # Get the sheets for this output file
        names = []
        sheet_names = output_file_config['final_client']['sheets_names']
        for sheet in sheet_names:
            sheet = resolve_sheet_name(sheet)
            names.append(sheet)
        print(f"Looking for sheets: {names}")  # Debug

        # Find the matching delivery pattern in delivery_data
        matching_delivery = delivery_data.get(pattern, {})
        print(f"Matching delivery data: {matching_delivery.keys()}")  # Debug

        # Get sheets from the matching delivery pattern
        sheets_to_write = {name: matching_delivery[name]
                           for name in names
                           if name in matching_delivery}
        print(f"Found sheets to write: {sheets_to_write.keys()}")  # Debug
        # sheets_to_write = {name: delivery_data[name] for name in sheet_names if name in delivery_data.values()}
        # print(f"Found sheets to write: {sheets_to_write.keys()}, {delivery_data.values()}")  # Debug

        if not sheets_to_write:
            print("No sheets to write for this file")  # Debug
            continue

        book = xlsxwriter.Workbook(output,{'nan_inf_to_errors': True})
        if client == 'Arabesque_Delivery_File':
            headers = book.add_format({'bold': False, 'font_color': 'black',
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        else:
            headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black', 'border': True,
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        cells = book.add_format({'bold': False, 'font_color': 'black',
                                 'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
        format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})

        cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                       'align': 'left', 'valign':'vcenter', 'bottom': True, 'border': True, 'font_size':'12'})
        c_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                    'align': 'left', 'bottom': True})
        empty_format = book.add_format({'bold': False, 'font_color': 'black', 'text_wrap': False,
                                    'align': 'left', 'bottom': False})

        format1 = book.add_format({'bg_color': '#FFFF00', 'font_color': 'black', 'num_format': '0.00%'})

        format2 = book.add_format({'bg_color': '#FFFF00', 'font_color': 'black'})

        percent_format = book.add_format({'font_color': 'black', 'num_format': '0.00%'})

        # writing Field Group Tabs:
        empty_lst = {}

        for tab, sheet in sheets_to_write.items():
            if sheet is not None and len(sheet) > 0 and tab == 'GCC Universe':
                s = book.add_worksheet(tab)
                for col_no, data in enumerate(sheet, 0):
                    s.write(0, 0, "Shariah & Non Shariah Universe", cell_format)
                    s.write(1, 0, 'Scope: GCC Equities', cell_format)
                    s.write(2, 0, f'Date: 1st {month} {year}', cell_format)
                    s.write(4, col_no, data, headers)  # row adjusted
                    s.conditional_format('G6:H1048576', {'type': 'cell',
                                                         'criteria': 'greater than',
                                                         'value': '0.3333',
                                                         'format': format1})

                    s.conditional_format('G6:H1048576', {'type': 'cell',
                                                         'criteria': 'less than',
                                                         'value': '0.3333',
                                                         'format': percent_format})

                    s.conditional_format('J6:K1048576', {'type': 'text',
                                                         'criteria': 'containing',
                                                         'value': 'FALSE',
                                                         'format': format2})

                    # s.conditional_format('J6:K1048576', {'type': 'cell',
                    #                                      'criteria': 'equal to',
                    #                                      'value': "",
                    #                                      'format': empty_format})
                    row_count = 5
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        s.set_column(0, 0, 32)
                        #s.set_column('G:H', max(column) + 1, format1)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

            if sheet is not None and len(sheet) > 0 and tab == 'Change Compliance':
                s = book.add_worksheet(tab)
                for col_no, data in enumerate(sheet, 0):
                    s.write(0, 0, "Change Compliance", c_format)
                    s.write(1, col_no, data, headers)  # row adjusted
                    row_count = 2
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        s.set_column(0, 0, 20)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

        book.close()
        output.seek(0)
        output_files[filename] = output

        # Generate CSV files if needed
        for sheet_name, sheet_df in sheets_to_write.items():
            if sheet_df.attrs.get('generate_csv', False):
                csv_output = io.BytesIO()
                sheet_df.to_csv(csv_output, index=False)
                csv_output.seek(0)
                csv_filename = filename.replace('.xlsx', f'_{sheet_name}.csv')
                output_files[csv_filename] = csv_output

    return output_files

def writing_delivery_alrajhigcc_url(delivery_data: dict, client: str, delivery_config: dict):
    # Preparations
    output = io.BytesIO()
    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    month = datetime.datetime(2023, int(next_month), 1).strftime("%B")
    month_l = datetime.datetime(2023, int(month_f), 1).strftime("%B")
    image = os.path.join("myproject/static/IdealRatings.png")
    quarter = pd.Timestamp(datetime.date.today()).quarter
    last_quarter = int(quarter-1)

    format_vars = {
        'client': client,
        'year': year,
        'next_month_letters': month,
        'current_month': month_f,
        'month_letters': month_l,
        'day': day,
        'quarter': quarter,
        'next_month': next_month,
        'last_quarter': last_quarter
    }
    print("Starting writing_delivery function")  # Debug
    print(f"Delivery data keys: {delivery_data.keys()}")  # Debug

    # Prepare filename mapping
    filename_mapping = prepare_filenames(delivery_config, format_vars)
    output_files = {}
    print(f"Filename mapping: {filename_mapping}")  # Debug

    # Process each output file configuration
    for output_file_config in delivery_config.get("output_files", []):
        print(f"Processing output file config: {output_file_config}")  # Debug
        pattern = output_file_config['filename_pattern']
        filename = filename_mapping.get(pattern)
        print(f"Pattern: {pattern}, Filename: {filename}")  # Debug
        if not filename:
            print(f"No filename matched for pattern: {pattern}")  # Debug
            continue

        # Get the sheets for this output file
        names = []
        sheet_names = output_file_config['final_client']['sheets_names']
        for sheet in sheet_names:
            sheet = resolve_sheet_name(sheet)
            names.append(sheet)
        print(f"Looking for sheets: {names}")  # Debug

        # Find the matching delivery pattern in delivery_data
        matching_delivery = delivery_data.get(pattern, {})
        print(f"Matching delivery data: {matching_delivery.keys()}")  # Debug

        # Get sheets from the matching delivery pattern
        sheets_to_write = {name: matching_delivery[name]
                           for name in names
                           if name in matching_delivery}
        print(f"Found sheets to write: {sheets_to_write.keys()}")  # Debug

        if not sheets_to_write:
            print("No sheets to write for this file")  # Debug
            continue

        book = xlsxwriter.Workbook(output,{'nan_inf_to_errors': True})
        if client == 'Arabesque_Delivery_File':
            headers = book.add_format({'bold': False, 'font_color': 'black',
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        else:
            headers = book.add_format({'bold': True, 'bg_color': '#ED7D31', 'font_color': 'black', 'border': True,
                                       'text_wrap': False, 'align': 'left', 'bottom': True, 'bottom_color': ''})
        cells = book.add_format({'bold': False, 'font_color': 'black',
                                 'text_wrap': False, 'align': 'left', 'bottom': True, 'border': True})
        format3 = book.add_format({'num_format': 'mm/dd/yy', 'bottom': True, 'border': True})

        cell_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                       'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True,
                                       'font_size': '12'})

        cell_format2 = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False, 'bg_color': '#ED7D31',
                                       'align': 'left', 'valign': 'vcenter', 'bottom': True, 'border': True, 'font_size': '12'})

        c_format = book.add_format({'bold': True, 'font_color': 'black', 'text_wrap': False,
                                    'align': 'left', 'bottom': True, 'font_size': '12'})

        format1 = book.add_format({'bg_color': '#FFFF00', 'font_color': 'black', 'num_format': '0.00%'})

        #format2 = book.add_format({'text_wrap': False,'bottom': False, 'border':False})

        percent_format = book.add_format({'font_color': 'black', 'num_format': '0.00%', 'border': True, 'align': 'left'})

        merge_format = book.add_format({"border": False, 'text_wrap': False, "align": "center", "valign": "vcenter"})

        # writing Field Group Tabs:
        empty_lst = {}
        for tab, sheet in sheets_to_write.items():
            if sheet is not None and len(sheet) > 0 and tab == 'Compliance':
                s = book.add_worksheet(tab)
                s.merge_range(0, 0, 4, 1, None)
                for col_no, data in enumerate(sheet, 0):
                    s.insert_image(1, 0, image)
                    s.write(5, 0, "Report Type", cell_format2)
                    s.write(5,1, 'Shariah & Non Shariah Universe', cell_format)
                    s.write(6, 0, 'Scope', cell_format2)
                    s.write(6, 1, 'GCC ex-Saudi', cell_format)
                    s.write(7, 0, 'Results Incorporated', cell_format2)
                    s.write(7,1, f"Q{quarter-1} {year}", cell_format)
                    s.write(8,0, 'Effective From', cell_format2)
                    s.write(8, 1, f'{month_l} 6th, {year}', cell_format)
                    s.write(10, col_no, data, headers)  # row adjusted
                    row_count = 11
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        elif 'ratio' in str(data).lower():
                            s.write(row_count, col_no, d[1], percent_format)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        s.set_column(0, 0, 18)
                        s.set_column(1, 1, 30)
                        #s.set_column('G:H', max(column) + 1, format1)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

            if sheet is not None and len(sheet) > 0 and tab == 'Change Compliance':
                s = book.add_worksheet(tab)
                for col_no, data in enumerate(sheet, 0):
                    s.write(0, 0, "Change Compliance", c_format)
                    s.write(1, col_no, data, headers)  # row adjusted
                    row_count = 2
                    for d in enumerate(sheet[data], 0):
                        if 'date' in str(data).lower():
                            s.write(row_count, col_no, d[1], format3)
                        else:
                            s.write(row_count, col_no, d[1], cells)
                        row_count += 1

                    count2 = 0
                    for i in sheet.columns:
                        column = []
                        for c in sheet[i]:
                            column.append(len(str(c)))
                            column.append(len(str(i)))
                        s.set_column(count2, count2, max(column) + 1)
                        s.set_column(0, 0, 20)
                        count2 += 1
            else:
                empty_lst[tab] = 'No Changes'

        book.close()
        output.seek(0)
        output_files[filename] = output

        # Generate CSV files if needed
        for sheet_name, sheet_df in sheets_to_write.items():
            if sheet_df.attrs.get('generate_csv', False):
                csv_output = io.BytesIO()
                sheet_df.to_csv(csv_output, index=False)
                csv_output.seek(0)
                csv_filename = filename.replace('.xlsx', f'_{sheet_name}.csv')
                output_files[csv_filename] = csv_output

    return output_files


# def create_zip(output_files, file_names):
#     zip_buffer = BytesIO()
#     with zipfile.ZipFile(zip_buffer, "w") as zip_file:
#         for output, name in zip(output_files, file_names):
#             zip_file.writestr(name, output.getvalue())
#     zip_buffer.seek(0)
#     return zip_buffer

def create_zip(*file_sources):
    """
    Create a zip file containing multiple file sources.
    Supports:
    - Dictionary of {filename: BytesIO} (from writing_delivery)
    - Individual BytesIO objects with explicit filenames
    - Lists of BytesIO objects with corresponding filenames

    Args:
        *file_sources: Variable arguments that can be:
            - A dictionary {filename: BytesIO}
            - A list/tuple where:
                - First item is BytesIO content
                - Second item is filename string
            - Separate arguments of any of the above types

    Returns:
        BytesIO object containing the zip file
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for source in file_sources:
            # Handle dictionary input (from writing_delivery)
            if isinstance(source, dict):
                for filename, file_content in source.items():
                    file_content.seek(0)
                    zip_file.writestr(filename, file_content.read())

            # Handle list/tuple input
            elif isinstance(source, (list, tuple)):
                # Case 1: List of (BytesIO, filename) pairs
                if all(isinstance(item, (list, tuple)) and len(item) == 2 for item in source):
                    for file_content, filename in source:
                        file_content.seek(0)
                        zip_file.writestr(filename, file_content.read())
                # Case 2: Single (BytesIO, filename) pair
                elif len(source) == 2 and isinstance(source[0], io.BytesIO) and isinstance(source[1], str):
                    file_content, filename = source
                    file_content.seek(0)
                    zip_file.writestr(filename, file_content.read())
                else:
                    raise ValueError(
                        "List/tuple input must be either [(BytesIO, filename), ...] or (BytesIO, filename)")

            # Handle standalone BytesIO with filename (if needed)
            elif isinstance(source, io.BytesIO):
                raise ValueError("Standalone BytesIO must be paired with a filename")

    zip_buffer.seek(0)
    return zip_buffer

# def create_zip(output_files, file_names):
#     """
#     Creates a ZIP file from multiple file objects.
#
#     Args:
#         output_files: List of either BytesIO objects or bytes
#         file_names: List of corresponding filenames
#
#     Returns:
#         BytesIO object containing the ZIP file
#     """
#     zip_buffer = BytesIO()
#     with zipfile.ZipFile(zip_buffer, "w") as zip_file:
#         for output, name in zip(output_files, file_names):
#             # Handle both BytesIO and raw bytes
#             if isinstance(output, BytesIO):
#                 content = output.getvalue()
#             else:
#                 content = output  # Assume it's already bytes
#             zip_file.writestr(name, content)
#     zip_buffer.seek(0)
#     return zip_buffer


class Finders:

    def index_column(file:pd.DataFrame):
        column = None
        for i in ['IR WID', 'IR_WID', 'WID', 'wid', 'ir wid',
                  'ir_wid', 'IR_wid', 'IRWID', 'irwid']:
            if i in file.columns:
                column = i
            else:
                pass

        if column is not None and len(str(column)) >= 3:
            return column
        else:
            for i in file.columns:
                if 'wid' in str(i).lower().strip():
                    column = i
                    break
            return column

    def isin_finder(file: pd.DataFrame):
        column = 'ISIN Column Not Found'

        for i in file.columns:
            if len(list(filter(lambda x: re.match(re.sub(r"[^a-zA-Z0-9-.]", " ", i)
                    , x, re.IGNORECASE), ['ISIN']))) > 0:
                column = i

        return column

    def market_cap_finder(file: pd.DataFrame):
        column = ''
        for i in file.columns:
            if str(i).lower().strip() == 'market cap':
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'market' in str(i).lower().strip() and 'trailing' not in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''

        return column

    def issue_active_finder(file:pd.DataFrame):
        column = 'Issue Active Not Found'
        for i in file.columns:
            if len(list(filter(lambda x: re.match(re.sub(r"[^a-zA-Z0-9-.]", " ", x)
                    , i, re.IGNORECASE), ['Issue Active Status']))) > 0:
                column = i
        return column

    def ticker_finder(file: pd.DataFrame):
        column = 'Ticker Column Not Found'

        for i in file.columns:
            if len(list(filter(lambda x: re.match(re.sub(r"[^a-zA-Z0-9-.]", " ", i)
                    , x, re.IGNORECASE), ['Ticker']))) > 0:
                column = i

        return column

    def riccode_finder(file: pd.DataFrame):
        column = ''
        for i in [x for x in file.columns[file.columns.str.contains('ric code', case=False)]]:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'ric code' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''

        return column

    def finder_ratios_alrayan(file:pd.DataFrame):
        ratios_columns = []
        for i in file.columns:
            if ('[al rayan]' in str(i).lower().strip() and 'ratio' in str(i).lower().strip()) or (
                    'non-permissible income' in str(i).lower().strip()):
                ratios_columns.append(i)
            else:
                pass
        return ratios_columns

    def period_end_date_finder(file: pd.DataFrame):
        column = ''
        for i in [x for x in file.columns[file.columns.str.contains('end date', case=False)]]:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'period' in str(i).lower().strip() and 'end' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''
        return column

    def fiscal_period_finder(file: pd.DataFrame):
        column = 'Fiscal Period Not Found'
        for i in file.columns:
            if len(list(filter(lambda x: re.match(re.sub(r"[^a-zA-Z0-9-.]", " ", x)
                    , i, re.IGNORECASE), ['Fiscal Period']))) > 0:
                column = i
        return column

    def BA_finder(file: pd.DataFrame):
        column = None
        for i in file.columns:
            if 'business' in str(i).lower().strip():
                column = i
            else:
                pass
        return column

    def nation_finder(file:pd.DataFrame):
        column = None
        for i in ['nation', 'Nation', 'NATION']:
            if i in file.columns:
                column = i
            else:
                pass

        if column is not None and len(str(column)) > 0:
            return column
        else:
            for i in file.columns:
                if 'nation' in str(i).lower().strip():
                    column = i
                    break
            return column

    def adib_finder(file: pd.DataFrame):
        column = ''
        for i in [x for x in file.columns[file.columns.str.contains('adib cons', case=False)]]:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'adib' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''

        return column

    def date_finder(file: pd.DataFrame):
        column = []
        for i in file.columns:
            if 'date' in str(i).lower().strip():
                column.append(i)
            else:
                pass
        return column

    def date_current_price_finder(file: pd.DataFrame):
        column = ''
        for i in [x for x in file.columns[file.columns.str.contains('Current Price', case=False)]]:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'current' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''

        return column

    def albilad_finder(file: pd.DataFrame):
        column = ''
        for i in ['albilad', 'ALBILAD', 'Albilad', 'AlBilad']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
           return column
        else:
           for i in file.columns:
              if 'albilad' in str(i).lower().strip():
                  column = i
                  break
              else:
                  column = ''

        return column

    def albilad_pure_finder(file: pd.DataFrame):
        column = ''
        for i in ['albilad pure', 'ALBILAD PURE', 'Albilad Pure', 'AlBilad Pure', 'Albilad Pure']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
           return column
        else:
           for i in file.columns:
              if 'albilad pure' in str(i).lower().strip():
                  column = i
                  break
              else:
                  column = ''

        return column

    def aaoifi_finder(file:pd.DataFrame):
        column = ''
        for i in [x for x in file.columns[file.columns.str.contains('AAOIFI', case=False)]]:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'aaoifi' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''

        return column

    def aaoifi_albilad_finder(file:pd.DataFrame):
        column = ''
        for i in ['AAOIFI Albilad', 'AAOIFI ALBILAD', 'AAOIFI AlBilad']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'aaoifi' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''

        return column

    def finder_ratios_alrajhi(file:pd.DataFrame):
        ratios_columns = []
        for i in file.columns:
            if ('[al-rajhi]' in str(i).lower().strip() or '[alrajhi pure]' in str(
                    i).lower().strip()) and 'ratio' in str(i).lower().strip() and 'country' not in str(
                    i).lower().strip():
                ratios_columns.append(i)
            else:
                pass
        return ratios_columns

    def alrajhi_finder(file: pd.DataFrame):
        column = ''
        for i in file.columns:
            if 'al-rajhi' in str(i).lower().strip() and '[' not in str(i):
                column = i
                break
            else:
                column = ''
        return column

    def alrajhipure_finder(file: pd.DataFrame):
        column = ''
        for i in file.columns:
            if 'alrajhi pure' in str(i).lower().strip() and '[' not in str(i) or\
               'al rajhi pure' in str(i).lower().strip() and '[' not in str(i) or\
               'Al Rajhi Pure' in str(i).strip():
                column = i
                break
            else:
                column = ''
        return column

    def finder_ratios_fransi(file: pd.DataFrame):
        ratios_columns = []
        for i in file.columns:
            if ('[fransicapital]' in str(i).lower().strip() or '[fransicapital]' in str(
                    i).lower().strip()) and 'total' not in str(i).lower().strip():
                ratios_columns.append(i)
            else:
                pass
        return ratios_columns

    def nonperm_income_finder(file: pd.DataFrame):
        column = ''

        for i in file.columns:
            if 'income' in str(i).lower().strip():
                column = i
                break
            else:
                column = ''
        return column

    def alrajhi_ba_finder(file: pd.DataFrame):
        column = ''
        for i in ['Al Rajhi Business Activity ', 'Al Rajhi Business Activity']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'business activity' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''
            return column

    def alrajhi_brok_finder(file: pd.DataFrame):
        column = ''
        for i in ['Al Rajhi Brokerage Alternative ', 'Al Rajhi Brokerage Alternative']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
            return column
        else:
            for i in file.columns:
                if 'brokerage alternative' in str(i).lower().strip():
                    column = i
                    break
                else:
                    column = ''
            return column

    def purification_finder(file: pd.DataFrame):
        purification_columns = []
        for i in file.columns:
            if 'purification' in str(i).lower().strip():
                purification_columns.append(i)
            else:
                pass
        return purification_columns


    def albilad_old_finder(file: pd.DataFrame):
        column = ''
        for i in ['Albilad Old Methodology', 'ALBILAD OLD METHODOLOGY', 'albilad old methodology']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
           return column
        else:
           for i in file.columns:
              if 'albilad old methodology' in str(i).lower().strip():
                  column = i
                  break
              else:
                  column = ''

        return column

    def albilad_amc_finder(file: pd.DataFrame):
        column = ''
        for i in ['Albilad (A/MC)', 'ALBILAD (A/MC)', 'albilad (a/mc)', 'Albilad(A/MC)']:
            if i in file.columns:
                column = i
            else:
                pass

        if len(column) > 0:
           return column
        else:
           for i in file.columns:
              if 'albilad (a/mc)' in str(i).lower().strip():
                  column = i
                  break
              else:
                  column = ''

        return column