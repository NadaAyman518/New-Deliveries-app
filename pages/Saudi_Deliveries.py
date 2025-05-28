import streamlit as st
import pandas as pd
import json
from datetime import datetime
import dateutil
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta

from myproject.functions import Deliveryformatting # Assuming you have a DeliveryConfig class
import io
from myproject.processingfunctions import *

from auth import check_auth, authenticator
import io

# Must be first
st.set_page_config(
        page_title="Saudi Deliveries",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

if not check_auth():
    st.warning("Please log in to access the application")
    st.stop()  # Stop execution if not authenticated

# Show sidebar only when authenticated
st.sidebar.markdown("## Navigation")
authenticator.logout(key="Logout", location="sidebar")
# Clear session state
def clear_results():
    if "extraction" in st.session_state or "old_file" in st.session_state:
        keys_to_delete = ['new_df', 'final_old_df', 'raw_old_df']
        for key in keys_to_delete:
            if key in st.session_state:
                del st.session_state[key]

def inject_session_dates(filters):
    for f in filters:
        if f["function"] in ["period_end_date_except_reits", 'period_end_date_saudireits', 'period_end_date']:
            f["args"]["Period End Date"] = st.session_state.period_end_date.strftime('%Y-%m-%d')
        if f["function"] in ["date_of_current_price", 'date_of_current_price_except_reits', 'date_of_current_price_saudireits']:
            f["args"]["Date Of Current Price"] = st.session_state.date_extraction.strftime('%Y-%m-%d')
    return filters

# Load delivery configuration
def load_config():
    try:
        with open('deliveries_config2.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("Configuration file not found. Please ensure deliveries_config3.json is in the root directory.")
        return {"deliveries": []}

# Map function names to their implementations
# FUNCTION_MAP = {
#     "issue_active": issue_active_status,
#     'ticker_isin_null': ticker_isin_null,
#     "market_cap": market_cap}

def process_file(uploaded_file, sheet_name=None):
    """Process uploaded files with error handling and memory management"""
    if uploaded_file is None:
        return None
    file_bytes = uploaded_file.getvalue()
    try:
        # Read file based on extension
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_bytes))
        elif uploaded_file.name.endswith(('.xlsx', '.xls')):
            if sheet_name:
                df = read_excel_sheet_func(io.BytesIO(file_bytes), sheet=sheet_name)
            else:
                df = read_new_function(io.BytesIO(file_bytes))
        else:
            st.error("Unsupported file format")
            return None

        # Clean the dataframe
        df.fillna("", inplace=True)
        df.columns = df.columns.str.strip()
        if 'Name' in df.columns:
            df['Name'] = df['Name'].str.strip()

        return df

    except Exception as e:
        st.error(f"Error processing {uploaded_file.name}: {str(e)}")
        return None


# Main logic for file uploads and previews
def main_logic():

    st.title("Saudi Deliveries")
    st.write("Upload the extraction file and apply processing functions.")

    # Load configuration
    config = load_config()
    delivery_name = st.selectbox(
        "Choose a delivery:",
        options=[delivery["name"] for delivery in config["deliveries"]]
    )

    # Get the selected delivery's configuration
    delivery_config = next(
        (delivery for delivery in config["deliveries"] if delivery["name"] == delivery_name),
        None
    )

    if not delivery_config:
        st.error("Delivery configuration not found.")
        return

    # Display delivery details
    # st.write(f"### Selected Delivery: {delivery_name}")
    # st.write(f"**Input Files Required:** {delivery_config['input_files']}")
    # st.write(f"**Functions to Apply:** {delivery_config['pre_processing']}")
    # st.write(f"**Filters to Apply:** {delivery_config['filters']}")
    # st.write(f"**Output Columns:** {delivery_config['output_columns']}")

    # File uploader
    if delivery_name != 'Saudi Fransi':
        extraction = st.file_uploader(
            "Upload the extraction file (CSV or Excel)", type=["csv", "xlsx"], key="extraction", on_change=clear_results)
        if delivery_name in ['AlBilad', 'AlRajhi']:
            with st.expander(f"Special requirements for {delivery_name}"):
                if delivery_name == 'AlBilad':
                    st.markdown("""
                    **Required Sheets:**
                    - Blacklisted AAOIFI (Extraction)
                    - AAOIFI (Raw Old File)
                    - Albilad  (Raw Old File)
                    - Albilad Pure (Raw Old File)
                    """)
                elif delivery_name == 'AlRajhi':
                    st.markdown("""
                    **Required Sheets in raw old file:**
                    - Final 
                    - AlRajhi Pure List
                    """)

        old_file = st.file_uploader("Upload raw old file (CSV or Excel)", type=["csv", "xlsx"], on_change=clear_results)
    else:
        extraction = st.file_uploader(
                "Upload Saudi extraction file (CSV or Excel)", type=["csv", "xlsx"], key="extraction",
                on_change=clear_results)
        st.session_state.us_file = st.file_uploader(
            "Upload US extraction file (CSV or Excel)",
            type=["csv", "xlsx"],
            key="us_file_uploader",
            on_change=clear_results
        )

        st.session_state.gcc_file = st.file_uploader(
            "Upload GCC extraction file (CSV or Excel)",
            type=["csv", "xlsx"],
            key="gcc_file_uploader",
            on_change=clear_results
        )

        old_file = st.file_uploader(
            "Upload Saudi old file (CSV or Excel)",
            type=["csv", "xlsx"],
            on_change=clear_results
        )

        st.session_state.us_old_file = st.file_uploader(
            "Upload US old file (CSV or Excel)",
            type=["csv", "xlsx"],
            key="us_old_file_uploader",
            on_change=clear_results
        )

        st.session_state.gcc_old_file = st.file_uploader(
            "Upload GCC old file (CSV or Excel)",
            type=["csv", "xlsx"],
            key="gcc_old_file_uploader",
            on_change=clear_results)

    period_end_date = st.date_input("Enter Period End Date", format="MM/DD/YYYY")
    date_extraction = st.date_input('Enter Date of Extraction', format="MM/DD/YYYY")

    rb = st.text_input("RuleBook")
    st.session_state.rb = rb

    compliant_only = st.checkbox('Compliant Only')

    # Additional validation for these clients
    if old_file and delivery_name in ['AlBilad', 'AlRajhi']:
        if delivery_name == 'AlBilad':
            required_sheets = {'Blacklisted AAOIFI', 'AAOIFI', 'Albilad', 'Albilad Pure'}
        elif delivery_name == 'AlRajhi':
            required_sheets = {'Final', 'AlRajhi Pure List'}
        else:
            required_sheets = None

    st.session_state.old_file = old_file
    st.session_state.extract = extraction

    if extraction:
        # new_df = read_new_function(extraction)
        # new_df.fillna("", inplace=True)
        # new_df.columns = new_df.columns.str.strip()
        new_df = process_file(extraction)
        date_current_price = Finders.date_current_price_finder(new_df)
        new_df = new_df.rename(columns={date_current_price: 'Date Of Current Price'})
        new_df['Name'] = new_df['Name'].str.strip()
        st.session_state.new_df = new_df
        st.write("### Extraction Preview")
        st.dataframe(new_df)

    if old_file and delivery_name not in ['AlBilad', 'Saudi Fransi']:
        # old_df = read_excel_sheet_func(old_file, 'Final')
        old_df = process_file(old_file, 'Final')
        old_df.fillna("", inplace=True)
        st.session_state.old_df = old_df
        st.write("### Old File Preview")
        st.dataframe(old_df)

    if delivery_name not in ['Saudi Fransi']:
        # raw_old_df = read_excel_sheet_func(old_file, 'Universe')
        raw_old_df = process_file(old_file, 'Universe')
        raw_old_df.fillna("", inplace=True)
        st.session_state.raw_old_df = raw_old_df
        st.write("### Raw Old File Preview")
        st.dataframe(raw_old_df)

    if period_end_date:
        st.session_state.period_end_date = period_end_date
    if date_extraction:
        st.session_state.date_extraction = date_extraction

    delivery_config["filters"] = inject_session_dates(delivery_config["filters"])

    year = datetime.date.today().year
    day = f'{datetime.date.today().day:02d}'
    month_f = f'{datetime.date.today().month:02d}'
    today = datetime.date.today() + dateutil.relativedelta.relativedelta(days=30)
    next_month = f'{today.month:02d}'
    quarter = pd.Timestamp(datetime.date.today()).quarter

    if st.button('Generate Output'):
        # Helper function to read files
        universe = {}

        pure_delivery_file = {}
        us_delivery_file = {}
        gcc_delivery_file = {}
        delivery_all_file = {}
        delivery_file = {}
        delivery = {}
        delivery_us = {}
        delivery_gcc = {}
        new_universe = {}
        # Initialize processing based on delivery type
        if delivery_name == 'AlBilad':
            processor = Deliveries_Processing(
                raw_file=st.session_state.extract,
                raw_old_file=st.session_state.old_file,
                rb=st.session_state.rb,
                delivery_name='AlBilad'
            )

            # AlBilad-specific processing
            universe['Extraction'] = st.session_state.new_df
            universe['Blacklist'] = read_excel_sheet_func(st.session_state.extraction, 'Blacklisted AAOIFI')
            universe['Old_AAOIFI'] = read_excel_sheet_func(st.session_state.old_file, 'AAOIFI')
            universe['Old_Albilad'] = read_excel_sheet_func(st.session_state.old_file, 'Albilad')
            universe['Old_Albilad_Pure'] = read_excel_sheet_func(st.session_state.old_file, 'Albilad Pure')

            # Apply filters and processing
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)
            universe['Change Compliance_Albilad'], universe['Change Compliance_AAOIFI'], \
                universe['Change Compliance_Albilad Pure'] = Deliveries_Processing(raw_file=universe['Universe'],
                                      raw_old_file=st.session_state.raw_old_df).comparison_albilad()

            universe['AAOIFI'], universe['Albilad'], universe['Albilad Pure'] = \
                Deliveries_Processing(raw_file=universe['Universe']).compliance_status_albilad()

            universe['Excluded Albilad'], universe['Included Albilad'], universe['Error Albilad'] = \
                Deliveries_Processing(raw_file=universe['Albilad'],
                                      final_old_file=universe['Old_Albilad']).included_and_excluded_error()

            universe['Excluded AAOIFI'], universe['Included AAOIFI'], universe['Error AAOIFI'] = \
                Deliveries_Processing(raw_file=universe['AAOIFI'],
                                      final_old_file=universe['Old_AAOIFI']).included_and_excluded_error()

            universe['Excluded Albilad Pure'], universe['Included Albilad Pure'], universe['Error Albilad Pure'] = \
                Deliveries_Processing(raw_file=universe['Albilad Pure'],
                                      final_old_file=universe['Old_Albilad_Pure']).included_and_excluded_error()

            new_universe_df = universe['Universe']
            new_universe = universe

            # strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            # delivery_file = strategy.prepare_output(universe, delivery_config, new_universe_df)
            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(universe, delivery_config)

        elif delivery_name == 'AlRajhi':
            processor = Deliveries_Processing(
                raw_file=st.session_state.new_df,
                final_old_file=st.session_state.old_df,
                raw_old_file=st.session_state.raw_old_df,
                rb=st.session_state.rb
            )

            universe['Extraction'] = st.session_state.new_df
            universe['Old'] = st.session_state.old_df
            universe['Old_Pure'] = read_excel_sheet_func(st.session_state.old_file, 'AlRajhi Pure List')

            # Apply processing
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)

            alrajhi_rb = Finders.alrajhi_finder(universe['Extraction'])
            universe['Change Compliance'] = Deliveries_Processing(raw_file=universe['Universe'], raw_old_file=st.session_state.raw_old_df,
                                                   rb=alrajhi_rb).comparison_rb()

            universe['Excluded'], universe['Included'], universe['Error'] = Deliveries_Processing(raw_file=universe['Universe'],
                                                                               final_old_file=st.session_state.old_df).included_and_excluded_error()
            alrajhi_pure = Finders.alrajhipure_finder(universe['Extraction'])
            universe['AlRajhi Pure List'] = Deliveries_Processing(raw_file=universe['Universe'], raw_old_file=st.session_state.raw_old_df,
                                                   rb=alrajhi_pure).compliance_rb()

            universe['Excluded Pure'], universe['Included Pure'], universe['Error Pure'] = Deliveries_Processing(raw_file=universe['AlRajhi Pure List'],
                                                                               final_old_file=universe['Old_Pure']).included_and_excluded_error()

            print(f"al rajhi pure rulebook is {alrajhi_pure}")
            universe['Pure CC'] = Deliveries_Processing(raw_file=universe['Universe'], raw_old_file=st.session_state.raw_old_df,
                                                   rb=alrajhi_pure).comparison_rb()

            universe['Final'] = universe['Universe']

            new_universe_df = universe['Universe']
            pure_universe_df = universe['AlRajhi Pure List']
            new_universe = universe

            # pure_delivery_file['AlRajhi Pure List'] = pure_universe_df
            # pure_delivery_file['AlRajhi Pure List'] = pure_delivery_file['AlRajhi Pure List'][["Name",
            #   "ISIN",
            #   "Ticker",
            #   "Nation"]]
            # pure_delivery_file['AlRajhi Pure List'].sort_values(by='Name', inplace=True, ascending=True,
            #                                                    key=lambda col: col.str.lower())

            # strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            # delivery_file = strategy.prepare_output(universe, delivery_config, new_universe_df)
            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(universe, delivery_config)

        elif delivery_name == "Saudi Fransi":

            # processor = Deliveries_Processing(
            #     raw_file=st.session_state.extract,
            #     final_old_file=st.session_state.old_file,
            #     us_file=st.session_state.us_file,
            #     gcc_file=st.session_state.gcc_file,
            #     old_file_us=st.session_state.us_old_file,
            #     old_file_gcc=st.session_state.gcc_old_file,
            #     rb=st.session_state.rb
            # )

            # Process the main files
            # universe['Extraction'] = st.session_state.new_df
            # universe['Old'] = st.session_state.old_df
            universe['Saudi Extraction'] = read_new_function(st.session_state.extract)
            universe['US Extraction'] = read_new_function(st.session_state.us_file)
            universe['GCC Extraction'] = read_new_function(st.session_state.gcc_file)

            old_saudi = read_new_function(st.session_state.old_file)
            old_us = read_new_function(st.session_state.us_old_file)
            old_gcc = read_new_function(st.session_state.gcc_old_file)

            # Apply processing functions
            universe['Saudi (SAR)'], universe['Removed_Saudi (SAR)'] = Deliveries_Processing(raw_file=st.session_state.extract,
                                final_old_file=old_saudi, rb=st.session_state.rb, delivery_name='Saudi Fransi').functions_applications(delivery_config)

            universe['Saudi (USD)'], universe['Removed_Saudi (USD)'] = Deliveries_Processing(raw_file=st.session_state.us_file,
                                final_old_file=old_us, rb=st.session_state.rb, delivery_name='Saudi Fransi').functions_applications(delivery_config)

            universe['GCC ex-Saudi'], universe['Removed_GCC ex-Saudi'] = Deliveries_Processing(raw_file=st.session_state.gcc_file,
                                final_old_file=old_gcc, rb=st.session_state.rb, delivery_name='Saudi Fransi').functions_applications(delivery_config)

            # Add RIC codes and process each region
            for region in ['Saudi (SAR)', 'Saudi (USD)', 'GCC ex-Saudi']:
                universe[region] = Functions.ric_code(universe[region])

                # Convert to dict for processing
                universe_dict = universe[region].to_dict('index')
                universe_dict = Functions.fiscal_period_to_period_end_date(
                    new_file=universe_dict,
                    file=universe[region]
                )
                universe_dict = Functions.adjust_ratios_fransi(
                    new_file=universe_dict,
                    file=universe[region]
                )
                universe_dict = Functions.clean_ratios_fransi(
                    new_file=universe_dict,
                    file=universe[region]
                )

                # Convert back to DataFrame
                universe[region] = pd.DataFrame.from_dict(universe_dict, orient='index')

                # Apply currency formatting for specific columns
                columns_to_format = [
                    'Receivables Total', 'Cash and Cash Equivalents Total',
                    'Debt Conv', 'Cash and Short Term Inv Conv',
                    '[FransiCapital] Total NPIN (1)',
                    'Trailing 12 Months Market Cap Most Recent Statement',
                    'Income Total'
                ]

                for col in columns_to_format:
                    if col in universe[region].columns:
                        if region == 'Saudi (SAR)':
                            universe[region][col] = universe[region][col].apply(
                                Functions.safe_format_currency_sar)
                        else:
                            universe[region][col] = universe[region][col].apply(
                                Functions.safe_format_currency_us)

            universe['CC_Saudi'] = Deliveries_Processing(raw_file=st.session_state.extract,
                                                         final_old_file=old_saudi, delivery_name='Saudi Fransi').comparison_fransi()

            universe['CC_US'] = Deliveries_Processing(raw_file=st.session_state.us_file,
                                                         final_old_file=old_us, delivery_name='Saudi Fransi').comparison_fransi()

            universe['CC_GCC'] = Deliveries_Processing(raw_file=st.session_state.gcc_file,
                                                         final_old_file=old_gcc, delivery_name='Saudi Fransi').comparison_fransi()

            universe['Excluded_Saudi'], universe['Included_Saudi'], universe['Error'] = Deliveries_Processing(raw_file=universe['Saudi (SAR)'],
                                                                              final_old_file=old_saudi).included_and_excluded_error()

            universe['Excluded_US'], universe['Included_US'], universe['Error'] = Deliveries_Processing(
                raw_file=universe['Saudi (USD)'],
                final_old_file=old_us).included_and_excluded_error()

            universe['Excluded_GCC'], universe['Included_GCC'], universe['Error'] = Deliveries_Processing(
                raw_file=universe['GCC ex-Saudi'],
                final_old_file=old_gcc).included_and_excluded_error()

            # Prepare the final delivery files
            delivery_all_fransi = {
                'Saudi (SAR)': universe['Saudi (SAR)'].iloc[:, 1:18],
                'Saudi (USD)': universe['Saudi (USD)'].iloc[:, 1:18],
                'GCC ex-Saudi': universe['GCC ex-Saudi'].iloc[:, 1:18]
            }

            # Sort each dataframe by Name
            for region in delivery_all_fransi:
                delivery_all_fransi[region].sort_values(
                    by=['Name'],
                    ascending=True,
                    inplace=True,
                    key=lambda col: col.str.lower()
                )
            new_universe = universe
            # delivery_all_file = writing_delivery(delivery_all_fransi, client=delivery_name,
            #                                                              delivery_config=delivery_config)
            # # delivery_all_file_name = f"Saudi_GCC_Securities_Q{quarter}_{year}.xlsx"
            # print(f"alldelivery file name: {delivery_all_file_name}")

            # Generate the output files
            # new_universe_df = universe['Saudi (SAR)']
            # new_universe_us = universe['Saudi (USD)']
            # new_universe_gcc = universe['GCC ex-Saudi']

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(universe, delivery_config)
            # us_delivery_file = strategy.prepare_output(universe, delivery_config, new_universe_us)
            # gcc_delivery_file = strategy.prepare_output(universe, delivery_config, new_universe_gcc)

            # print(f"deliveries files : {delivery_file}, {us_delivery_file}, {gcc_delivery_file}")

            # delivery['Saudi (SAR)'] = universe['Saudi (SAR)'].iloc[:, 1:18]
            # delivery['Saudi (SAR)'].sort_values(by=['Name'], ascending=True, inplace=True,
            #     key=lambda col: col.str.lower())
            # delivery_us['Saudi (USD)'] = universe['Saudi (USD)'].iloc[:, 1:18]
            # delivery_us['Saudi (USD)'].sort_values(by=['Name'], ascending=True, inplace=True,
            #                                     key=lambda col: col.str.lower())
            # delivery_gcc['GCC ex-Saudi'] = universe['GCC ex-Saudi'].iloc[:, 1:18]
            # delivery_gcc['GCC ex-Saudi'].sort_values(by=['Name'], ascending=True, inplace=True,
            #                                        key=lambda col: col.str.lower())

        else:
            # Standard processing for other deliveries
            processor = Deliveries_Processing(
                raw_file=st.session_state.new_df,
                final_old_file=st.session_state.old_df,
                raw_old_file=st.session_state.raw_old_df,
                rb=st.session_state.rb
            )
            universe['Extraction'] = st.session_state.new_df
            universe['Old'] = st.session_state.old_df
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)

            if compliant_only:
                new_universe_df = Deliveries_Processing(raw_file=universe['Universe'], rb=st.session_state.rb).compliance_rb()
            else:
                new_universe_df = universe['Universe']
            universe['Excluded'], universe['Included'], universe['Error'] = Deliveries_Processing(raw_file=new_universe_df, final_old_file=st.session_state.old_df).included_and_excluded_error()
        # if delivery_name not in ['AlBilad']:
            universe['Change Compliance'] = Deliveries_Processing(raw_file=universe['Universe'], raw_old_file=st.session_state.raw_old_df, rb=st.session_state.rb).comparison_rb()
        # if delivery_name in ['ADIB']:
            # universe['Included CC'], universe['Excluded CC'] = Deliveries_Processing(raw_file=universe['CC'], rb=rb).excluded_included_compliance()
            universe['Final'] = new_universe_df

            if delivery_config.get('other_functions', False):
                new_universe = Deliveryformatting.apply_other_functions(universe, delivery_config, rb=st.session_state.rb)
            else:
                new_universe = universe

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(new_universe, delivery_config)

            print(f"in all deliveries delivery files is {deliveries_files}")

        raw_file = writing(new_universe)

        if delivery_name == 'Introspect Saudi':
            final_file = writing_introspect_url(deliveries_files, client=delivery_name, delivery_config=delivery_config)
        elif delivery_name == "Alpha Capital":
            # final_file, filename = writing_delivery_alpha_url(delivery_file, client=delivery_name)
            final_file = writing_delivery_alpha_url(deliveries_files, client=delivery_name)
        elif delivery_name == 'AlRajhi':
            final_file = writing_delivery(deliveries_files, client=delivery_name, delivery_config=delivery_config)
            # pure_final_file, pure_filename = writing_pure_alrajhi_url(pure_delivery_file, client=delivery_name,
            #                                                           delivery_config=delivery_config)
        else:
            final_file = writing_delivery(deliveries_files, client=delivery_name, delivery_config=delivery_config)

        # if delivery_name == 'AlRajhi':
        #     results_write = create_zip([raw_file, final_file, pure_final_file],
        #                                [f'{delivery_name}_Raw_File_{day}{month_f}{year}.xlsx', f"{filename}",
        #                                 f"{pure_filename}"])
        # else:
        results_write = create_zip(final_file, [raw_file, f'{delivery_name}_Raw_File_{day}{month_f}{year}.xlsx'])


        st.download_button(
            label="Download All Results as ZIP",
            data=results_write,
            file_name=f"Results {delivery_name}.zip",
            mime="application/zip"
        )


# Run the app
if __name__ == "__main__":
    main_logic()

