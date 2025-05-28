import streamlit as st
from datetime import datetime
from dateutil import relativedelta
from myproject.processingfunctions import *
from auth import check_auth, authenticator

st.set_page_config(
        page_title="GCC Deliveries",
        layout="wide",
        initial_sidebar_state="auto"
    )

if not check_auth():
    st.warning("Please log in to access the application")
    st.stop()  # Stop execution if not authenticated

# Show sidebar only when authenticated
st.sidebar.markdown("## Navigation")
authenticator.logout(key="Logout", location="sidebar")

# Clear session state
def clear_results():
    keys_to_delete = ['new_df', 'final_old_df', 'raw_old_df', 'albilad_old_df', 'amc_old_df']
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
# def load_config():
#     with open('deliveries_config3.json', 'r') as f:
#         return json.load(f)
def load_config():
    # For cloud deployment, you have several options:
    # 1. Package the config file with your app
    # 2. Store it in a cloud storage bucket
    # 3. Use environment variables

    # Option 1: Package with app (recommended for simplicity)
    try:
        # This path works in Streamlit Cloud when file is in same directory
        with open('deliveries_config3.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("Configuration file not found. Please ensure deliveries_config3.json is in the root directory.")
        return {"deliveries": []}

# Main logic for file uploads and previews
def main_logic():
    # st.set_page_config(
    #     page_title="GCC Deliveries",
    #     layout="wide",
    #     initial_sidebar_state="auto"
    # )

    st.title("GCC Deliveries")
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

    period_end_date = st.date_input("Enter Period End Date", format="MM/DD/YYYY")
    date_extraction = st.date_input('Enter Date of Extraction', format="MM/DD/YYYY")

    rb = st.text_input("RuleBook")
    st.session_state.rb = rb

    compliant_only = st.checkbox('Compliant Only')

    # Additional validation for these clients
    # if old_file and delivery_name in ['AlBilad', 'AlRajhi']:
    #     if delivery_name == 'AlBilad':
    #         required_sheets = {'Blacklisted AAOIFI', 'AAOIFI', 'Albilad', 'Albilad Pure'}
    #     elif delivery_name == 'AlRajhi':
    #         required_sheets = {'Final', 'AlRajhi Pure List'}
    #     else:
    #         required_sheets = None

    st.session_state.old_file = old_file
    st.session_state.extract = extraction

    if extraction:
        new_df = read_new_function(extraction)
        new_df.fillna("", inplace=True)
        new_df.columns = new_df.columns.str.strip()
        date_current_price = Finders.date_current_price_finder(new_df)
        new_df = new_df.rename(columns={date_current_price: 'Date Of Current Price'})
        new_df['Name'] = new_df['Name'].str.strip()
        st.session_state.new_df = new_df
        st.write("### Extraction Preview")
        st.dataframe(new_df)

    if old_file and delivery_name not in ['AlBilad GCC']:
        old_df = read_excel_sheet_func(old_file, 'Final')
        old_df.fillna("", inplace=True)
        st.session_state.old_df = old_df
        st.write("### Old File Preview")
        st.dataframe(old_df)

        raw_old_df = read_excel_sheet_func(old_file, 'Universe')
        raw_old_df.fillna("", inplace=True)
        st.session_state.raw_old_df = raw_old_df
        st.write("### Raw Old File Preview")
        st.dataframe(raw_old_df)

    elif old_file and delivery_name == 'AlBilad GCC':
        # old_df = read_excel_sheet_func(old_file, 'Final')
        # old_df.fillna("", inplace=True)
        # st.session_state.old_df = old_df
        # st.write("### Old File Preview")
        # st.dataframe(old_df)
        raw_old_df = read_excel_sheet_func(old_file, 'Universe')
        raw_old_df.fillna("", inplace=True)
        albilad_old_file = read_excel_sheet_func(old_file, sheet='Albilad-Albilad Old')
        albilad_old_file.fillna("", inplace=True)
        amc_old_file = read_excel_sheet_func(old_file, sheet='Max (Assets-Market Cap)')
        amc_old_file.fillna("", inplace=True)
        st.session_state.raw_old_df = raw_old_df
        st.session_state.albilad_old_df = albilad_old_file
        st.session_state.amc_old_df = amc_old_file
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
        if delivery_name == 'AlBilad GCC':
            processor = Deliveries_Processing(
                raw_file=st.session_state.new_df,
                raw_old_file=st.session_state.raw_old_df,
                rb=st.session_state.rb,
                delivery_name='AlBilad GCC'
            )

            # AlBilad-specific processing
            universe['Extraction'] = st.session_state.new_df
            universe['Old'] = st.session_state.raw_old_df

            # Apply filters and processing
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)

            universe['Universe'] = Deliveries_Processing(raw_file=universe['Universe']).purification_status()

            universe['Old'] = Deliveries_Processing(st.session_state.raw_old_df).purification_status()

            universe['Pure_universe'] = Deliveries_Processing(raw_file=universe['Universe']).pure_companies()
            universe['Pure_old'] = Deliveries_Processing(raw_file=universe['Old']).pure_companies()

            universe['Excluded_Pure'], universe['Included_Pure'], universe['Error_Pure'] = \
                Deliveries_Processing(raw_file=universe['Pure_universe'],
                                      final_old_file=universe['Pure_old']).included_and_excluded_error()

            universe['Purification Ratio check'] = Deliveries_Processing(raw_file=universe['Universe']).purification_ratio_check()

            universe['CC_Albilad'], universe['CC_Albilad Old Methodology'], \
                universe['CC_Albilad Pure'], universe['CC_Albilad (A-MC)']= Deliveries_Processing(raw_file=universe['Universe'],
                                      raw_old_file=universe['Old']).comparison_albiladgcc()

            universe['Excluded'], universe['Included'], universe['Error'] = \
                Deliveries_Processing(raw_file=universe['Universe'],
                                      final_old_file=universe['Old']).included_and_excluded_error()

            universe['Albilad-Albilad Old'] = universe['Universe']
            universe['Max (Assets-Market Cap)'] = universe['Universe']
            new_universe = universe

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(universe, delivery_config)

        elif delivery_name == 'AlRajhi GCC':
            processor = Deliveries_Processing(
                raw_file=st.session_state.new_df,
                final_old_file=st.session_state.old_df,
                raw_old_file=st.session_state.raw_old_df,
                rb=st.session_state.rb
            )

            universe['Extraction'] = st.session_state.new_df
            universe['Old'] = st.session_state.old_df

            # Apply processing
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)

            alrajhi_rb = Finders.alrajhi_finder(universe['Extraction'])
            universe['Change Compliance'] = Deliveries_Processing(raw_file=universe['Universe'],
                                                                  raw_old_file=st.session_state.raw_old_df,
                                                                  rb=alrajhi_rb).comparison_alrajhi()
            alrajhi_ba = Finders.alrajhi_ba_finder(universe['Extraction'])
            universe['CC AlRajhi BA'] = Deliveries_Processing(raw_file=universe['Universe'],
                                                              raw_old_file=st.session_state.raw_old_df,
                                                              rb=alrajhi_ba).comparison_rb()

            alrajhi_brok = Finders.alrajhi_brok_finder(universe['Extraction'])
            universe['CC AlRajhi Brokerage'] = Deliveries_Processing(raw_file=universe['Universe'],
                                                              raw_old_file=st.session_state.raw_old_df,
                                                              rb=alrajhi_brok).comparison_rb()

            universe['Excluded'], universe['Included'], universe['Error'] = Deliveries_Processing(raw_file=universe['Universe'],
                                                                               final_old_file=st.session_state.old_df).included_and_excluded_error()

            universe['Final'] = universe['Universe']

            new_universe = universe

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(universe, delivery_config)

        elif delivery_name == 'NBK MENA':
            processor = Deliveries_Processing(
                raw_file=st.session_state.new_df,
                final_old_file=st.session_state.old_df,
                raw_old_file=st.session_state.raw_old_df,
                rb=st.session_state.rb
            )
            universe['Extraction'] = st.session_state.new_df
            universe['Old'] = st.session_state.old_df
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)

            nations = Deliveries_Processing(raw_file=universe['Universe']).divide_nations()

            universe['Saudi'] = Deliveries_Processing(raw_file=nations[0], rb='ASRHC Saudi').compliance_rb()

            universe['Mena ex-Saudi'] = Deliveries_Processing(raw_file=nations[1], rb='ASRHC Global').compliance_rb()

            universe['Change Compliance_MENA'] = Deliveries_Processing(raw_file=nations[1],
                                                                       raw_old_file=st.session_state.raw_old_df
                                                                       , rb='ASRHC Global').comparison_rb()
            print(universe['Saudi'])

            universe['Change Compliance_Saudi'] = Deliveries_Processing(raw_file=nations[0],
                                                                       raw_old_file=st.session_state.raw_old_df,
                                                                        rb='ASRHC Saudi').comparison_rb()

            cc = pd.concat([universe['Change Compliance_MENA'], universe['Change Compliance_Saudi']])
            cc = cc.drop_duplicates(keep='first')
            cc_dict = cc.to_dict(orient='index')

            compliance = Deliveries_Processing(raw_file=universe['Universe']).compliance_nbk_mena()

            universe['Excluded'], universe['Included'], universe['Error'] = Deliveries_Processing(
                raw_file=compliance, final_old_file=st.session_state.old_df).included_and_excluded_error()

            universe['Included'] = Deliveries_Processing(raw_file=universe['Included']).included_comments_nbk(cc_dict=cc_dict)

            universe['Final'] = compliance
            new_universe = universe

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(new_universe, delivery_config)

        elif delivery_name == 'Introspect MENA':
            processor = Deliveries_Processing(
                raw_file=st.session_state.new_df,
                final_old_file=st.session_state.old_df,
                raw_old_file=st.session_state.raw_old_df,
                rb=st.session_state.rb
            )
            universe['Extraction'] = st.session_state.new_df
            universe['Old'] = st.session_state.old_df
            universe['Universe'], universe['Removed'] = processor.functions_applications(delivery_config)

            duplicated_isin = Deliveries_Processing(raw_file=universe['Universe']).remove_duplicate_isin()[1]
            check_dup = Deliveries_Processing(raw_file=universe['Universe']).duplicated_names(duplicates=duplicated_isin)
            dup = check_dup[0]
            dup_df = pd.DataFrame.from_dict(dup, orient='index')
            universe['Duplicates'] = dup_df
            universe['Companies to check'] = pd.DataFrame.from_dict(check_dup[1], orient='index')

            universe['Change Compliance'] = Deliveries_Processing(raw_file=universe['Universe'],
                                                                  final_old_file=st.session_state.raw_old_df).comparison_introspect()

            universe['Compliance Ratio Check'] = Deliveries_Processing(raw_file=universe['Universe']
                                                                       ).compliance_ratio_check_introspect()

            universe['Excluded'], universe['Included'], universe['Error'] = \
                Deliveries_Processing(raw_file=universe['Universe'], final_old_file=universe['Old']
                                      ).included_and_excluded_error()

            universe['Final'] = universe['Universe']

            new_universe = universe

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(new_universe, delivery_config)
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

            universe['Change Compliance'] = Deliveries_Processing(raw_file=universe['Universe'], raw_old_file=st.session_state.raw_old_df, rb=st.session_state.rb).comparison_rb()
            if delivery_name == 'AlRayan GCC':
                new_universe_df = Deliveries_Processing(raw_file=universe['Universe']).alrayan_purif()

            universe['Final'] = new_universe_df

            if delivery_config.get('other_functions', False):
                new_universe = Deliveryformatting.apply_other_functions(universe, delivery_config, rb=st.session_state.rb)
            else:
                new_universe = universe

            strategy = DeliveryStrategyFactory.get_strategy(delivery_name)
            deliveries_files = strategy.prepare_output(new_universe, delivery_config)

            print(f"in all deliveries delivery files is {deliveries_files}")

        raw_file = writing(new_universe)

        if delivery_name == 'Introspect MENA':
            final_file = writing_introspect_url(deliveries_files, client=delivery_name, delivery_config=delivery_config)
        elif delivery_name == "Alpha Capital GCC":
            final_file = writing_delivery_alphagcc_url(deliveries_files, client=delivery_name, delivery_config=delivery_config)
        elif delivery_name == "AlRayan GCC":
            final_file = writing_rayangcc_delivery_url(deliveries_files, client=delivery_name,
                                                       delivery_config=delivery_config)
        elif delivery_name == 'AlRajhi GCC':
            final_file = writing_delivery_alrajhigcc_url(deliveries_files, client=delivery_name,
                                                         delivery_config=delivery_config)
        else:
            final_file = writing_delivery(deliveries_files, client=delivery_name, delivery_config=delivery_config)

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