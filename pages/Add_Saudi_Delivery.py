import json
import streamlit as st
import logging
import boto3
from botocore.exceptions import ClientError
from functools import lru_cache
import re
from typing import Dict, List, Optional
import os
from auth import check_auth, authenticator

st.set_page_config(
    page_title="Delivery Configurator",
    layout="wide",
    initial_sidebar_state="expanded"
)

if not check_auth():
    st.warning("Please log in to access the application")
    st.stop()  # Stop execution if not authenticated

# Show sidebar only when authenticated
st.sidebar.markdown("## Navigation")
authenticator.logout(key="Logout", location="sidebar")
# def add_delivery_interface():
#     st.subheader("Add New Delivery Configuration")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Storage configuration
USE_S3 = os.getenv('USE_S3', 'false').lower() == 'true'
S3_BUCKET = os.getenv('S3_BUCKET', 'your-config-bucket')

# Constants
CONFIG_FILE = 'deliveries_config2.json'

def load_config() -> Dict:
    """Load configuration from appropriate backend"""
    if USE_S3:
        try:
            import boto3
            from botocore.exceptions import ClientError

            s3 = boto3.client('s3')
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=CONFIG_FILE)
                return json.loads(obj['Body'].read().decode('utf-8'))
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    return {"deliveries": []}
                raise
        except Exception as e:
            logger.warning(f"S3 load failed, falling back to local: {str(e)}")

    # Fallback to local file
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"deliveries": []}


def save_config(config: Dict) -> bool:
    """Save configuration to appropriate backend"""
    try:
        if USE_S3:
            import boto3
            s3 = boto3.client('s3')

            # Write to temporary location first
            temp_key = f"{CONFIG_FILE}.tmp"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=temp_key,
                Body=json.dumps(config, indent=2),
                ContentType='application/json'
            )

            # Atomic rename
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={'Bucket': S3_BUCKET, 'Key': temp_key},
                Key=CONFIG_FILE
            )
            s3.delete_object(Bucket=S3_BUCKET, Key=temp_key)
        else:
            # Local file with atomic write pattern
            temp_path = f"{CONFIG_FILE}.tmp"
            with open(temp_path, 'w') as f:
                json.dump(config, f, indent=2)

            # Atomic rename (works on Windows)
            if os.path.exists(CONFIG_FILE):
                os.remove(CONFIG_FILE)
            os.rename(temp_path, CONFIG_FILE)

        load_config.clear()  # Clear cache
        return True
    except Exception as e:
        logger.error(f"Failed to save configuration: {str(e)}")
        return False

# Validation Functions
def validate_delivery_name(name: str) -> bool:
    """Validate delivery name format"""
    if not re.match(r'^[a-zA-Z0-9_\- ]{2,50}$', name):
        raise ValueError(
            "Name must be 2-50 characters with only letters, numbers, spaces, hyphens and underscores"
        )
    return True

# def validate_filename_pattern(pattern: str) -> bool:
#     """Validate filename pattern contains required placeholders"""
#     required_placeholders = {'{client}', '{year}', '{month}', '{day}'}
#     placeholders = set(re.findall(r'\{(\w+)\}', pattern))
#     if not required_placeholders.issubset(placeholders):
#         missing = required_placeholders - placeholders
#         raise ValueError(f"Missing required placeholders: {', '.join(missing)}")
#     return True

FUNCTION_MAP = {
    "market_cap_filter": {
        "name": "Market Cap Filter",
        "function": "market_cap",
        "args": {"Market Cap": 5000000},
        "description": "Remove companies with market cap < 5M"
    },
    "issue_active_filter": {
        "name": "Issue Active Status",
        "function": "issue_active_status",
        "args": {},
        "description": "Remove issue active status = False"
    },
    "issue_active_xpt_reits_filter": {
        "name": "Issue Active Status (Except REITs)",
        "function": "issue_active_status_except_reits",
        "args": {},
        "description": "Remove issue active status = False except for REITs"
    },
    "ticker_isin_filter": {
        "name": "Ticker/ISIN Null Check",
        "function": "ticker_isin_null",
        "args": {},
        "description": "Remove blank Ticker and blank ISIN"
    },
    "ticker_null_filter": {
        "name": "Ticker Null Check",
        "function": "ticker_null",
        "args": {},
        "description": "Remove blank Ticker"
    },
    "isin_null_filter": {
        "name": "ISIN Null Check",
        "function": "isin_null",
        "args": {},
        "description": "Remove blank ISIN"
    },
    "period_end_date_filter": {
        "name": "Period End Date",
        "function": "period_end_date",
        "args": {"Period End Date": None},
        "description": "Remove old period end date"
    },
    "date_of_current_price_filter": {
        "name": "Current Price Date",
        "function": "date_of_current_price",
        "args": {"Date Of Current Price": None},
        "description": "Remove old date of current price"
    },
    "period_end_date_xpt_reits_filter": {
        "name": "Period End Date (Except REITs)",
        "function": "period_end_date_except_reits",
        "args": {"Period End Date": None},
        "description": "Remove old period end date except for REITs"
    },
    "period_end_date_xpt_saudireits_filter": {
        "name": "Period End Date (Except Saudi REITS)",
        "function": "period_end_date_saudireits",
        "args": {"Period End Date": None},
        "description": "Remove old period end date except for Saudi REITs"
    },
    "date_of_current_price_xpt_reits_filter": {
        "name": "Current Price Date (Except REITs)",
        "function": "date_of_current_price_except_reits",
        "args": {"Date Of Current Price": None},
        "description": "Remove old date of current price except for REITs"
    },
    "date_of_current_price_xpt_saudireits_filter": {
        "name": "Current Price Date (Except Saudi REITs)",
        "function": "date_of_current_price_saudireits",
        "args": {"Date Of Current Price": None},
        "description": "Remove old date of current price except for Saudi REITs"
    },
    "turn_failing_reits_into_passing": {
        "name": "REITs Passing Filter",
        "function": "passing_reits_filter",
        "args": {},
        "description": "Check for failing REITs and turn them into PASS"
    },
    "blank_identifiers_filter": {
        "name": "Blank Identifier",
        "function": "blank_identifiers",
        "args": {},
        "description": "Remove blank identifiers[ISIN, SEDOL, Ticker]"
    },
    "remove_failing_reits": {
        "name": "Remove Failing REITs",
        "function": "reits_filter",
        "args": {},
        "description": "Remove Failing REITs on AAOIFI and AlBilad Rulebooks"
    }
}

OTHER_FUNCTIONS = {
    "excluded_included_compliance": {
        "name": "Excluded/Included Compliance",
        "description": "Split into included/excluded based on compliance status",
        "inputs": ["CC", "rb"]
    }
}

#     with st.form("new_delivery_form"):
#         # Basic delivery info
#         delivery_name = st.text_input("Delivery Name*", help="Unique name for this delivery configuration")
#         input_files = st.number_input("Number of Input Files Required*", min_value=1, value=2)
#
#         # Number of output files
#         output_files_count = st.number_input("Number of Output Files*", min_value=1, value=1)
#
#         # Pre-processing functions
#         st.markdown("### Pre-processing Functions")
#         pre_processing = []
#         with st.expander("Add Pre-processing Function"):
#             function_name = st.selectbox(
#                 "Select Function",
#                 options=["ric_code", "replace", "other", "filter_blacklist"],
#                 help="Select a pre-processing function"
#             )
#
#             # Use form_submit_button for buttons inside forms
#             add_function = st.form_submit_button("Add Function")
#
#             if function_name == "ric_code":
#                 if add_function:
#                     pre_processing.append({"function": "ric_code"})
#                     st.success("Added RIC code function")
#             elif function_name == "filter_blacklist":
#                 if add_function:
#                     pre_processing.append({"function": "filter_blacklist_albilad"})
#                     st.success(f"Added filter_blacklist_albilad")
#
#         # Filters
#         st.markdown("### Filters")
#         st.write("Select which filters to apply:")
#
#         selected_filters = []
#         for key, filter_info in FUNCTION_MAP.items():
#             col1, col2 = st.columns([1, 4])
#             with col1:
#                 enabled = st.checkbox(f"Enable {filter_info['name']}", key=key)
#             with col2:
#                 st.caption(filter_info["description"])
#
#             if enabled:
#                 filter_config = {
#                     "function": filter_info["function"],
#                     "args": filter_info["args"].copy()
#                 }
#
#                 if filter_info["args"]:
#                     with st.expander(f"{filter_info['name']} Parameters"):  # Removed key parameter
#                         for arg_name, default_val in filter_info["args"].items():
#                             if arg_name in ["Period End Date", "Date Of Current Price"]:
#                                 st.write(f"{arg_name}: Will use the date selected in main interface")
#                             elif isinstance(default_val, (int, float)):
#                                 filter_config["args"][arg_name] = st.number_input(
#                                     arg_name,
#                                     value=default_val,
#                                     key=f"{key}_{arg_name}"  # Keep key for input elements
#                                 )
#                             else:
#                                 filter_config["args"][arg_name] = st.text_input(
#                                     arg_name,
#                                     value=str(default_val) if default_val is not None else "",
#                                     key=f"{key}_{arg_name}"  # Keep key for input elements
#                                 )
#
#                 selected_filters.append(filter_config)
#
#         # Other functions
#         st.markdown("### Other Processing Functions")
#         other_functions = []
#         for key, func_info in OTHER_FUNCTIONS.items():
#             if st.checkbox(f"Enable {func_info['name']}", key=f"other_{key}"):
#                 func_config = {
#                     "function": key,
#                     "sheets_names": ["Included CC", "Excluded CC"],
#                     "inputs": func_info["inputs"]
#                 }
#                 other_functions.append(func_config)
#
#         # Output files configuration
#         output_configs = []
#         for i in range(output_files_count):
#             # Use a container instead of expander for the main output file configuration
#             with st.container():
#                 st.subheader(f"Output File {i + 1} Configuration")
#
#                 # Filename pattern for this output file
#                 filename_pattern = st.text_input(
#                     f"Filename Pattern*",
#                     value="{client}_{year}{current_month}{day}.xlsx",
#                     key=f"filename_pattern_{i}"
#                 )
#
#                 # Output format for this file
#                 output_format = st.selectbox(
#                     "Output Format Type",
#                     options=["Delivery Sheet", "Multiple Sheets"],
#                     key=f"output_format_{i}"
#                 )
#
#                 # CSV option for this file
#                 csv_output = st.checkbox(
#                     "Generate CSV output",
#                     value=False,
#                     key=f"csv_output_{i}"
#                 )
#
#                 if output_format == "Delivery Sheet":
#                     # Single sheet configuration
#                     sheet_name = st.text_input("Sheet Name*", key=f"sheet_name_{i}")
#                     sheet_columns = st.text_area(
#                         "Columns (comma-separated)*",
#                         value="ISIN, SEDOL, RIC Code, Weight, Date, Code",
#                         key=f"sheet_columns_{i}"
#                     )
#
#                     # Default values section
#                     st.markdown("#### Default Values")
#                     defaults = st.text_area(
#                         "Default Values (JSON format)",
#                         value=json.dumps({}, indent=2),
#                         key=f"defaults_{i}"
#                     )
#
#                     # Sorting section
#                     st.markdown("#### Sorting")
#                     sort_col = st.text_input("Sort by column", value="Name", key=f"sort_col_{i}")
#                     sort_asc = st.checkbox("Sort ascending", value=True, key=f"sort_asc_{i}")
#
#                     output_columns = {
#                         f"{sheet_name}": [col.strip() for col in sheet_columns.split(",")],
#                         "defaults": json.loads(defaults) if defaults else {},
#                         "sort": sort_col,
#                         "ascending": sort_asc,
#                         "csv": csv_output
#                     }
#
#                     final_client = {
#                         "sheets_names": [sheet_name]
#                     }
#                 else:
#                     # Multiple sheets configuration - use tabs instead of nested expanders
#                     num_sheets = st.number_input(
#                         "Number of Sheets",
#                         min_value=1,
#                         max_value=10,
#                         value=3,
#                         key=f"num_sheets_{i}"
#                     )
#
#                     sheet_configs = {}
#                     tabs = st.tabs([f"Sheet {j + 1}" for j in range(num_sheets)])
#
#                     for j, tab in enumerate(tabs):
#                         with tab:
#                             sheet_name = st.text_input(
#                                 f"Sheet Name*",
#                                 value=f"Sheet{j + 1}",
#                                 key=f"sheet_name_{i}_{j}"
#                             )
#                             sheet_columns = st.text_area(
#                                 f"Columns (comma-separated)*",
#                                 value="Name, ISIN, Ticker, Sector, Market Cap",
#                                 key=f"sheet_columns_{i}_{j}"
#                             )
#                             sheet_configs[sheet_name] = [col.strip() for col in sheet_columns.split(",")]
#
#                     # Default values section (after all sheet configurations)
#                     st.markdown("#### Default Values")
#                     defaults = st.text_area(
#                         "Default Values (JSON format)",
#                         value=json.dumps({}, indent=2),
#                         help='Default values for columns',
#                         key=f"defaults_{i}"
#                     )
#
#                     # Sorting section (after defaults)
#                     st.markdown("#### Sorting (applies to all sheets)")
#                     sort_col = st.text_input("Sort by column", value="Name", key=f"sort_col_{i}")
#                     sort_asc = st.checkbox("Sort ascending", value=True, key=f"sort_asc_{i}")
#
#                     output_columns = {
#                         **sheet_configs,
#                         "defaults": json.loads(defaults) if defaults else {},
#                         "sort": sort_col,
#                         "ascending": sort_asc,
#                         "csv": csv_output
#                     }
#
#                     final_client = {
#                         "sheets_names": list(sheet_configs.keys())
#                     }
#
#                 # Add this output file's configuration to the list
#                 output_configs.append({
#                     "filename_pattern": filename_pattern,
#                     "output_columns": output_columns,
#                     "final_client": final_client
#                 })
#
#         # Main form submit button
#         submitted = st.form_submit_button("Save New Delivery")
#
#         if submitted:
#             if not all([delivery_name, input_files, output_files_count]):
#                 st.error("Please fill all required fields (marked with *)")
#                 st.stop()
#
#             # Validate each output file configuration
#             for i, config in enumerate(output_configs):
#                 if not config['filename_pattern']:
#                     st.error(f"Please provide a filename pattern for Output File {i + 1}")
#                     st.stop()
#                 if not config['output_columns']:
#                     st.error(f"Please configure output columns for Output File {i + 1}")
#                     st.stop()
#
#             try:
#                 new_delivery = {
#                     "name": delivery_name,
#                     "input_files": input_files,
#                     "pre_processing": pre_processing,
#                     "filters": selected_filters,
#                     "other_functions": other_functions,
#                     "output_files": output_configs  # Now storing multiple output configurations
#                 }
#
#                 try:
#                     with open('deliveries_config2.json', 'r') as f:
#                         config = json.load(f)
#                 except (FileNotFoundError, json.JSONDecodeError):
#                     config = {"deliveries": []}
#
#                 if any(d.get('name') == delivery_name for d in config.get("deliveries", [])):
#                     st.error(f"Delivery '{delivery_name}' already exists")
#                     st.stop()
#
#                 config["deliveries"].append(new_delivery)
#
#                 with open('deliveries_config2.json', 'w') as f:
#                     json.dump(config, f, indent=2)
#
#                 st.success("Delivery configuration saved successfully!")
#                 st.balloons()
#
#             except json.JSONDecodeError as e:
#                 st.error(f"Invalid JSON: {str(e)}")
#             except Exception as e:
#                 st.error(f"Error: {str(e)}")
#
#
# if __name__ == "__main__":
#     add_delivery_interface()

def initialize_ui():
    """Set up the Streamlit UI configuration"""
    # st.set_page_config(
    #     page_title="Delivery Configurator",
    #     layout="wide",
    #     initial_sidebar_state="expanded"
    # )
    st.title("Delivery Configuration Manager")


def render_filter_ui() -> List[Dict]:
    """Render the filters section and return selected filters"""
    selected_filters = []

    st.markdown("### Filters")
    st.write("Select which filters to apply:")

    for key, filter_info in FUNCTION_MAP.items():
        col1, col2 = st.columns([1, 4])
        with col1:
            enabled = st.checkbox(f"Enable {filter_info['name']}", key=key)
        with col2:
            st.caption(filter_info["description"])

        if enabled:
            filter_config = {
                "function": filter_info["function"],
                "args": filter_info["args"].copy()
            }

            if filter_info["args"]:
                with st.expander(f"{filter_info['name']} Parameters"):
                    for arg_name, default_val in filter_info["args"].items():
                        if arg_name in ["Period End Date", "Date Of Current Price"]:
                            st.write(f"{arg_name}: Will use the date selected in main interface")
                        elif isinstance(default_val, (int, float)):
                            filter_config["args"][arg_name] = st.number_input(
                                arg_name,
                                value=default_val,
                                key=f"{key}_{arg_name}"
                            )
                        else:
                            filter_config["args"][arg_name] = st.text_input(
                                arg_name,
                                value=str(default_val) if default_val is not None else "",
                                key=f"{key}_{arg_name}"
                            )

            selected_filters.append(filter_config)

    return selected_filters


def render_output_config(output_index: int) -> Dict:
    """Render UI for a single output file configuration"""
    config = {}

    with st.container():
        st.subheader(f"Output File {output_index + 1} Configuration")

        # Filename pattern
        config["filename_pattern"] = st.text_input(
            f"Filename Pattern*",
            value="{client}_{year}{month}{day}.xlsx",
            key=f"filename_pattern_{output_index}",
            help="Available placeholders: {client}, {year}, {month}, {day}, {timestamp}, {quarter}, {next_month}, {next_month_letters}"
        )

        # Output format
        output_format = st.selectbox(
            "Output Format Type",
            options=["Delivery Sheet", "Multiple Sheets"],
            key=f"output_format_{output_index}"
        )
        config["csv_output"] = st.checkbox(
            "Generate CSV output",
            value=False,
            key=f"csv_output_{output_index}"
        )

        if output_format == "Delivery Sheet":
            # Single sheet configuration
            sheet_name = st.text_input(
                "Sheet Name*",
                value="Delivery",
                key=f"sheet_name_{output_index}"
            )
            sheet_columns = st.text_area(
                "Columns (comma-separated)*",
                value="ISIN, SEDOL, RIC Code, Weight, Date, Code",
                key=f"sheet_columns_{output_index}"
            )

            config["output_columns"] = {
                sheet_name: [col.strip() for col in sheet_columns.split(",")],
                "defaults": get_default_values_ui(output_index),
                "sort": get_sorting_ui(output_index),
                "csv": config["csv_output"]
            }
            config["final_client"] = {"sheets_names": [sheet_name]}
        else:
            # Multiple sheets configuration
            num_sheets = st.number_input(
                "Number of Sheets",
                min_value=1,
                max_value=10,
                value=3,
                key=f"num_sheets_{output_index}"
            )

            sheet_configs = {}
            tabs = st.tabs([f"Sheet {j + 1}" for j in range(num_sheets)])

            for j, tab in enumerate(tabs):
                with tab:
                    sheet_name = st.text_input(
                        f"Sheet Name*",
                        value=f"Sheet{j + 1}",
                        key=f"sheet_name_{output_index}_{j}"
                    )
                    sheet_columns = st.text_area(
                        f"Columns (comma-separated)*",
                        value="Name, ISIN, Ticker, Sector, Market Cap",
                        key=f"sheet_columns_{output_index}_{j}"
                    )
                    sheet_configs[sheet_name] = [col.strip() for col in sheet_columns.split(",")]

            config["output_columns"] = {
                **sheet_configs,
                "defaults": get_default_values_ui(output_index),
                "sort": get_sorting_ui(output_index),
                "csv": config["csv_output"]
            }
            config["final_client"] = {"sheets_names": list(sheet_configs.keys())}

    return config


def get_default_values_ui(output_index: int) -> Dict:
    """Render UI for default values configuration"""
    st.markdown("#### Default Values")
    defaults = st.text_area(
        "Default Values (JSON format)",
        value=json.dumps({"Currency": "USD", "Source": "System"}, indent=2),
        key=f"defaults_{output_index}",
        help='Default values for columns in JSON format'
    )
    try:
        return json.loads(defaults) if defaults.strip() else {}
    except json.JSONDecodeError:
        st.error("Invalid JSON format for default values")
        return {}


def get_sorting_ui(output_index: int) -> Dict:
    """Render UI for sorting configuration"""
    st.markdown("#### Sorting")
    sort_col = st.text_input(
        "Sort by column",
        value="Name",
        key=f"sort_col_{output_index}"
    )
    sort_asc = st.checkbox(
        "Sort ascending",
        value=True,
        key=f"sort_asc_{output_index}"
    )
    return {"column": sort_col, "ascending": sort_asc}


def add_delivery_interface():
    """Main interface for adding new delivery configurations"""
    initialize_ui()
    st.subheader("Add New Delivery Configuration")

    with st.form("new_delivery_form"):
        # Basic delivery info
        delivery_name = st.text_input(
            "Delivery Name*",
            help="Unique name for this delivery configuration"
        )
        input_files = st.number_input(
            "Number of Input Files Required*",
            min_value=1,
            value=2
        )

        # Output files configuration
        output_files_count = st.number_input(
            "Number of Output Files*",
            min_value=1,
            value=1
        )
        output_configs = [
            render_output_config(i)
            for i in range(output_files_count)
        ]

        # Filters section
        selected_filters = render_filter_ui()

        # Form submission
        submitted = st.form_submit_button("Save New Delivery")

        if submitted:
            try:
                # Validate inputs
                validate_delivery_name(delivery_name)
                # for config in output_configs:
                #     validate_filename_pattern(config["filename_pattern"])

                # Check for duplicate name
                config = load_config()
                if any(d.get('name') == delivery_name for d in config.get("deliveries", [])):
                    st.error(f"Delivery '{delivery_name}' already exists")
                    st.stop()

                # Create new delivery config
                new_delivery = {
                    "name": delivery_name,
                    "input_files": input_files,
                    "filters": selected_filters,
                    "output_files": output_configs
                }

                # Save to cloud storage
                config["deliveries"].append(new_delivery)
                if save_config(config):
                    st.success("Delivery configuration saved successfully!")
                    st.balloons()
                else:
                    st.error("Failed to save configuration. Please try again.")

            except ValueError as e:
                st.error(f"Validation error: {str(e)}")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON format: {str(e)}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {str(e)}")
                logger.exception("Error saving delivery configuration")


if __name__ == "__main__":
    add_delivery_interface()