import streamlit_authenticator as stauth
import streamlit as st
from Home import credentials  # Store credentials separately

authenticator = stauth.Authenticate(
    credentials=credentials,
    cookie_name="app_login",
    key="fepsir_key",
    cookie_expiry_days=1
)


def check_auth():
    if 'authentication_status' not in st.session_state:
        st.session_state['authentication_status'] = None

    if not st.session_state.get('authentication_status'):
        login_result = authenticator.login(key='Login', location='main')
        if login_result:
            name, authentication_status, username = login_result
            st.session_state.update({
                'name': name,
                'authentication_status': authentication_status,
                'username': username
            })

            if authentication_status:
                st.rerun()
            elif authentication_status == False:
                st.error("Username/password is incorrect")

    # Return authentication status
    return st.session_state.get('authentication_status')
