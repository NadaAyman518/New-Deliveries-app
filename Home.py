import streamlit as st
import streamlit_authenticator as stauth



st.set_page_config(
    page_title='Homepage',
    layout='wide',
    initial_sidebar_state='collapsed'
)

# st.title('Homepage')
# st.write(f"Welcome {st.session_state['name']}!")

hashed_password = stauth.Hasher.hash('Fepsir@12342768')
# 1. Configure credentials properly
credentials = {
    "usernames": {
        "admin": {
            "name": "Admin",
            "password": hashed_password
        }
    }
}

# 2. Initialize authenticator
authenticator = stauth.Authenticate(
    credentials=credentials,
    cookie_name="app_login",
    key="fepsir_key",  # Should be a long random string
    cookie_expiry_days=1,
    preauthorized=None
)

def check_authentication():
    """Centralized authentication check for all pages"""
    if 'authentication_status' not in st.session_state:
        st.session_state['authentication_status'] = None

    # Handle login if not authenticated
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

def main():
    is_authenticated = check_authentication()

    if is_authenticated:
        # Show sidebar only when authenticated
        st.sidebar.markdown("## Navigation")
        authenticator.logout(key="Logout", location="sidebar")

        # Main page content
        st.title('Homepage')
        # st.write(f"Welcome {st.session_state['name']}!")
        st.write("You are successfully logged in!")

        # Your other page content here

    elif is_authenticated == False:
        st.error("Authentication failed")
    else:
        st.warning("Please log in to access the application")


# # Set page configuration
# st.set_page_config(
#     page_title='Homepage',
#     layout='wide',
#     initial_sidebar_state='auto')


# def main():
#     st.title('Homepage')
#     st.write("Welcome to the Home page!")
#     st.sidebar.markdown("Navigate to other pages using the sidebar.")


if __name__ == '__main__':
    main()