# import streamlit as st

# def load_theme():
#     theme = st.session_state.get('theme', 'light')
    
#     # Common styles
#     common_css = """
#         <style>
#             /* Common styles for both themes */
#             .stButton button {
#                 width: 100%;
#                 border-radius: 5px;
#                 height: 3em;
#             }
#         </style>
#     """
    
#     # Theme-specific styles
#     if theme == 'light':
#         css = """
#             <style>
#                 /* Load light theme CSS */
#                 @import url('styles/light_mode.css');
                
#                 /* Additional light theme overrides */
#                 .stApp {
#                     background-color: #ffffff;
#                     color: #000000;
#                 }
#                 .stButton button {
#                     background-color: #f0f2f6;
#                     color: #000000;
#                 }
#                 /* Add more light theme styles */
#             </style>
#         """
#     else:
#         css = """
#             <style>
#                 /* Load dark theme CSS */
#                 @import url('styles/dark_mode.css');
                
#                 /* Additional dark theme overrides */
#                 .stApp {
#                     background-color: #0e1117;
#                     color: #ffffff;
#                 }
#                 .stButton button {
#                     background-color: #262730;
#                     color: #ffffff;
#                 }
#                 /* Add more dark theme styles */
#             </style>
#         """
    
#     # Inject CSS
#     st.markdown(common_css, unsafe_allow_html=True)
#     st.markdown(css, unsafe_allow_html=True) 