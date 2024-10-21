import streamlit as st
import snowflake.connector
import pandas as pd
from openai import OpenAI
import re
import plotly.express as px

st.set_page_config(page_title="Loan Officer Performance Chatbot", page_icon="🏦", layout="wide")

st.title("🏦 Loan Officer Performance Chatbot")

# Initialize session state variables
if "connected" not in st.session_state:
    st.session_state.connected = False
if "snowflake_conn" not in st.session_state:
    st.session_state.snowflake_conn = None
if "openai_client" not in st.session_state:
    st.session_state.openai_client = None
if "kpi_scores" not in st.session_state:
    st.session_state.kpi_scores = None

# Function to initialize Snowflake connection
def init_snowflake_connection(password):
    return snowflake.connector.connect(
        account="au02318.eu-west-2.aws",
        user="salesmachinesPOC",
        password=password,
        warehouse="COMPUTE_WH",
        database="FIRSTDB",
        schema="PUBLIC"
    )

def calculate_kpi_scores(conn):
    cursor = conn.cursor()
    
    kpi_queries = {
        "Total Loans Closed": "SELECT COUNT(*) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Total Loan Value": "SELECT SUM(AMOUNT) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Average Loan Size": "SELECT AVG(AMOUNT) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Loan Approval Rate": """
            SELECT 
                CAST(COUNT(CASE WHEN STAGENAME = 'Closed Won' THEN 1 END) AS FLOAT) / 
                NULLIF(COUNT(*), 0) * 100 
            FROM OPPORTUNITY
        """
    }
    
    # Attempt to find a customer satisfaction metric
    customer_satisfaction_queries = [
        "SELECT AVG(REVIEWSTARRATING__C) FROM ACCOUNT",
        "SELECT AVG(CUSTOMER_SATISFACTION_SCORE__C) FROM ACCOUNT",
        "SELECT AVG(SATISFACTION_SCORE__C) FROM ACCOUNT",
        "SELECT AVG(RATING) FROM ACCOUNT WHERE RATING IS NOT NULL"
    ]
    
    kpi_scores = {}
    for kpi, query in kpi_queries.items():
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            kpi_scores[kpi] = result[0] if result and result[0] is not None else 0
        except Exception as e:
            st.warning(f"Error calculating {kpi}: {str(e)}")
            kpi_scores[kpi] = 0

    # Try different customer satisfaction queries
    for query in customer_satisfaction_queries:
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            if result and result[0] is not None:
                kpi_scores["Customer Satisfaction"] = result[0]
                break
        except:
            continue
    
    if "Customer Satisfaction" not in kpi_scores:
        st.warning("Could not calculate Customer Satisfaction KPI. Please check your database schema.")
        kpi_scores["Customer Satisfaction"] = 0

    # Calculate an overall score (this is a simplified example)
    overall_score = sum(kpi_scores.values()) / len(kpi_scores)
    kpi_scores["Overall Score"] = overall_score

    cursor.close()
    return kpi_scores

# Connection interface
if not st.session_state.connected:
    with st.form("connection_form"):
        snowflake_password = st.text_input("Enter Snowflake password:", type="password")
        openai_api_key = st.text_input("Enter OpenAI API Key:", type="password")
        submit_button = st.form_submit_button("Connect")

    if submit_button:
        if not snowflake_password or not openai_api_key:
            st.error("Please enter both Snowflake password and OpenAI API key.")
        else:
            try:
                st.session_state.snowflake_conn = init_snowflake_connection(snowflake_password)
                st.session_state.openai_client = OpenAI(api_key=openai_api_key)
                st.session_state.kpi_scores = calculate_kpi_scores(st.session_state.snowflake_conn)
                st.session_state.connected = True
                st.success("Connected successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to connect: {e}")
else:
    # Function to get table schema
    @st.cache_data
    def get_table_schema(table_name):
        cursor = st.session_state.snowflake_conn.cursor()
        cursor.execute(f"DESCRIBE TABLE FirstDB.PUBLIC.{table_name}")
        columns = cursor.fetchall()
        cursor.close()
        return [(col[0], col[1]) for col in columns]

    # List of tables
    tables = [
        "OPPORTUNITY", "ACCOUNT", "CONTACT", "REFERRAL__C", "TASK", "EVENT",
        "COMMISSIONFEE__C", "ADDITIONAL_LOAN__C", "OUTBOUND_REFERRAL__C",
        "LOAN_REFERRAL__C", "REAL_ESTATE_OWNED__C", "ASSET__C", "LIABILITY__C", "LEAD", "Offer__c",
        "OPPORTUNITYTEAMMEMBER"
    ]

    # Generate system prompt (only once)
    if "system_prompt" not in st.session_state:
        table_info = []
        for table in tables:
            columns = get_table_schema(table)
            column_info = ", ".join([f"{col[0]} ({col[1]})" for col in columns])
            table_info.append(f"Table: {table}\nColumns: {column_info}")
    
        table_context = "\n\n".join(table_info)
    
        st.session_state.system_prompt = f"""You are an AI Snowflake SQL expert named LoanBot. Your goal is to give correct, executable SQL queries to users asking about loan officer performance. You will be replying to users who will be confused if you don't respond in the character of LoanBot.

        The user will ask questions about loan officer performance; for each question, you should respond and include a SQL query based on the question and the available tables in FirstDB.PUBLIC schema.

        <table_context>
        {table_context}
        </table_context>

        Here are 7 critical rules for the interaction you must abide:
        <rules>
        1. You MUST MUST wrap the generated SQL queries within ```sql code markdown
        2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
        3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
        4. Make sure to generate a single Snowflake SQL code snippet, not multiple. 
        5. You should only use the table columns given in the table context, you MUST NOT use columns that are not listed in the schema.
        6. DO NOT put numerical at the very front of SQL variable.
        7. Use only valid Snowflake SQL syntax.
        7. For boolean conditions, use the actual boolean values 'true' or 'false' without quotes, not string representations.
        </rules>

        Now to get started, please briefly introduce yourself, describe the available data at a high level, and share some example metrics that can be analyzed in 2-3 sentences. Then provide 3 example questions using bullet points.
    """

    # Initialize chat messages
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "system", "content": st.session_state.system_prompt}]

    # Display chat messages
    for message in st.session_state.messages:
        if message["role"] != "system":
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                if "results" in message:
                    st.dataframe(message["results"])
                if "chart" in message:
                    st.plotly_chart(message["chart"])

    # Chat input
    # Inside your chat loop:
    if prompt := st.chat_input("Ask about loan officer performance or KPI scores"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""

            # Check if the user is asking about KPI scores
            if "kpi" in prompt.lower() and "score" in prompt.lower():
                kpi_response = "Here are the current KPI scores based on our database:\n\n"
                for category, score in st.session_state.kpi_scores.items():
                    kpi_response += f"{category}: {score:.2f}\n"
                full_response = kpi_response
                message_placeholder.markdown(full_response)

                # Create a bar chart for KPI scores
                kpi_df = pd.DataFrame(list(st.session_state.kpi_scores.items()), columns=['KPI', 'Score'])
                fig = px.bar(kpi_df, x='KPI', y='Score', title='KPI Scores')
                st.plotly_chart(fig)
            else:
                # Existing chat completion logic
                for response in st.session_state.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
                    stream=True,
                ):
                    full_response += (response.choices[0].delta.content or "")
                    message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)


        # Execute SQL if present
            sql_match = re.search(r"```sql\n(.*?)\n```", full_response, re.DOTALL)
            if sql_match:
                sql = sql_match.group(1).strip()
                try:
                    cursor = st.session_state.snowflake_conn.cursor()
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(results, columns=columns)

                # Generate a human-like response with the actual results
                    if not df.empty:
                        human_response = f"Great! I've executed the query and here are the results:\n\n{df.to_string(index=False)}\n\nWould you like me to explain any part of these results in more detail?"
                        st.dataframe(df)
                    
                    # Visualization (if applicable)
                        if len(df.columns) >= 2 and df[df.columns[1]].dtype in ['int64', 'float64']:
                            fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=f"{df.columns[1]} by {df.columns[0]}")
                            st.plotly_chart(fig)
                            human_response += "\n\nI've also created a bar chart to visualize this data for you. Does this help illustrate the information more clearly?"
                    else:
                        human_response = "I've run the query, but it looks like there were no results matching the criteria. Would you like me to modify the query or check something else for you?"
                
                    full_response += f"\n\n{human_response}"
                    message_placeholder.markdown(full_response)
                    cursor.close()
                except Exception as e:
                    error_message = f"I apologize, but I encountered an error while trying to execute the SQL query. The specific error was: {str(e)}. Let me try to rephrase the query to address this issue."
                    full_response += f"\n\n{error_message}"
                    message_placeholder.markdown(full_response)
                    st.error(f"Error executing SQL: {e}")

            st.session_state.messages.append({"role": "assistant", "content": full_response})

    # Add a disconnect button
    if st.button("Disconnect"):
        if st.session_state.snowflake_conn:
            st.session_state.snowflake_conn.close()
        st.session_state.clear()
        st.rerun()

# Add a sidebar with additional information or controls
with st.sidebar:
    st.header("About LoanBot")
    st.write("LoanBot is an AI-powered assistant designed to help you analyze loan officer performance and KPI scores. It can answer questions about various metrics and provide insights based on your Snowflake database.")
    st.write("To get started, simply ask a question about loan officer performance or KPI scores in the chat input box.")

    if st.session_state.connected:
        st.success("Connected to Snowflake")
        st.success("KPI Scores Calculated")
    else:
        st.warning("Not connected to Snowflake")
        st.warning("KPI Scores Not Available")

    # You can add more controls or information here as needed      