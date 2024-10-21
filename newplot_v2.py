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
        "Total Number of Loans Closed": "SELECT COUNT(*) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Total Dollar Value of Loans Closed": "SELECT SUM(AMOUNT) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Loan Types": """
            SELECT 
                COUNT(CASE WHEN LOANTYPE__C = 'Fixed' THEN 1 END) AS Fixed,
                COUNT(CASE WHEN LOANTYPE__C = 'ARM' THEN 1 END) AS ARM,
                COUNT(CASE WHEN LOANTYPE__C = 'FHA' THEN 1 END) AS FHA
            FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'
        """,
        "Average Loan Size": "SELECT AVG(AMOUNT) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Loan Approval Rate": """
            SELECT 
                CAST(COUNT(CASE WHEN STAGENAME = 'Closed Won' THEN 1 END) AS FLOAT) / 
                NULLIF(COUNT(*), 0) * 100 
            FROM OPPORTUNITY
        """,
        "Customer Satisfaction Scores": "SELECT AVG(REVIEWSTARRATING__C) FROM ACCOUNT",
        "Referral Rates": "SELECT COUNT(*) FROM REFERRAL__C",
        "Time to Close": """
            SELECT AVG(DATEDIFF('day', CREATEDDATE, CLOSEDATE)) 
            FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'
        """,
        "Default Rates": "SELECT COUNT(*) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Lost' AND TYPE = 'Default'",
        "Market Share Growth": """
            SELECT 
                (SELECT COUNT(*) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won' AND CLOSEDATE >= DATEADD(year, -1, CURRENT_DATE())) /
                NULLIF((SELECT COUNT(*) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won' AND CLOSEDATE < DATEADD(year, -1, CURRENT_DATE()) AND CLOSEDATE >= DATEADD(year, -2, CURRENT_DATE())), 0) * 100 - 100
        """,
        "Regulatory Compliance": "SELECT COUNT(*) FROM OPPORTUNITY WHERE ISCOMPLIANT__C = TRUE",
        "Profitability per Loan": "SELECT AVG(REVENUE__C - COST__C) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'",
        "Adaptability to Market Changes": """
            SELECT STDDEV(AMOUNT) / AVG(AMOUNT) * 100
            FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'
        """,
        "Cross-Selling Ratio": """
            SELECT AVG(NUMBER_OF_PRODUCTS__C) 
            FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'
        """,
        "Repeat Business Rate": """
            SELECT 
                COUNT(DISTINCT CASE WHEN NUMBER_OF_CLOSED_OPPORTUNITIES__C > 1 THEN ACCOUNTID END) * 100.0 / 
                NULLIF(COUNT(DISTINCT ACCOUNTID), 0)
            FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'
        """,
        "Conversion Rate": """
            SELECT 
                COUNT(CASE WHEN STAGENAME = 'Closed Won' THEN 1 END) * 100.0 / 
                NULLIF(COUNT(CASE WHEN STAGENAME IN ('Prospecting', 'Qualification') THEN 1 END), 0)
            FROM OPPORTUNITY
        """,
        "Loan Origination Fees": "SELECT AVG(ORIGINATION_FEES__C) FROM OPPORTUNITY WHERE STAGENAME = 'Closed Won'"
    }
    
    kpi_scores = {}
    for kpi, query in kpi_queries.items():
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            kpi_scores[kpi] = result[0] if result and result[0] is not None else 0
        except Exception as e:
            st.warning(f"Error calculating {kpi}: {str(e)}")
            kpi_scores[kpi] = 0
    
    cursor.close()
    return kpi_scores

def calculate_lo_impact_scores():
    # This would typically be based on business logic or predefined weightings
    # For this example, we'll use placeholder values
    return {
        "Total Number of Loans Closed": 9,
        "Total Dollar Value of Loans Closed": 10,
        "Loan Types": 6,
        "Average Loan Size": 8,
        "Loan Approval Rate": 7,
        "Customer Satisfaction Scores": 8,
        "Referral Rates": 7,
        "Time to Close": 6,
        "Default Rates": 8,
        "Market Share Growth": 6,
        "Regulatory Compliance": 9,
        "Profitability per Loan": 7,
        "Adaptability to Market Changes": 5,
        "Cross-Selling Ratio": 4,
        "Repeat Business Rate": 7,
        "Conversion Rate": 6,
        "Loan Origination Fees": 5
    }

def calculate_dino_lo_percentage_achievement(kpi_scores):
    # This would typically involve comparing against targets or benchmarks
    # For this example, we'll use placeholder logic
    max_possible = {
        "Total Number of Loans Closed": 1000,
        "Total Dollar Value of Loans Closed": 100000000,
        "Loan Types": 100,  # Percentage of diversity
        "Average Loan Size": 500000,
        "Loan Approval Rate": 100,
        "Customer Satisfaction Scores": 5,
        "Referral Rates": 500,
        "Time to Close": 30,  # Lower is better
        "Default Rates": 0,  # Lower is better
        "Market Share Growth": 20,
        "Regulatory Compliance": 100,
        "Profitability per Loan": 10000,
        "Adaptability to Market Changes": 100,
        "Cross-Selling Ratio": 3,
        "Repeat Business Rate": 100,
        "Conversion Rate": 100,
        "Loan Origination Fees": 5000
    }
    
    achievement_scores = {}
    for kpi, score in kpi_scores.items():
        if kpi in max_possible:
            if kpi in ["Time to Close", "Default Rates"]:
                achievement_scores[kpi] = max(0, (max_possible[kpi] - score) / max_possible[kpi]) * 100
            else:
                achievement_scores[kpi] = min(100, (score / max_possible[kpi]) * 100)
        else:
            achievement_scores[kpi] = 0
    
    return achievement_scores

def calculate_lo_ranking_scores(impact_scores, achievement_scores):
    ranking_scores = {}
    for kpi in impact_scores.keys():
        ranking_scores[kpi] = (impact_scores[kpi] * achievement_scores[kpi]) / 10
    return ranking_scores

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

# Add this to your chat input handling logic
# Chat input handling
    if prompt := st.chat_input("Ask about loan officer performance, KPI scores, or any other question"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""

            if "kpi scores" in prompt.lower() or "loan officer performance" in prompt.lower():
                try:
                    kpi_scores = calculate_kpi_scores(st.session_state.snowflake_conn)
                    impact_scores = calculate_lo_impact_scores()
                    achievement_scores = calculate_dino_lo_percentage_achievement(kpi_scores)
                    ranking_scores = calculate_lo_ranking_scores(impact_scores, achievement_scores)

                    df = pd.DataFrame({
                        'KPI': kpi_scores.keys(),
                        'KPI Score': kpi_scores.values(),
                        'LO Impact Score': impact_scores.values(),
                        'DINO LO % Achievement': achievement_scores.values(),
                        'LO Ranking Score': ranking_scores.values()
                    })

                    full_response = "Here's a comprehensive analysis of the Loan Officer's KPI scores:\n\n"
                    full_response += df.to_string(index=False)
                    full_response += "\n\nWould you like me to explain any specific KPI or score in more detail?"

                    message_placeholder.markdown(full_response)
                    st.dataframe(df)

                    fig = px.bar(df, x='KPI', y='LO Ranking Score', title='Loan Officer KPI Ranking Scores')
                    st.plotly_chart(fig)

                except Exception as e:
                    error_message = f"I apologize, but I encountered an error while calculating the KPI scores. The specific error was: {str(e)}. Please check the database connection and try again."
                    full_response = error_message
                    message_placeholder.markdown(full_response)
                    st.error(f"Error calculating KPI scores: {e}")
            else:
                # Existing chat completion logic for other types of questions
                for response in st.session_state.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": st.session_state.system_prompt},
                        *[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages[-5:]]
                    ],
                    stream=True,
                ):
                    full_response += (response.choices[0].delta.content or "")
                    message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)

            # Check if the response contains a SQL query
                sql_match = re.search(r"```sql\n(.*?)\n```", full_response, re.DOTALL)
                if sql_match:
                    sql = sql_match.group(1).strip()
                    try:
                        cursor = st.session_state.snowflake_conn.cursor()
                        cursor.execute(sql)
                        results = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(results, columns=columns)

                        if not df.empty:
                            st.dataframe(df)
                        
                        # Visualization (if applicable)
                            if len(df.columns) >= 2 and df[df.columns[1]].dtype in ['int64', 'float64']:
                                fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=f"{df.columns[1]} by {df.columns[0]}")
                                st.plotly_chart(fig)
                                full_response += "\n\nI've also created a bar chart to visualize this data for you. Does this help illustrate the information more clearly?"
                        else:
                            full_response += "\n\nI've run the query, but it looks like there were no results matching the criteria. Would you like me to modify the query or check something else for you?"
                
                        message_placeholder.markdown(full_response)
                        cursor.close()
                    except Exception as e:
                        error_message = f"\n\nI apologize, but I encountered an error while trying to execute the SQL query. The specific error was: {str(e)}. Let me try to rephrase the query to address this issue."
                        full_response += error_message
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
           
