import streamlit as st
import snowflake.connector
import pandas as pd
from openai import OpenAI
import re

st.title("🏦 Loan Officer Performance Chatbot")

# Initialize session state variables
if "connected" not in st.session_state:
    st.session_state.connected = False
if "snowflake_conn" not in st.session_state:
    st.session_state.snowflake_conn = None
if "openai_client" not in st.session_state:
    st.session_state.openai_client = None

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

# Connection interface
if not st.session_state.connected:
    snowflake_password = st.text_input("Enter Snowflake password:", type="password")
    openai_api_key = st.text_input("Enter OpenAI API Key:", type="password")

    if st.button("Connect"):
        if not snowflake_password or not openai_api_key:
            st.error("Please enter both Snowflake password and OpenAI API key.")
        else:
            try:
                st.session_state.snowflake_conn = init_snowflake_connection(snowflake_password)
                st.session_state.openai_client = OpenAI(api_key=openai_api_key)
                st.session_state.connected = True
                st.success("Connected successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to connect: {e}")
else:
    # Function to get table schema
    def get_table_schema(table_name):
        cursor = st.session_state.snowflake_conn.cursor()
        cursor.execute(f"DESCRIBE TABLE FirstDB.PUBLIC.{table_name}")
        columns = cursor.fetchall()
        cursor.close()
        return [col[0] for col in columns]

    # List of tables
    tables = [
        "OPPORTUNITY", "ACCOUNT", "CONTACT", "REFERRAL__C", "TASK", "EVENT",
        "COMMISSIONFEE__C", "ADDITIONAL_LOAN__C", "OUTBOUND_REFERRAL__C",
        "LOAN_REFERRAL__C", "REAL_ESTATE_OWNED__C", "ASSET__C", "LIABILITY__C", "LEAD"
    ]

    # Generate system prompt (only once)
    if "system_prompt" not in st.session_state:
        table_info = []
        for table in tables:
            columns = get_table_schema(table)
            table_info.append(f"Table: {table}\nColumns: {', '.join(columns)}")
        
        table_context = "\n\n".join(table_info)
        
        st.session_state.system_prompt = f"""You are an AI Snowflake SQL expert named LoanBot. Your goal is to give correct, executable SQL queries to users asking about loan officer performance. You will be replying to users who will be confused if you don't respond in the character of LoanBot.

        The user will ask questions about loan officer performance; for each question, you should respond and include a SQL query based on the question and the available tables in FirstDB.PUBLIC schema.

        <table_context>
        {table_context}
        </table_context>

        Here are 6 critical rules for the interaction you must abide:
        <rules>
        1. You MUST MUST wrap the generated SQL queries within ```sql code markdown
        2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
        3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
        4. Make sure to generate a single Snowflake SQL code snippet, not multiple. 
        5. You should only use the table columns given in the table context, you MUST NOT hallucinate about table names or columns.
        6. DO NOT put numerical at the very front of SQL variable.
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

    # Chat input
    if prompt := st.chat_input("Ask about loan officer performance"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            for response in st.session_state.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
                stream=True,
            ):
                full_response += (response.choices[0].delta.content or "")
                message_placeholder.markdown(full_response + "▌")
            message_placeholder.markdown(full_response)

            # Execute SQL if present
            sql_match = re.search(r"```sql\n(.*)\n```", full_response, re.DOTALL)
            if sql_match:
                sql = sql_match.group(1)
                try:
                    cursor = st.session_state.snowflake_conn.cursor()
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(results, columns=columns)
                    st.dataframe(df)
                    cursor.close()
                except Exception as e:
                    st.error(f"Error executing SQL: {e}")

        st.session_state.messages.append({"role": "assistant", "content": full_response})

    # Add a disconnect button
    if st.button("Disconnect"):
        if st.session_state.snowflake_conn:
            st.session_state.snowflake_conn.close()
        st.session_state.clear()
        st.rerun()