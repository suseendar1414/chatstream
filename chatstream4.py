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
        return [(col[0], col[1]) for col in columns]

    # List of tables
    tables = [
        "OPPORTUNITY", "ACCOUNT", "CONTACT", "REFERRAL__C", "TASK", "EVENT",
        "COMMISSIONFEE__C", "ADDITIONAL_LOAN__C", "OUTBOUND_REFERRAL__C",
        "LOAN_REFERRAL__C", "REAL_ESTATE_OWNED__C", "ASSET__C", "LIABILITY__C", "LEAD"
    ]

# Generate system prompt
    if "system_prompt" not in st.session_state:
        table_info = []
        for table in tables:
            columns = get_table_schema(table)
            column_info = ", ".join([f"{col[0]} ({col[1]})" for col in columns])
            table_info.append(f"Table: {table}\nColumns: {column_info}")
    
        table_context = "\n\n".join(table_info)
    
        st.session_state.system_prompt = f"""You are an AI Snowflake SQL expert named LoanBot. Your goal is to give correct, executable SQL queries to users asking about loan officer performance and related financial data. You will be replying to users who will be confused if you don't respond in the character of LoanBot.

        The user will ask questions about loan officer performance and financial data; for each question, you should respond and include a SQL query based on the question and the available tables in FirstDB.PUBLIC schema.

        <table_context>
        {table_context}
        </table_context>

        Here are critical rules for the interaction you must abide:
        <rules>
        1. You MUST MUST wrap the generated SQL queries within ```sql code markdown
        2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
        3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
        4. Make sure to generate a single Snowflake SQL code snippet, not multiple. 
        5. You should only use the table columns given in the table context, you MUST NOT use columns that are not listed in the schema.
        6. DO NOT put numerical at the very front of SQL variable.
        7. For boolean conditions, use the actual boolean values TRUE or FALSE without quotes, not string representations.
        8. When joining tables, make sure to use appropriate join conditions based on the available columns.
        9. If a question requires data from multiple tables, use appropriate JOINs to combine the necessary information.
        10. Always consider data types when comparing or manipulating values in your queries.
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
            # if sql_match:
            #     sql = sql_match.group(1)
            #     try:
            #         cursor = st.session_state.snowflake_conn.cursor()
            #         cursor.execute(sql)
            #         results = cursor.fetchall()
            #         columns = [desc[0] for desc in cursor.description]
            #         df = pd.DataFrame(results, columns=columns)
            #         st.dataframe(df)
            #         cursor.close()
            #     except Exception as e:
            #         st.error(f"Error executing SQL: {e}")

            if sql_match:
                sql = sql_match.group(1)
                try:
                    cursor = st.session_state.snowflake_conn.cursor()
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(results, columns=columns)
        
                # Generate a human-like response with the actual results
                    if not df.empty:
                        if 'BOOK_OF_BUSINESS_VALUE' in df.columns:
                            value = df['BOOK_OF_BUSINESS_VALUE'].iloc[0]
                            human_response = f"Great question! I've analyzed your book of business, and I'm excited to share the results with you. The total value of your closed and funded loans under management is ${value:,.2f}. This represents the cumulative amount of all your successfully closed opportunities. It's an impressive figure that showcases your performance and the trust your clients place in you. Is there anything specific about this value you'd like to know more about, such as how it compares to previous periods or your goals?"
                        else:
                # General case for other types of queries
                            human_response = f"I've got the results for you! Here's what I found:\n\n{df.to_string(index=False)}\n\nWould you like me to explain any part of these results in more detail?"
                    else:
                        human_response = "I've run the query, but it looks like there were no results matching the criteria. This could mean that there are no closed and won opportunities in the system yet. Would you like me to modify the query or check something else for you?"
        
                    full_response += f"\n\n{human_response}"
                    message_placeholder.markdown(full_response)
                    st.dataframe(df)
                    cursor.close()
                except Exception as e:
                    error_message = f"I apologize, but I encountered an error while trying to fetch that information for you. The specific error was: {str(e)}. Could you please rephrase your question or ask about a different aspect of loan officer performance? I'm here to help in any way I can."
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