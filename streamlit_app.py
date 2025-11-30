import re
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from openai import OpenAI
import os
import bcrypt


def get_env_vars():
    """Load environment variables from st.secrets (Streamlit Cloud) or .env (local)"""
    # Try Streamlit secrets first (for cloud deployment)
    if hasattr(st, 'secrets') and len(st.secrets) > 0:
        try:
            return {
                'api_key': st.secrets.get("OPENAI_API_KEY"),
                'hashed_password': st.secrets.get("HASHED_PASSWORD", "").encode("utf-8") if st.secrets.get("HASHED_PASSWORD") else b""
            }
        except Exception:
            pass
    
    # Fall back to .env file for local development
    load_dotenv(override=True)
    return {
        'api_key': os.getenv("OPENAI_API_KEY"),
        'hashed_password': os.getenv("HASHED_PASSWORD", "").encode("utf-8")
    }

# Get environment variables
env_vars = get_env_vars()
OPENAI_API_KEY = env_vars['api_key']
HASHED_PASSWORD = env_vars['hashed_password']


# Database schema for context
DATABASE_SCHEMA = """
Database Schema:

DIMENSION/LOOKUP TABLES:
- Region (
    RegionID SERIAL PRIMARY KEY,
    Region TEXT NOT NULL UNIQUE
  )

- Country (
    CountryID SERIAL PRIMARY KEY,
    Country TEXT NOT NULL UNIQUE,
    RegionID INTEGER (FK to Region)
  )

- ProductCategory (
    ProductCategoryID SERIAL PRIMARY KEY,
    ProductCategory TEXT NOT NULL UNIQUE,
    ProductCategoryDescription TEXT
  )

ENTITY TABLES:
- Customer (
    CustomerID SERIAL PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Address TEXT,
    City TEXT,
    CountryID INTEGER (FK to Country)
  )

- Product (
    ProductID SERIAL PRIMARY KEY,
    ProductName TEXT NOT NULL UNIQUE,
    ProductUnitPrice REAL NOT NULL,
    ProductCategoryID INTEGER (FK to ProductCategory)
  )

FACT TABLE:
- OrderDetail (
    OrderID SERIAL PRIMARY KEY,
    CustomerID INTEGER (FK to Customer),
    ProductID INTEGER (FK to Product),
    OrderDate DATE NOT NULL,
    QuantityOrdered INTEGER NOT NULL
  )

IMPORTANT NOTES:
- Use JOINs to get descriptive values from dimension tables
- OrderDate is DATE type - use DATE functions for filtering and grouping
- To calculate revenue: ProductUnitPrice * QuantityOrdered
- To get quarters: EXTRACT(QUARTER FROM OrderDate)
- To get year: EXTRACT(YEAR FROM OrderDate)
- To get month: EXTRACT(MONTH FROM OrderDate)
- Always use proper JOINs for foreign key relationships
- Full customer name: FirstName || ' ' || LastName

POSTGRESQL GROUP BY RULES (CRITICAL):
- When using aggregate functions (SUM, COUNT, AVG, etc.), ALL non-aggregated columns in SELECT must be in GROUP BY
- Example: If you SELECT FirstName, LastName, and use SUM(), you must GROUP BY CustomerID, FirstName, LastName
- Correct: GROUP BY c.CustomerID, c.FirstName, c.LastName
- Wrong: GROUP BY c.CustomerID (if selecting FirstName and LastName)
"""



def login_screen():
    """Display login screen and authenticate user."""
    st.title("🔐 Secure Login")
    st.markdown("---")
    st.write("Enter your password to access the AI SQL Query Assistant.")
    
    password = st.text_input("Password", type="password", key="login_password")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("🔓 Login", type="primary", use_container_width=True)
    
    if login_btn:
        if password:
            # Reload env vars fresh for authentication
            env_vars = get_env_vars()
            hashed_pw = env_vars['hashed_password']
            
            if not hashed_pw or len(hashed_pw) < 10:
                st.error("❌ Configuration Error: HASHED_PASSWORD not set!")
                st.info("For Streamlit Cloud: Add HASHED_PASSWORD to your app's Secrets. For local: Add to your .env file.")
            else:
                try:
                    if bcrypt.checkpw(password.encode('utf-8'), hashed_pw):
                        st.session_state.logged_in = True
                        st.success("✅ Authentication successful! Redirecting...")
                        st.rerun()
                    else:
                        st.error("❌ Incorrect password")
                        st.info("Default password is: admin123")
                except ValueError as e:
                    st.error(f"❌ Configuration Error: Invalid HASHED_PASSWORD format in .env file!")
                    st.info("Run `python debug_password.py` to generate a valid hash.")
                except Exception as e:
                    st.error(f"❌ Authentication error: {type(e).__name__}: {e}")
                    st.info("Run `python debug_password.py` to diagnose the issue.")
        else:
            st.warning("⚠️ Please enter a password")
    
    st.markdown("---")
    st.info("""
    **Security Notice:**
    - Passwords are protected using bcrypt hashing
    - Your session is secure and isolated
    - You will remain logged in until you close the browser or click logout
    """)


def require_login():
    """Enforce login before showing main app."""
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()

@st.cache_resource
def get_db_url():
    # Try Streamlit secrets first (for cloud deployment)
    if hasattr(st, 'secrets') and len(st.secrets) > 0:
        try:
            POSTGRES_SERVER = st.secrets.get("POSTGRES_SERVER", "")
            if POSTGRES_SERVER.startswith("postgresql://") or POSTGRES_SERVER.startswith("postgres://"):
                return POSTGRES_SERVER
            else:
                POSTGRES_USERNAME = st.secrets.get("POSTGRES_USERNAME")
                POSTGRES_PASSWORD = st.secrets.get("POSTGRES_PASSWORD")
                POSTGRES_DATABASE = st.secrets.get("POSTGRES_DATABASE")
                return f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"
        except Exception:
            pass
    
    # Fall back to .env for local development
    POSTGRES_SERVER = os.getenv("POSTGRES_SERVER", "")
    
    if POSTGRES_SERVER.startswith("postgresql://") or POSTGRES_SERVER.startswith("postgres://"):
        # Already a full URL, use it directly
        return POSTGRES_SERVER
    else:
        # Build the URL from components
        POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
        return f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"

DATABASE_URL = get_db_url()


@st.cache_resource
def get_db_connection():

    """Create and cache database connection."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None
    
def run_query(sql):
    """Execute SQL query and return results as DataFrame."""
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None 
    

@st.cache_resource
def get_openai_client():
    """Create and cache OpenAI client."""
    return OpenAI(api_key=OPENAI_API_KEY)

def extract_sql_from_response(response_text):
    clean_sql = re.sub(r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE).strip()
    return clean_sql


def generate_sql_with_gpt(user_question):
    client = get_openai_client()
    prompt = f"""You are a PostgreSQL expert. Given the following database schema and a user's question, generate a valid PostgreSQL query.

{DATABASE_SCHEMA}

User Question: {user_question}

Requirements:
1. Generate ONLY the SQL query that I can directly use. No other response.
2. Use proper JOINs to get descriptive names from lookup tables
3. Use appropriate aggregations (COUNT, AVG, SUM, etc.) when needed
4. Add LIMIT clauses for queries that might return many rows (default LIMIT 100)
5. Use proper date/time functions for DATE columns
6. Make sure the query is syntactically correct for PostgreSQL
7. Add helpful column aliases using AS
8. CRITICAL: When using aggregate functions, include ALL non-aggregated columns in GROUP BY clause
   Example: SELECT FirstName, LastName, SUM(amount) ... GROUP BY CustomerID, FirstName, LastName

Generate the SQL query:"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a PostgreSQL expert who generates accurate SQL queries based on natural language questions."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1000
        )
        
        sql_query = extract_sql_from_response(response.choices[0].message.content)
        return sql_query
    
    except Exception as e:
        st.error(f"Error calling OpenAI API: {e}")
        return None, None

def main():
    require_login()
    st.title("🤖 AI-Powered Sales Analytics Assistant")
    st.markdown("Ask questions about sales, orders, customers, and products in natural language!")
    st.markdown("---")


    st.sidebar.title("💡 Example Questions")
    st.sidebar.markdown("""
    Try asking questions like:
                        
    **Sales Analysis:**
    - What is the total revenue by region?
    - Who are the top 10 customers by total spending?
    - What are the monthly sales trends?
                        
    **Product Analysis:**
    - Which products generate the most revenue?
    - What is the average order value by product category?
                        
    **Customer Insights:**
    - How many customers do we have by country?
    - Which customers haven't ordered in the last 90 days?
    """)
    st.sidebar.markdown("---")
    st.sidebar.info("""
        📈 **How it works:**
        1. Enter your question in plain English
        2. AI generates SQL query
        3. Review and optionally edit the query
        4. Click "Run Query" to execute           
    """)

    st.sidebar.markdown("---")
    if st.sidebar.button("🚪Logout"):
        st.session_state.logged_in = False
        st.rerun()

    # Init state

    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = None
    if 'current_question' not in st.session_state:
        st.session_state.current_question = None


    # main input

    user_question = st.text_area(
        "📊 What would you like to know?",
        height=100, 
        placeholder="e.g., What is the total revenue by region? or Who are the top 5 customers?"
    )

    col1, col2, col3 = st.columns([1, 1, 4])
    
    with col1:
        generate_button = st.button(" Generate SQL", type="primary", width="stretch")

    with col2:
        if st.button(" Clear History", width="stretch"):
            st.session_state.query_history = []
            st.session_state.generated_sql = None
            st.session_state.current_question = None

    if generate_button and user_question:
        user_question = user_question.strip()

        if st.session_state.current_question != user_question:
            st.session_state.generated_sql = None
            st.session_state.current_question = None
            


        with st.spinner("🧠 AI is thinking and generating SQL..."):
            sql_query = generate_sql_with_gpt(user_question)
            if sql_query:        
                st.session_state.generated_sql = sql_query
                st.session_state.current_question = user_question

    if st.session_state.generated_sql:
        st.markdown("---")
        st.subheader("Generated SQL Query")
        st.info(f"**Question:** {st.session_state.current_question}")

        edited_sql = st.text_area(
            "Review and edit the SQL query if needed:", 
            value=st.session_state.generated_sql,
            height=200,
        )

        col1, col2 = st.columns([1, 5])

        with col1:
            run_button = st.button("Run Query", type="primary", width="stretch")

        if run_button:
            with st.spinner("Executing query ..."):
                df = run_query(edited_sql)
                
                if df is not None:
                    st.session_state.query_history.append(
                        {'question': user_question, 
                        'sql': edited_sql, 
                        'rows': len(df)}
                    )

                    st.markdown("---")
                    st.subheader("📊 Query Results")
                    st.success(f"✅ Query returned {len(df)} rows")
                    st.dataframe(df, width="stretch")


    if st.session_state.query_history:
        st.markdown('---')
        st.subheader("📜 Query History")
        for idx, item in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history)-idx}: {item['question'][:60]}..."):
                st.markdown(f"**Question:** {item['question']}")
                st.code(item['sql'], language="sql")
                st.caption(f"Returned {item['rows']} rows")
                if st.button(f"Re-run this query", key=f"rerun_{idx}"):
                    df = run_query(item['sql'])
                    if df is not None:
                        st.dataframe(df, width="stretch")


if __name__ == "__main__":
    main()
