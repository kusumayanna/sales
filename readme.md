# Sales Analytics Dashboard

An AI-powered sales analytics dashboard using PostgreSQL (Render), OpenAI GPT, and Streamlit.

## 🎯 Project Overview

This application allows users to query a sales database using natural language. The AI (GPT-4) converts questions into SQL queries, which are then executed against a PostgreSQL database hosted on Render.

## 📊 Database Schema

The database contains sales, order, customer, and product data:

**Dimension Tables:**
- `region` - Geographic regions (9 rows)
- `country` - Countries with region relationships (21 rows)
- `productcategory` - Product categories (8 rows)

**Entity Tables:**
- `customer` - Customer information (91 rows)
- `product` - Product catalog with pricing (77 rows)

**Fact Table:**
- `orderdetail` - Order transactions (621,806 rows)

## 🚀 Features

- 🔐 **Secure Authentication** - Password-protected with bcrypt hashing
- 🤖 **AI-Powered Queries** - Natural language to SQL conversion using OpenAI GPT
- 📊 **Interactive Dashboard** - Real-time query results with editable SQL
- 📝 **Query History** - Track and re-run previous queries
- 💾 **PostgreSQL Database** - Hosted on Render with 600K+ order records

## 📁 Project Structure

```
query_patients/
├── streamlit_app.py           # Main Streamlit application
├── populate_db.py             # Database migration/population script
├── utils.py                   # Database utility functions
├── demo_notebook.ipynb        # Jupyter notebook for demos
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables (not in git)
├── check_database_tables.py   # Verify database tables
├── test_env.py               # Test environment configuration
├── debug_password.py          # Debug authentication
└── DEMO_PREPARATION.md        # Demo guide
```

## 🛠️ Setup Instructions

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd query_patients
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# PostgreSQL Database (Render)
POSTGRES_USERNAME=your-username
POSTGRES_PASSWORD=your-password
POSTGRES_SERVER=your-server-url
POSTGRES_DATABASE=your-database-name

# OpenAI API Key
OPENAI_API_KEY=your-openai-api-key

# Hashed Password for App Login
HASHED_PASSWORD=your-bcrypt-hash
```

**Generate the hashed password:**
```bash
python generate_password.py
```

### 5. Populate Database (if needed)

```bash
python populate_db.py
```

### 6. Run the Application

```bash
streamlit run streamlit_app.py
```

The app will open in your browser at `(https://salesgit-4t2gfasjgm2dijforpv5zc.streamlit.app/)`

## 🔐 Login Credentials

- **Password:** `admin123` (default)

## 📋 Example Queries

Try these natural language questions:

- "What is the total revenue by region?"
- "Who are the top 10 customers by spending?"
- "What are the best selling products?"
- "Show me sales trends by month"
- "Which country has the highest revenue?"
- "How many orders were placed in 2023?"

## 🧪 Testing

### Check Database Connection

```bash
python check_database_tables.py
```

### Test Environment Variables

```bash
python test_env.py
```

### Debug Authentication

```bash
python debug_password.py
```

### Run Jupyter Notebook Demo

```bash
jupyter notebook demo_notebook.ipynb
```

## 📚 Dependencies

- **streamlit** - Web application framework
- **pandas** - Data manipulation
- **psycopg2** - PostgreSQL adapter
- **openai** - OpenAI API client
- **python-dotenv** - Environment variable management
- **bcrypt** - Password hashing

## 🔒 Security

- Passwords are hashed using bcrypt
- API keys and database credentials stored in `.env` (not committed to git)
- Database uses Render's secure hosting
- SSL/TLS encryption for database connections

## 📊 Database Statistics

- **Total Orders:** 621,806
- **Total Customers:** 91
- **Total Products:** 77
- **Countries:** 21
- **Regions:** 9

## 🎓 Academic Context

This project was developed for a database systems course demonstrating:
- Database normalization and design
- PostgreSQL with cloud hosting (Render)
- AI integration for natural language processing
- Full-stack web application development
- Secure authentication implementation

## 📝 License

This project is for academic purposes.

## 👨‍💻 Author

Kusuma Reddy

## 🙏 Acknowledgments

- OpenAI for GPT API
- Render for PostgreSQL hosting
- Streamlit for the web framework
