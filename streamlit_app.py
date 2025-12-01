import re
import streamlit as st
import pandas as pd
import psycopg2
from google import genai
from dotenv import load_dotenv
import bcrypt

load_dotenv()

GEMINI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

st.set_page_config(
    page_title="AI SQL Query Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATABASE_SCHEMA = """
Database Schema:

CORE TABLES:

- region (
    region_id INTEGER PRIMARY KEY,
    region TEXT NOT NULL
  )

- country (
    country_id INTEGER PRIMARY KEY,
    country TEXT NOT NULL,
    region_id INTEGER NOT NULL (FK to region.region_id)
  )

- customer (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    country_id INTEGER NOT NULL (FK to country.country_id)
  )

- product_category (
    product_category_id INTEGER PRIMARY KEY,
    product_category TEXT NOT NULL,
    product_category_description TEXT NOT NULL
  )

- product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_unit_price NUMERIC(12,2) NOT NULL,
    product_category_id INTEGER NOT NULL (FK to product_category.product_category_id)
  )

- order_detail (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL (FK to customer.customer_id),
    product_id INTEGER NOT NULL (FK to product.product_id),
    order_date DATE NOT NULL,
    quantity_ordered INTEGER NOT NULL
  )

IMPORTANT NOTES:
- Sales total per row: product.product_unit_price * order_detail.quantity_ordered
- Joins you will commonly need:
  order_detail -> customer -> country -> region
  order_detail -> product -> product_category
- order_date is DATE type
- For year/month/quarter:
  EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date), EXTRACT(QUARTER FROM order_date)
- Use ROUND(..., 2) for currency-style totals
"""


def _inject_theme():
    st.markdown(
        """
        <style>
          .stApp {
            background:
              radial-gradient(1100px 520px at 14% 8%, rgba(120, 90, 255, .16), transparent 60%),
              radial-gradient(900px 480px at 92% 18%, rgba(0, 200, 255, .14), transparent 55%),
              linear-gradient(180deg, #0b1020 0%, #070b16 100%);
          }

          .block-container {
            max-width: 1180px;
            padding-top: 1.9rem;
            padding-bottom: 2.6rem;
            padding-left: 1.6rem;
            padding-right: 1.6rem;
          }

          [data-testid="stSidebar"]{
            background: linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02));
            border-right: 1px solid rgba(255,255,255,.10);
          }

          .hero {
            margin-top: .45rem;
            border: 1px solid rgba(255,255,255,.14);
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(255,255,255,.10), rgba(255,255,255,.03));
            padding: 18px 18px 14px 18px;
            box-shadow: 0 24px 60px rgba(0,0,0,.35);
          }

          .stTextArea textarea, .stTextInput input {
            background: rgba(255,255,255,.06) !important;
            border: 1px solid rgba(255,255,255,.14) !important;
            border-radius: 16px !important;
          }

          button[kind="primary"], button { border-radius: 999px !important; }

          .stAlert { border-radius: 18px; border: 1px solid rgba(255,255,255,.12); }

          [data-testid="stExpander"]{
            border: 1px solid rgba(255,255,255,.10);
            border-radius: 18px;
            overflow: hidden;
            background: rgba(255,255,255,.04);
          }

          div[data-testid="stMetric"]{
            background: rgba(255,255,255,.05);
            border: 1px solid rgba(255,255,255,.12);
            border-radius: 18px;
            padding: 14px 14px 10px 14px;
          }

          .stDataFrame { border-radius: 18px; overflow: hidden; border: 1px solid rgba(255,255,255,.10); }

          hr { margin: 1.15rem 0; border-color: rgba(255,255,255,.12); }

          #MainMenu {visibility: hidden;}
          footer {visibility: hidden;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _hero():
    st.markdown(
        """
        <div class="hero">
          <div style="display:flex;align-items:center;gap:12px;">
            <div style="
              width:44px;height:44px;border-radius:14px;
              background: radial-gradient(circle at 30% 30%, rgba(0,200,255,.6), rgba(120,90,255,.45));
              border: 1px solid rgba(255,255,255,.16);
              display:flex;align-items:center;justify-content:center;
              font-size:22px;
            ">🤖</div>
            <div>
              <div style="font-size:22px;font-weight:750;color:rgba(255,255,255,.95);">
                AI-Powered SQL Query Assistant
              </div>
              <div style="font-size:13px;color:rgba(255,255,255,.72);margin-top:2px;">
                Orders DB • Generate PostgreSQL • Review • Run • Explore
              </div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def login_screen():
    _inject_theme()
    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

    left, center, right = st.columns([2, 5, 2])
    with center:
        st.markdown(
            """
            <div class="hero">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
                <div style="font-size:20px;">🔐</div>
                <div style="font-size:20px;font-weight:750;">Secure Login</div>
              </div>
              <div style="color:rgba(255,255,255,.72);font-size:13px;">
                Enter your password to access the assistant.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.write("")

        password = st.text_input("Password", type="password", key="login_password")
        c1, c2 = st.columns(2)
        with c1:
            login_btn = st.button("🔓 Login", type="primary", use_container_width=True)
        with c2:
            st.button(
                "🧹 Clear",
                use_container_width=True,
                on_click=lambda: st.session_state.update({"login_password": ""}),
            )

        if login_btn:
            if password:
                try:
                    if bcrypt.checkpw(password.encode("utf-8"), HASHED_PASSWORD):
                        st.session_state.logged_in = True
                        st.session_state.login_password = ""  # clear field
                        st.rerun()  # ✅ important: rerun so login UI disappears
                    else:
                        st.error("❌ Incorrect password")
                except Exception as e:
                    st.error(f"❌ Authentication error: {e}")
            else:
                st.warning("⚠️ Please enter a password")

        st.info(
            """
            **Security Notice**
            - Passwords are protected using bcrypt hashing  
            - Your session stays active until you logout or close the browser
            """
        )


def require_login():
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login_screen()
        st.stop()


@st.cache_resource
def get_db_url():
    POSTGRES_USERNAME = st.secrets["POSTGRES_USERNAME"]
    POSTGRES_PASSWORD = st.secrets["POSTGRES_PASSWORD"]
    POSTGRES_SERVER = st.secrets["POSTGRES_SERVER"]
    POSTGRES_DATABASE = st.secrets["POSTGRES_DATABASE"]
    return f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"


DATABASE_URL = get_db_url()


@st.cache_resource
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None


def run_query(sql):
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None


@st.cache_resource
def get_openai_client():
    return genai.Client(api_key=GEMINI_API_KEY)


def extract_sql_from_response(response_text):
    return re.sub(r"^```sql\s*|\s*```$", "", response_text, flags=re.IGNORECASE | re.MULTILINE).strip()


def generate_sql_with_gpt(user_question):
    client = get_openai_client()
    prompt = f"""You are a PostgreSQL expert. Given the following database schema and a user's question, generate a valid PostgreSQL query.

{DATABASE_SCHEMA}

User Question: {user_question}

Requirements:
1. Generate ONLY the SQL query that I can directly use. No other response.
2. Use proper JOINs when needed (region/country/customer/product/product_category).
3. Use appropriate aggregations (COUNT, AVG, SUM, etc.) when needed.
4. Add LIMIT clauses for queries that might return many rows (default LIMIT 100).
5. Use proper date/time functions for DATE columns (order_date).
6. Make sure the query is syntactically correct for PostgreSQL.
7. Add helpful column aliases using AS.
8. For revenue/sales totals, use product_unit_price * quantity_ordered and ROUND(..., 2).

Generate the SQL query:"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=f"You are a PostgreSQL expert who generates accurate SQL queries based on natural language questions. Generate the query for the following: {prompt}"
        )
        return extract_sql_from_response(response.text)
    except Exception as e:
        st.error(f"Error calling Gemini API: {e}")
        return None


@st.cache_data(ttl=60)
def fetch_metrics():
    sql_orders = "SELECT COUNT(*) AS orders FROM order_detail;"
    sql_customers = "SELECT COUNT(*) AS customers FROM customer;"
    sql_products = "SELECT COUNT(*) AS products FROM product;"
    sql_revenue = """
        SELECT COALESCE(ROUND(SUM(p.product_unit_price * od.quantity_ordered), 2), 0) AS revenue
        FROM order_detail od
        JOIN product p ON p.product_id = od.product_id;
    """

    out = {"orders": 0, "customers": 0, "products": 0, "revenue": 0.0}

    odf = run_query(sql_orders)
    cdf = run_query(sql_customers)
    pdf = run_query(sql_products)
    rdf = run_query(sql_revenue)

    if odf is not None and not odf.empty:
        out["orders"] = int(odf.iloc[0]["orders"])
    if cdf is not None and not cdf.empty:
        out["customers"] = int(cdf.iloc[0]["customers"])
    if pdf is not None and not pdf.empty:
        out["products"] = int(pdf.iloc[0]["products"])
    if rdf is not None and not rdf.empty:
        out["revenue"] = float(rdf.iloc[0]["revenue"])

    return out


def main():
    require_login()
    _inject_theme()

    if "query_history" not in st.session_state:
        st.session_state.query_history = []
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "current_question" not in st.session_state:
        st.session_state.current_question = None
    if "question_text" not in st.session_state:
        st.session_state.question_text = ""
    if "selected_prompt" not in st.session_state:
        st.session_state.selected_prompt = "Top Customers by Spend"

    prompt_map = {
        "Revenue by Region (Top 10)": "Show total revenue by region, highest first, limit 10.",
        "Top Customers by Spend": "Who are the top 10 customers by total spend?",
        "Monthly Sales Trend": "Show total revenue by month for all years.",
        "Top Products by Revenue": "List the top 10 products by revenue.",
        "Revenue by Category": "Show total revenue by product category.",
        "Recent Orders": "Show the 50 most recent orders with customer and product details."
    }

    st.sidebar.markdown("## 🧭 Quick Prompts")
    st.sidebar.caption("Pick a prompt, then click **Apply**.")
    with st.sidebar.form("prompt_form", border=False):
        st.selectbox("Choose a prompt", list(prompt_map.keys()), key="selected_prompt")
        col1, col2 = st.columns(2)
        with col1:
            apply_clicked = st.form_submit_button("🪄 Apply", type="primary", use_container_width=True)
        with col2:
            clear_clicked = st.form_submit_button("🧹 Clear", use_container_width=True)

    if apply_clicked:
        st.session_state.question_text = prompt_map[st.session_state.selected_prompt]
        st.session_state.generated_sql = None
        st.session_state.current_question = None

    if clear_clicked:
        st.session_state.query_history = []
        st.session_state.generated_sql = None
        st.session_state.current_question = None

    st.sidebar.markdown("---")
    st.sidebar.markdown("### ✅ Notes (meaningful)")
    st.sidebar.markdown(
        """
- **Revenue** = `p.product_unit_price * od.quantity_ordered`
- Join paths:
  - `od → customer → country → region`
  - `od → product → product_category`
- Time grouping:
  - Month: `DATE_TRUNC('month', od.order_date)`
  - Year: `EXTRACT(YEAR FROM od.order_date)`
- If you want lots of rows, mention **LIMIT** (otherwise AI adds LIMIT 100).
        """
    )
    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.generated_sql = None
        st.session_state.current_question = None
        st.session_state.query_history = []
        st.rerun()

    _hero()
    st.write("")

    k1, k2, k3, k4 = st.columns(4)
    o = k1.empty(); c = k2.empty(); p = k3.empty(); r = k4.empty()
    o.metric("🧾 Orders", "—")
    c.metric("👤 Customers", "—")
    p.metric("📦 Products", "—")
    r.metric("💰 Total Revenue", "—")

    try:
        m = fetch_metrics()
        o.metric("🧾 Orders", f"{m['orders']:,}")
        c.metric("👤 Customers", f"{m['customers']:,}")
        p.metric("📦 Products", f"{m['products']:,}")
        r.metric("💰 Total Revenue", f"${m['revenue']:,.2f}")
    except Exception:
        pass

    st.markdown("---")

    st.subheader("🗣️ Ask in plain English")
    user_question = st.text_area(
        "What would you like to know?",
        height=120,
        key="question_text",
        label_visibility="collapsed",
        placeholder="Example: Show total revenue by region and country, top 20 rows."
    )

    b1, b2, _ = st.columns([1, 1, 6])
    with b1:
        generate_button = st.button("⚡ Generate SQL", type="primary", use_container_width=True)
    with b2:
        local_clear = st.button("🧹 Clear History", use_container_width=True)

    if local_clear:
        st.session_state.query_history = []
        st.session_state.generated_sql = None
        st.session_state.current_question = None

    if generate_button and user_question:
        uq = user_question.strip()
        if st.session_state.current_question != uq:
            st.session_state.generated_sql = None
            st.session_state.current_question = None

        with st.spinner("🧠 Generating SQL..."):
            sql_query = generate_sql_with_gpt(uq)
            if sql_query:
                st.session_state.generated_sql = sql_query
                st.session_state.current_question = uq

    if st.session_state.generated_sql:
        st.markdown("---")
        st.subheader("🧾 Generated SQL")
        st.info(f"**Question:** {st.session_state.current_question}")

        edited_sql = st.text_area(
            "Review and edit the SQL query if needed:",
            value=st.session_state.generated_sql,
            height=220,
        )

        r1, _ = st.columns([1, 7])
        with r1:
            run_button = st.button("▶ Run Query", type="primary", use_container_width=True)

        if run_button:
            with st.spinner("Executing query ..."):
                df = run_query(edited_sql)
                if df is not None:
                    st.session_state.query_history.append(
                        {"question": user_question, "sql": edited_sql, "rows": len(df)}
                    )
                    st.markdown("---")
                    st.subheader("📊 Results")
                    st.success(f"✅ Query returned {len(df)} rows")
                    st.dataframe(df, use_container_width=True)

    if st.session_state.query_history:
        st.markdown("---")
        st.subheader("📜 Query History")
        for idx, item in enumerate(reversed(st.session_state.query_history[-8:])):
            label = f"{item['question'][:70]}..." if len(item["question"]) > 70 else item["question"]
            with st.expander(f"Query {len(st.session_state.query_history)-idx}: {label}"):
                st.markdown(f"**Question:** {item['question']}")
                st.code(item["sql"], language="sql")
                st.caption(f"Returned {item['rows']} rows")
                rr1, _ = st.columns([1, 7])
                with rr1:
                    if st.button("Re-run", key=f"rerun_{idx}", type="primary", use_container_width=True):
                        df = run_query(item["sql"])
                        if df is not None:
                            st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
