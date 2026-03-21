# NLP-to-SQL: Generative AI Query Engine

An intelligent data retrieval tool that translates natural language questions into precise SQL queries using the **Google Gemini Pro** LLM. 

## 🚀 Overview
This project bridges the gap between non-technical users and databases. Users can ask questions in plain English (e.g., "Show me all students in the Data Science department"), and the app generates and executes the corresponding SQL query against an SQLite database.

## 🛠️ Tech Stack
* **LLM Engine:** Google Gemini Pro
* **Frontend:** Streamlit
* **Language:** Python 3.x
* **Database:** PostgreSQL

## 🌟 Key Features
* **Natural Language Processing:** Leverages Google's state-of-the-art Generative AI for complex query generation.
* **Live Execution:** Not only generates the code but executes it and displays results instantly.
* **Prompt Engineering:** Fine-tuned system prompts to ensure the LLM adheres to specific table schemas.

## 📋 How It Works
1.  **Input:** User enters a question in the Streamlit search bar.
2.  **Prompting:** The application wraps the question in a specific system prompt containing the database schema context.
3.  **LLM Processing:** Gemini Pro generates the SQL syntax.
4.  **Execution:** The app runs the query on the `student.db`.
5.  **Output:** Results are rendered in a clean, tabular format.

## ⚙️ Installation & Setup
1. Clone the repository:
   ```bash
   git clone [https://github.com/Aayushnepal09/Mini_project_2.git](https://github.com/Aayushnepal09/Mini_project_2.git)

2. Install dependencies:
Bash
pip install -r requirements.txt

3. Set up your Google API Key:
  Create a .env file in the root directory.
  Add your key: GOOGLE_API_KEY="your_api_key_here"

4. Run the app:
  Bash
  streamlit run app.py
