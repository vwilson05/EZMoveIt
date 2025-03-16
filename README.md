# **EZMoveIt: Lightweight Data Pipeline Orchestrator 🚀**

EZMoveIt is a **lightweight data ingestion and orchestration tool** built with **Python, Streamlit, and DuckDB**, leveraging **DLT** for seamless extraction and loading into **Snowflake**. It supports multiple **source types** (APIs, databases, cloud storage) and includes scheduling, logging, and metrics tracking.

---

## **🛠 Features**

- **🔗 Connect to multiple sources:** REST APIs (public/private), databases (Postgres, MySQL, BigQuery, Redshift, etc.), and S3.
- **👤 Load data into Snowflake** with automated schema management.
- **🗕 Schedule recurring data loads** or run one-time ingestions.
- **📊 View execution logs & pipeline metrics** inside the UI.
- **🔄 Handles schema drift & supports incremental loads** (WIP).

---

## **🛀 Project Structure**

```txt

EZMoveIt/
│── config/               # Stores connection configs (Snowflake, API keys, etc.)
│── data/                 # DuckDB database storage
│── src/
│   ├── db/               # Database connection & initialization scripts
│   ├── pipelines/        # DLT pipeline execution scripts
│   ├── sources/          # Data extraction from APIs, databases, and storage
│   ├── streamlit_app/    # Streamlit UI components
│       ├── pages/        # Modular Streamlit pages
│── venv/                 # Virtual environment (ignored in Git)
│── requirements.txt      # Required dependencies
│── README.md             # You’re reading this! 😊
│── .gitignore            # Ignore unnecessary files
```

---

## **🚀 Quickstart Guide**

### **1️⃣ Install Dependencies**

Clone the repo and install required Python packages:

```bash
git clone https://github.com/vwilson05/EZMoveIt.git
cd EZMoveIt
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

Initialize your duckdb and tables:
```bash
python src/db/duckdb_init.py
```

---

### **2️⃣ Configure Snowflake & Data Sources**

- **Snowflake:** Add your credentials to `config/snowflake_config.json`  
- **APIs/Databases:** Configuration files will be automatically created when setting up a new pipeline.

---

### **3️⃣ Run the Application**

Start the Streamlit UI:

```bash
streamlit run src/streamlit_app/app.py
```

Navigate to **`http://localhost:8501`** in your browser.

---

## **🖥 How to Use**

### **📌 Create a Pipeline**

1. Go to **Pipeline Creator**.
2. Select a **Data Source** (API, database, or cloud storage).
3. Enter **connection details**.
4. Choose **One-Time Run** or **Schedule Recurring Run**.
5. Click **Create Pipeline**.

### **🚀 Run & Monitor Pipelines**

- **Trigger manually** from the home screen.
- **View execution logs** in the **Execution Logs** tab.
- **Analyze pipeline performance** in the **Metrics** tab.

---

## **🖥 Supported Sources**

✅ **REST APIs (Public & Private)**  
✅ **Databases:** Postgres, MySQL, SQL Server, BigQuery, Redshift  
✅ **Cloud Storage:** S3  
✅ More coming soon!

---

## **📚 Documentation & Troubleshooting**

- **Common Issues:**  
  - Missing dependencies? Run `pip install -r requirements.txt`.
  - Permission issues? Check your `.env` and config files.
  - DuckDB errors? Ensure your `data/EZMoveIt.duckdb` file is properly initialized.

- **Schema Changes:**  
  - EZMoveIt **appends** new fields automatically when sources change.

- **Incremental Loads:**  
  - Work in progress! Future updates will allow column-based tracking.

---

## **🤝 Contributing**

We welcome contributions! Open an issue or PR to improve **EZMoveIt**.

---

## **🐝 License**

MIT License © 2025 **Victor Wilson**  
