import streamlit as st
import importlib
import inspect
import sys
from pathlib import Path

# Set up sys.path
root_path = Path(__file__).resolve().parents[3]
src_path = root_path / "src"
readme_path = root_path / "README.md"
for p in [str(root_path), str(src_path)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# App modules to auto-document
MODULES = {
    "DLT Pipeline": "pipelines.dlt_pipeline",
    "API Source": "sources.api_source",
    "Database Source": "sources.database_source",
    "File Source": "sources.file_source",
    "Pipeline Creator Page": "streamlit_app.page_modules.pipeline_creator",
    "Pipeline Metrics Page": "streamlit_app.page_modules.pipeline_metrics",
    "Execution Logs Page": "streamlit_app.page_modules.execution_logs",
    "Monitoring Dashboard": "streamlit_app.monitoring_dashboard",
}

def docs_page():
    st.title("📘 EZMoveIt Documentation")
    st.caption("Live docs with `inspect`, Markdown, and dynamic navigation.")

    nav = st.sidebar.selectbox("📚 Docs Navigation", ["Overview"] + list(MODULES.keys()))

    if nav == "Overview":
        render_overview()
    else:
        module_path = MODULES.get(nav)
        st.header(f"📁 {nav} ({module_path})")
        try:
            mod = importlib.import_module(module_path)
            members = inspect.getmembers(mod)
            for name, obj in members:
                if inspect.isfunction(obj):
                    render_function(name, obj)
                elif inspect.isclass(obj):
                    render_class(name, obj)
                elif not name.startswith("__"):
                    render_variable(name, obj)
        except Exception as e:
            st.error(f"❌ Could not import `{module_path}`: {e}")

def render_overview():
    st.subheader("📖 Project Overview")
    if readme_path.exists():
        with readme_path.open("r") as f:
            st.markdown(f.read())
    else:
        st.warning("README.md not found. Please place it in the project root.")

    st.subheader("🧭 What Each Page Does")
    st.markdown("""
    - **Pipeline Creator**: Build new pipelines using API, database, or file sources.
    - **Monitoring Dashboard**: View real-time status and metrics of running pipelines.
    - **Execution Logs**: Drill into historical runs and trace errors.
    - **Pipeline Metrics**: Analyze ingestion patterns, volume, duration.
    - **Docs**: You're here! Live API and UX documentation.

    ### How the App Works
    EZMoveIt combines `dlt` pipelines, DuckDB logging, and a Streamlit UI. Configs and credentials are stored under `/config`, and all logs go into a local DuckDB. Pipelines are dynamically built and executed using a standard template.

    ### How to Use It
    1. Go to Pipeline Creator and choose a data source.
    2. Enter your credentials and pipeline parameters.
    3. Run it once or schedule it.
    4. View results in the Monitoring Dashboard and Logs.

    This will grow into a full-featured, searchable, developer- and analyst-friendly docs hub — just like the DLT docs.
    """)

def render_function(name, fn):
    with st.expander(f"🔹 Function: `{name}()`"):
        st.code(inspect.signature(fn), language="python")
        doc = inspect.getdoc(fn)
        if doc:
            st.markdown(doc)

def render_class(name, cls):
    with st.expander(f"📦 Class: `{name}`"):
        doc = inspect.getdoc(cls)
        if doc:
            st.markdown(doc)
        for meth_name, meth in inspect.getmembers(cls, predicate=inspect.isfunction):
            st.markdown(f"**Method `{meth_name}()`**")
            st.code(inspect.signature(meth), language="python")
            doc = inspect.getdoc(meth)
            if doc:
                st.markdown(doc)

def render_variable(name, val):
    with st.expander(f"🧮 Variable `{name}`"):
        st.code(repr(val))
