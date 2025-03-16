# Use official Python image as base
FROM python:3.11

# Set the working directory inside the container
WORKDIR /app

# Copy necessary files
COPY requirements.txt ./
COPY config/ ./config/
COPY data/ ./data/
COPY src/ ./src/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run database initialization
RUN python src/db/duckdb_init.py

# Expose Streamlit's default port
EXPOSE 8501

# Set environment variables
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Run Streamlit app
CMD ["streamlit", "run", "src/streamlit_app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
