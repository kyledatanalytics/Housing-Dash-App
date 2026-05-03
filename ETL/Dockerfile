# Use a lightweight Python image
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY la_house_rentcast_api_to_BQ_ETL_Cloud_Run.py .
# Run the script when the container starts
CMD ["python", "la_house_rentcast_api_to_BQ_ETL_Cloud_Run.py"]