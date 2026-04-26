# Use the slim version of 3.12 to keep the image size small
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies required for some python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install your Python libraries
RUN pip install --no-cache-dir \
    pandas \
    pandas-gbq \
    requests \
    google-cloud-bigquery \
    pyarrow \

# Copy your script into the container
COPY la_house_rentcast_api_to_BQ_ETL.py .

# Run the script
CMD ["python", "la_house_rentcast_api_to_BQ_ETL.py"]