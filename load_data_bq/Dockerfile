# Use the official Python base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Upgrade pip before installing dependencies
RUN pip install --upgrade pip

# Copy the requirements.txt first to install dependencies before the rest of the app
COPY requirements.txt /app/

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire app into the working directory
COPY . /app/

# Expose the port that Cloud Run expects
EXPOSE 8080

# Command to run the application with Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
