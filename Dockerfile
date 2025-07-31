# Use an official Python runtime as the base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY main.py main.py
COPY requirements.txt requirements.txt

# Install any needed packages specified in requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]

