# 1. Use an official Python runtime as a parent image
FROM python:3.10-alpine

# 2. Set the working directory in the container
WORKDIR /app

# 3. Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of the application code
COPY . .

# 6. Command to run the application
CMD ["python", "main.py"]
