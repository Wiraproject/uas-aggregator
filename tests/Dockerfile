FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command: jalankan pytest
CMD ["pytest", "-v", "integration_test.py"]