FROM python:3.11-slim

# directorio de trabajo
WORKDIR /app

# copiar dependencias primero (mejora cache)
COPY requirements.txt .

# instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# copiar código del proyecto
COPY src ./src
COPY data ./data

# comando por defecto
CMD ["python", "src/ingestion/upload_bronze.py"]