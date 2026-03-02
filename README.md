# Data Engineering Platform – Cobranza

Pipeline de Data Engineering construido en **Google Cloud Platform (GCP)** para procesar datos de cobranza y generar dashboards analíticos en **Power BI**.

El proyecto implementa una arquitectura **Medallion (Bronze, Silver, Gold)** con automatización mediante **CI/CD**.

---

## Arquitectura

Flujo de datos:

Raw (local CSV) → Bronze (Cloud Storage) → Silver (BigQuery) → Gold (BigQuery) → Power BI

Tecnologías principales:

- Python
- Google Cloud Storage
- BigQuery
- Cloud Composer (Airflow)
- Spark Serverless
- GitHub Actions
- Docker
- Power BI

---

## Estructura del proyecto
