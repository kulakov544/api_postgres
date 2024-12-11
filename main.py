from fastapi import FastAPI, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from psycopg2.extras import RealDictCursor
import psycopg2
import redis
from apscheduler.schedulers.background import BackgroundScheduler
import json
from dotenv import load_dotenv
import os


# Конфигурация PostgreSQL
load_dotenv()
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT"))
}

# Параметры схемы и таблицы
DB_SCHEMA = os.getenv("DB_SCHEMA")
DB_TABLE = os.getenv("DB_TABLE")

# Конфигурация Redis из .env
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_CACHE_KEY = "province_region_cache"

# Получение секретного ключа API из .env
API_KEY = os.getenv("API_KEY")

# Инициализация приложения и кеша
app = FastAPI()
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Настройка заголовка для ключа API
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

# Функция для проверки API ключа
def check_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid API Key")
    return api_key

# Функция для подключения к PostgreSQL
def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

# Функция для обновления данных в кеше
def update_cache():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = f"SELECT province, region FROM {DB_SCHEMA}.{DB_TABLE};"
        cursor.execute(query)
        data = cursor.fetchall()
        redis_client.set(REDIS_CACHE_KEY, json.dumps(data))
        print("Cache updated successfully")
    except Exception as e:
        print(f"Error updating cache: {e}")
    finally:
        cursor.close()
        conn.close()

# Инициализация задачи для обновления кеша каждые 30 минут
scheduler = BackgroundScheduler()
scheduler.add_job(update_cache, "interval", minutes=30)
scheduler.start()

# Обновление кеша при запуске сервера
@app.on_event("startup")
def startup_event():
    update_cache()

# Точка API для получения списка уникальных провинций
@app.get("/provinces")
def get_provinces(api_key: str = Depends(check_api_key)):
    try:
        cached_data = redis_client.get(REDIS_CACHE_KEY)
        if cached_data:
            data = json.loads(cached_data)
            provinces = list(set([item["province"] for item in data]))
            return provinces
        else:
            raise HTTPException(status_code=500, detail="Cache is empty")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from cache: {e}")

# Точка API для получения списка уникальных регионов
@app.get("/regions")
def get_regions(api_key: str = Depends(check_api_key)):
    try:
        cached_data = redis_client.get(REDIS_CACHE_KEY)
        if cached_data:
            data = json.loads(cached_data)
            regions = list(set([item["region"] for item in data]))
            return regions
        else:
            raise HTTPException(status_code=500, detail="Cache is empty")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from cache: {e}")

# Точка API для получения регионов для конкретной провинции
@app.get("/regions_for_province/{province}")
def get_regions_for_province(province: str, api_key: str = Depends(check_api_key)):
    try:
        cached_data = redis_client.get(REDIS_CACHE_KEY)
        if cached_data:
            data = json.loads(cached_data)
            regions = [item["region"] for item in data if item["province"] == province]
            if regions:
                return regions
            else:
                raise HTTPException(status_code=404, detail=f"No regions found for province {province}")
        else:
            raise HTTPException(status_code=500, detail="Cache is empty")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from cache: {e}")

# Завершение задач при остановке приложения
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
