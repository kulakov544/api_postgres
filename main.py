from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor
import psycopg2
import redis
from apscheduler.schedulers.background import BackgroundScheduler
import json

# Конфигурация PostgreSQL
DB_CONFIG = {
    "dbname": "db.cropdata.ru",
    "user": "your_user",
    "password": "your_password",
    "host": "localhost",
    "port": 5432
}

# Конфигурация Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_CACHE_KEY = "province_region_cache"

# Инициализация приложения и кеша
app = FastAPI()
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

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
        cursor.execute("SELECT province, region FROM your_table;")
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

# Точка API для получения данных
@app.get("/data")
def get_data():
    try:
        cached_data = redis_client.get(REDIS_CACHE_KEY)
        if cached_data:
            return json.loads(cached_data)
        else:
            raise HTTPException(status_code=500, detail="Cache is empty")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from cache: {e}")

# Завершение задач при остановке приложения
@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
