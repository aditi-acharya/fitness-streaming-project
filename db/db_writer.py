import mysql.connector

# UPDATE THESE with your actual MySQL details
MYSQL_HOST = "172.19.96.1"
MYSQL_USER = "ubuntu"
MYSQL_PASSWORD = "adzoopass"
MYSQL_DB = "fitness_data"

def insert_aggregate(data):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO user_aggregates (user_id, window_start, window_end, avg_heart_rate, total_steps, total_calories)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = (
        data['user_id'],
        data['window']['start'],
        data['window']['end'],
        data['avg_heart_rate'],
        data['total_steps'],
        data['total_calories']
    )
    cursor.execute(query, values)
    conn.commit()
    conn.close()

def insert_alert(data):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO alerts (user_id, timestamp, heart_rate, steps, calories_burned)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = (
        data['user_id'],
        data['timestamp'],
        data['heart_rate'],
        data['steps'],
        data['calories_burned']
    )
    cursor.execute(query, values)
    conn.commit()
    conn.close()

