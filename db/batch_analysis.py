import mysql.connector
from tabulate import tabulate

# MySQL connection details
conn = mysql.connector.connect(
    host="172.19.96.1",
    user="ubuntu",
    password="adzoopass",
    database="fitness_data"
)

cursor = conn.cursor()

print("===== Batch Aggregation Results =====\n")

# Query 1: Overall average heart rate and total steps per user
cursor.execute("""
    SELECT 
        user_id,
        ROUND(AVG(avg_heart_rate), 2) AS overall_avg_heart_rate,
        SUM(total_steps) AS total_steps,
        ROUND(SUM(total_calories), 2) AS total_calories
    FROM user_aggregates
    GROUP BY user_id
""")
agg_results = cursor.fetchall()
print(tabulate(agg_results, headers=["User", "Avg HR", "Total Steps", "Total Calories"], tablefmt="pretty"))

print("\n===== Alert Count Per User =====\n")

# Query 2: Total alerts per user
cursor.execute("""
    SELECT 
        user_id,
        COUNT(*) AS alert_count
    FROM alerts
    GROUP BY user_id
""")
alert_results = cursor.fetchall()
print(tabulate(alert_results, headers=["User", "Alert Count"], tablefmt="pretty"))

conn.close()

