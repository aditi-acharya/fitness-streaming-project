import mysql.connector
from tabulate import tabulate

# Connect to MySQL
conn = mysql.connector.connect(
    host='172.19.96.1',
    port=3306,
    user='ubuntu',
    password='*****',
    database='fitness_data'
)

cursor = conn.cursor()

# 1. Total number of high heart rate alerts per user
print("\n High Heart Rate Alerts per User:")
cursor.execute("SELECT user_id, COUNT(*) AS alert_count FROM alerts GROUP BY user_id;")
rows = cursor.fetchall()
print(tabulate(rows, headers=["User ID", "Alert Count"], tablefmt="grid"))

# 2. Maximum average heart rate per user
print("\n Max Average Heart Rate per User:")
cursor.execute("SELECT user_id, MAX(avg_heart_rate) AS max_avg_hr FROM user_aggregates GROUP BY user_id;")
rows = cursor.fetchall()
print(tabulate(rows, headers=["User ID", "Max Avg HR"], tablefmt="grid"))

# 3. Top 5 users by total steps
print("\n‍♂️ Top 5 Users by Total Steps:")
cursor.execute("SELECT user_id, SUM(total_steps) AS total_steps FROM user_aggregates GROUP BY user_id ORDER BY total_steps DESC LIMIT 5;")
rows = cursor.fetchall()
print(tabulate(rows, headers=["User ID", "Total Steps"], tablefmt="grid"))

# Clean up
cursor.close()
conn.close()

