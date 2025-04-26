import mysql.connector
import time

conn = mysql.connector.connect(
    host="172.19.96.1",
    user="ubuntu",
    password="adzoopass",
    database="fitness_data"
)

cursor = conn.cursor()

start = time.time()

cursor.execute("""
    SELECT 
      user_id,
      AVG(avg_heart_rate),
      SUM(total_steps),
      SUM(total_calories)
    FROM user_aggregates
    GROUP BY user_id
""")

results = cursor.fetchall()
end = time.time()

print("Batch processing time: {:.4f} seconds".format(end - start))

for row in results:
    print(row)

conn.close()

