import mysql.connector
import matplotlib.pyplot as plt

# Connect to MySQL
conn = mysql.connector.connect(
    host="172.19.96.1",
    user="ubuntu",
    password="adzoopass",
    database="fitness_data"
)

cursor = conn.cursor()

# Get total steps from STREAMING windows
cursor.execute("""
    SELECT user_id, SUM(total_steps)
    FROM user_aggregates
    GROUP BY user_id
""")

stream_data = cursor.fetchall()
conn.close()

# Unpack
users = [row[0] for row in stream_data]
steps = [int(row[1]) for row in stream_data]

# Plot
plt.figure(figsize=(10, 6))
plt.bar(users, steps, color='skyblue')
plt.title("Total Steps per User (Streaming Mode)")
plt.xlabel("User ID")
plt.ylabel("Steps (from Spark windows)")
plt.tight_layout()
plt.savefig("stream_steps_per_user.png")
plt.show()

