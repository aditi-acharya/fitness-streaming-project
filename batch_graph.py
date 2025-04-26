import mysql.connector
import matplotlib.pyplot as plt

# Connect to MySQL
conn = mysql.connector.connect(
    host="172.19.96.1",
    user="ubuntu",
    password="*****",
    database="fitness_data"
)

cursor = conn.cursor()

# Query for total steps per user
cursor.execute("""
    SELECT user_id, SUM(total_steps) 
    FROM user_aggregates
    GROUP BY user_id
""")

data = cursor.fetchall()
conn.close()

# Separate into labels and values
users = [row[0] for row in data]
steps = [int(row[1]) for row in data]

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(users, steps)
plt.title("Total Steps per User (Batch Mode)")
plt.xlabel("User ID")
plt.ylabel("Total Steps")
plt.tight_layout()
plt.savefig("total_steps_per_user.png")
plt.show()

