import json
import psycopg2
from kafka import KafkaProducer

employee_topic_name = "employee_cdc"

class CaphcaProducer:
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        self.producer = KafkaProducer(bootstrap_servers=f"{self.host}:{self.port}", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def scan_cdc_table(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                port="5433",
                database="employees_db_a",
                user="postgres",
                password="postgres"
            )
            cur = conn.cursor()
            cur.execute("SELECT * FROM employees_cdc")
            rows = cur.fetchall()
            for row in rows:
                employee_data = {
                    'emp_id': row[0],
                    'first_name': row[1],
                    'last_name': row[2],
                    'dob': row[3].strftime('%Y-%m-%d'),  # Convert date to string
                    'city': row[4],
                    'action': row[5]
                }
                self.producer.send(employee_topic_name, employee_data)
            cur.execute("DELETE FROM employees_cdc")
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    producer = CaphcaProducer()
    producer.scan_cdc_table()