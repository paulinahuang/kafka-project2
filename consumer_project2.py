import json
import psycopg2
from kafka import KafkaConsumer

employee_topic_name = "employee_cdc"

class CaphcaConsumer:
    def __init__(self, host="localhost", port="29092", group_id="employee_consumer"):
        self.consumer = KafkaConsumer(
            employee_topic_name,
            bootstrap_servers=f"{host}:{port}",
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def consume_messages(self):
        try:
            for msg in self.consumer:
                print(f"Received message: {msg.value}")
                employee_data = msg.value
                persist_employee(employee_data)
        finally:
            self.consumer.close()

def persist_employee(employee_data):
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5434",
            database="employees_db_b",
            user="postgres",
            password="postgres"
        )
        conn.autocommit = True
        cur = conn.cursor()
        action = employee_data['action']
        if action == 'INSERT':
            insert_query = """
                INSERT INTO employees (emp_id, first_name, last_name, dob, city)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                employee_data['emp_id'],
                employee_data['first_name'],
                employee_data['last_name'],
                employee_data['dob'],
                employee_data['city']
            ))
        elif action == 'UPDATE':
            update_query = """
                UPDATE employees
                SET first_name = %s, last_name = %s, dob = %s, city = %s
                WHERE emp_id = %s
            """
            cur.execute(update_query, (
                employee_data['first_name'],
                employee_data['last_name'],
                employee_data['dob'],
                employee_data['city'],
                employee_data['emp_id']
            ))
        elif action == 'DELETE':
            delete_query = "DELETE FROM employees WHERE emp_id = %s"
            cur.execute(delete_query, (employee_data['emp_id'],))
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    consumer = CaphcaConsumer()
    consumer.consume_messages()