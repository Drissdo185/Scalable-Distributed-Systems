from kafka import KafkaConsumer
import json
from datetime import datetime
import pandas as pd

class LoanConsumer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092', topic='loan_applications'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='loan_processing_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.processed_count = 0

    def process_message(self, message):
        """Process individual message"""
        loan_data = message.value
        print(loan_data)
        print(f"\nReceived Loan Application:")
        print(f"Loan ID: {loan_data['LoanID']}")
        print(f"Amount: ${loan_data['LoanAmount']}")
        print(f"Purpose: {loan_data['LoanPurpose']}")
        print("-" * 50)
        self.processed_count += 1

    def start_consuming(self):
        """Start consuming messages"""
        print("Starting consumer... Press Ctrl+C to stop")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        finally:
            self.consumer.close()
            print(f"Processed {self.processed_count} messages")

if __name__ == "__main__":
    consumer = LoanConsumer()
    consumer.start_consuming()