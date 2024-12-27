from faker import Faker
import time 
from datetime import datetime
from kafka import KafkaProducer
import json 
import uuid
import random

class LoanSimulator:
    def __init__(self, kafka_bootstrap_servers='localhost:9092', topic='loan_application'):
        self.producer = KafkaProducer(
            bootstrap_servers = kafka_bootstrap_servers,
            # used to convert user-supplied message values to bytes
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
        
        
        self.age_range = (18, 75)
        self.income_range = (20000, 200000)
        self.loan_amount_range = (5000, 100000)
        self.credit_score_range = (300, 850)
        self.months_employed_range = (0, 480)
        self.education_levels = ['High School', 'Bachelor', 'Master', 'PhD']
        self.employment_types = ['Full-time', 'Part-time', 'Self-employed', 'Unemployed']
        self.marital_status = ['Single', 'Married', 'Divorced']
        self.loan_purposes = ['Home', 'Other', 'Education', 'Business', 'Auto']
    
    def generate_loan_application(self):
        loan_data = {
            'LoanID': str(uuid.uuid4()),
            'Age': random.randint(*self.age_range),
            'Income': random.randint(*self.income_range),
            'LoanAmount': random.randint(*self.loan_amount_range),
            'CreditScore': random.randint(*self.credit_score_range),
            'MonthsEmployed': random.randint(*self.months_employed_range),
            'NumCreditLines': random.randint(0, 10),
            'InterestRate': round(random.uniform(5, 25), 2),
            'LoanTerm': random.choice([12, 24, 36, 48, 60]),
            'DTIRatio': round(random.uniform(0, 0.5), 2),
            'Education': random.choice(self.education_levels),
            'EmploymentType': random.choice(self.employment_types),
            'MaritalStatus': random.choice(self.marital_status),
            'HasMortgage': random.choice(['Yes', 'No']),
            'HasDependents': random.choice(['Yes', 'No']),
            'LoanPurpose': random.choice(self.loan_purposes),
            'HasCoSigner': random.choice(['Yes', 'No']),
            # 'Default': random.choice([0, 1])
        }
        return loan_data

    def simulate_stream(self, interval=5, num_events=None):
        """
        Generate and send loan applications continuously
        interval: time between events in seconds
        num_events: optional limit on number of events to generate
        """
        count = 0
        try:
            while True:
                if num_events and count >= num_events:
                    break
                    
                loan_data = self.generate_loan_application()
                self.producer.send(self.topic, loan_data)
                print(f"Sent loan application {loan_data['LoanID']}")
                
                count += 1
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nStopping simulation...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    simulator = LoanSimulator()
    simulator.simulate_stream(interval=1)
    