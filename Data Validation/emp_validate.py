import pandas as pd
import os
import csv
from datetime import datetime

FILE_PATH = '/content/employees.csv'

def read_csv(FILE_PATH):
  with open(FILE_PATH, newline='', encoding='utf-8') as csvfile:
      reader = csv.DictReader(csvfile)
      return list(reader)

# This is for existence assertion
def validate_existence_assertion(data):

    missing_name = 0
    missing_title = 0 
    
    for row in data:
        if not row['name'] or row['name'].strip() == '':
                missing_name += 1

        if not row['title'] or row['title'].strip() == '':
                missing_title += 1

    print(f"Existence Assertion Records with missing name: {missing_name}")
    print(f"Existence Assertion Records with missing title: {missing_title}")

# This is for limit assertion
def validate_limit_assertion(data):

    hire_early_date = 0
    salary_high_than_100000 = 0

    for row in data:
      try:
        hire_date = datetime.strptime(row['hire_date'], '%Y-%m-%d')

        if hire_date < datetime(2015, 1, 1):
          hire_early_date += 1
         
        salary = float(row['salary'])

        if salary > 100000:
           salary_high_than_100000 += 1

      except ValueError:
          hire_early_date += 1
          salary_high_than_100000 += 1


    print(f"Limit Assertion Records hired before 2015: {hire_early_date}")
    print(f"Limit Assertion Salary high than 100000: {salary_high_than_100000}")

# This is for intra record assertion
def validate_intrarecord_assertion(data):

    hire_before_birth_date = 0
    phone_without_postal = 0

    for row in data:
      try:
        birth_date = datetime.strptime(row['birth_date'], '%Y-%m-%d')
        hire_date = datetime.strptime(row['hire_date'], '%Y-%m-%d')

        if birth_date >= hire_date:
           hire_before_birth_date += 1

        phone = row.get('phone', '').strip()
        postal_code = row.get('postal_code', '').strip()

        if phone and not postal_code:
            phone_without_postal += 1

      except ValueError:
        hire_before_birth_date += 1
        phone_without_postal += 1

    print(f"Intra-record Assertion Records employee born before they hired: {hire_before_birth_date}")
    print(f"Intra-record Assertion Records phone without postal code: {phone_without_postal}")

if __name__ == "__main__":
    data = read_csv(FILE_PATH)               
    validate_existence_assertion(data)
    validate_limit_assertion(data)
    validate_intrarecord_assertion(data)  
