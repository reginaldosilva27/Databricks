import json
from faker import Faker
import random

fake = Faker()

for i in range(1, 11):
    data = [{
        'name': fake.name(),
        'address': fake.address(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'job': fake.job(),
        'age': random.randint(18, 65),
        'company': fake.company(),
        'credit_card_number': fake.credit_card_number(),
        'date_joined': str(fake.date_this_decade())
    }]

    with open(f'/Users/reginaldosilva/Documents/Jsons/data{i}.json', 'w') as f:
        json.dump(data, f, indent=4)
