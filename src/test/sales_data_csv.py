import csv
import  os
import random
from datetime import timedelta,datetime

# from turtledemo.penrose import start

customer_ids = list(range(1,21000))

store_ids = list(range(121,124))

product_data = { 'sugar': 50,
                 'oats': 50,
                 'aata': 34,
                 'maida':20,
                 'beasan': 52,
                 'refined_oil':115,
                 'nutrella':40,
                 'sensodyne':135,
                 'rice':110,
                 'dove':2
                }

sales_person = {
                121 : [1, 2, 3],
                122 : [4, 5, 6],
                123 : [7, 8, 9]
              }
start_date = datetime(2024,3,14)
end_date   = datetime(2024,9,23)

file_location = 'C:\\Users\\satya\\Documents\\sales_data_generate\\'
csv_file_path = os.path.join(file_location, 'sales_data5.csv')

with open(csv_file_path, 'w+', newline='') as csv_file:
    csv_write = csv.writer(csv_file)

    csv_write.writerow(['customer_id', 'store_id', 'product_name', 'sales_date'
                        , 'sales_person_id', 'price', 'quantity', 'total_cost'])

    for i in range(5000000):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(days = random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_person[store_id])
        price = product_data[product_name]
        quantity = random.randint(1,10)
        total_cost = price * quantity

        csv_write.writerow([customer_id, store_id, product_name,sales_date.strftime('%Y-%m-%d')
                           , sales_person_id, price, quantity, total_cost])

print('csv file generated successfully')
