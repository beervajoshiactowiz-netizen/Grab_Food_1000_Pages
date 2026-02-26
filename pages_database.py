import json
import mysql.connector
import time


input_file = "Grab_food_Pages2026-02-26.json"
def load_file(file_name):
    with open(file_name, "rb") as f:
        data = json.loads(f.read().decode())
    return data

data=load_file(input_file)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="actowiz",
    database="grabfood_db"
)
cursor = conn.cursor()


create_res_query = """
CREATE TABLE IF NOT EXISTS grab_restaurant (
    merchant_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    cuisine VARCHAR(100),
    timingEveryday VARCHAR(100),
    distance FLOAT,
    ETA INT,
    rating FLOAT,
    DeliveryBy VARCHAR(100),
    DeliveryOption JSON,
    VoteCount INT,
    Tips JSON,
    BuisinessType VARCHAR(50),
    Offers JSON
);
"""
cursor.execute(create_res_query)

insert_res = """
INSERT INTO grab_restaurant
(merchant_id, name, cuisine, timingEveryday, distance,
 ETA, rating, DeliveryBy,DeliveryOption, VoteCount, Tips, BuisinessType,Offers)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
ON DUPLICATE KEY UPDATE
merchant_id=VALUES(merchant_id),
name=VALUES(name),
cuisine=VALUES(cuisine),
timingEveryday=VALUES(timingEveryday),
distance=VALUES(distance),
ETA=VALUES(ETA),
rating=VALUES(rating),
DeliveryBy=VALUES(DeliveryBy),
DeliveryOption=VALUES(DeliveryOption),
VoteCount=VALUES(VoteCount),
Tips=VALUES(Tips),
BuisinessType=VALUES(BuisinessType),
Offers= VALUES(Offers)
"""

create_menu_query = """
CREATE TABLE IF NOT EXISTS menu (
    item_id VARCHAR(100) PRIMARY KEY,
    merchant_id VARCHAR(100),
    category_name VARCHAR(100),
    name VARCHAR(255),
    description TEXT,
    price FLOAT,
    available BOOLEAN,
    images JSON,
    FOREIGN KEY (merchant_id) REFERENCES grab_restaurant(merchant_id) 
);
"""
cursor.execute(create_menu_query)

menu_insert = """
INSERT INTO menu
(item_id, merchant_id, category_name, name,
 description, price, available, images)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
item_id=VALUES(item_id),
merchant_id=VALUES(merchant_id),
category_name=VALUES(category_name),
name=VALUES(name),
description=VALUES(description),
price=VALUES(price),
available=VALUES(available),
images=VALUES(images)
"""
for restaurant in data:
    m_id = restaurant.get("merchant_id")
    if not m_id:
        continue

    cursor.execute(insert_res, (
        m_id,
        restaurant.get("name"),
        restaurant.get("cuisine"),
        restaurant.get("timingEveryday"),
        restaurant.get("distance"),
        restaurant.get("ETA"),
        restaurant.get("rating"),
        restaurant.get("DeliveryBy"),
        json.dumps(restaurant.get("DeliveryOption", [])),
        restaurant.get("VoteCount"),
        json.dumps(restaurant.get("Tips", [])),
        restaurant.get("BuisinessType"),
        json.dumps(restaurant.get("Offers", []))
    ))

    # Nested loop for menu
    for category in restaurant.get("menu", []):
        category_name = category.get("category_name")

        for item in category.get("items", []):
            cursor.execute(menu_insert, (
                item.get("item_id"),
                restaurant.get("merchant_id"),
                category_name,
                item.get("name"),
                item.get("description"),
                item.get("price_display"),
                item.get("available"),
                json.dumps(item.get("images", []))
            ))
conn.commit()
cursor.close()
conn.close()