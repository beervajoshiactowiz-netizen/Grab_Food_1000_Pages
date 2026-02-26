import json,re,os,gzip,zipfile
from datetime import datetime
import mysql.connector

def load_files(file_path):
    files_app = []
    for files in os.listdir(file_path):
        fullpath = os.path.join(file_path, files)
        try:
            with gzip.open(fullpath, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    files_app.append(data)
        except Exception as e:
            print("Error in file:", files, e)
    return files_app


def parser(pages):
    page=[]
    for data in pages:

        merchant = data.get("merchant") or {}

        # Main dictionary
        result = {
            "merchant_id": merchant.get("ID"),
            "name": merchant.get("name"),
            "cuisine": merchant.get("cuisine"),
            "timingEveryday":merchant.get("openingHours",{}).get("sun"),
            "distance": merchant.get("distanceInKm"),
            "ETA": merchant.get("ETA"),
            "rating": merchant.get("rating"),
            "DeliveryBy":merchant.get("deliverBy"),
            "DeliveryOption":merchant.get("deliveryOptions"),
            "VoteCount": merchant.get("voteCount"),
            "Tips": [merchant.get("sofConfiguration",{}).get("tips")],
            "BuisinessType":merchant.get("businessType"),
            "Offers":[],
            "menu": []
        }
        for offers in merchant.get("offerCarousel",{}).get("offerHighlights",[]):
            off={
                    "Title": offers.get("highlight").get("title"),
                    "SubTitle":offers.get("highlight").get("subtitle")
            }
            result["Offers"].append(off)

        # Build Menu List Category Wise
        for category in merchant.get("menu", {}).get("categories", []):

            category_block = {
                "category_name": category.get("name"),
                "items": []
            }

            for item in category.get("items", []):

                item_block = {
                    "item_id": item.get("ID"),
                    "name": item.get("name"),
                    "description": item.get("description"),
                    "price_display": float(item.get("priceV2", {}).get("amountDisplay")),
                    "available": item.get("available"),
                    "images": item.get("imgHref"),
                }

                category_block["items"].append(item_block)

            result["menu"].append(category_block)
        result = {k: v for k, v in result.items() if v is not None}
        page.append(result)
    return page


def data_extracted(extracted_data):
    with open(f"Grab_food_Pages{datetime.now().date()}.json","wb") as f:
        f.write(json.dumps(extracted_data, indent=4,ensure_ascii=False).encode())


file_name="grab_food_pages"
file_data=load_files(file_name)
extracted_data = parser(file_data)
data_extracted(extracted_data)
