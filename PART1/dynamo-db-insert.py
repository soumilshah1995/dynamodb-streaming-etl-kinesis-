try:
    import datetime
    import json
    import random
    import boto3
    import os
    import uuid
    import time
    from faker import Faker
    from dotenv import load_dotenv

    from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
    from pynamodb.models import Model
    from pynamodb.attributes import *

    load_dotenv(".env")
except Exception as e:
    pass

global faker
faker = Faker()


class Orders(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME_Order")
        aws_access_key_id = os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key = os.getenv("DEV_SECRET_KEY")

    orderid = UnicodeAttribute(hash_key=True)
    customer_id = UnicodeAttribute(range_key=True)
    ts = UnicodeAttribute(null=True)
    order_value = UnicodeAttribute(null=True)
    priority = UnicodeAttribute(null=True)


class Customers(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME_CUSTOMERS")
        aws_access_key_id = os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key = os.getenv("DEV_SECRET_KEY")

    customer_id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(null=True)
    state = UnicodeAttribute(null=True)
    city = UnicodeAttribute(null=True)
    email = UnicodeAttribute(null=True)
    ts = UnicodeAttribute(null=True)


def run():
    order_id = uuid.uuid4().__str__()
    customer_id = uuid.uuid4().__str__()
    partition_key = uuid.uuid4().__str__()

    print("Inserting into orders table ")
    Orders(
        orderid=order_id,
        customer_id=customer_id,
        ts=datetime.now().isoformat().__str__(),
        order_value=random.randint(10, 1000).__str__(),
        priority=random.choice(["LOW", "MEDIUM", "URGENT"])

    ).save()
    print("Going to sleep ")
    time.sleep(3)

    print("Inserting int customer table  ")
    Customers(
        customer_id=customer_id,
        name=faker.name(),
        state=faker.state(),
        city=faker.city(),
        email=faker.email(),
        ts=datetime.now().isoformat().__str__()

    ).save()

    print("Complete")


if __name__ == "__main__":
    run()
