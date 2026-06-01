import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC = "raw_transactions"

# ---------------------------------------------------------------------------
# Egyptian payment domain data
# ---------------------------------------------------------------------------
SERVICE_TYPES = [
    "electricity_bill",
    "water_bill",
    "gas_bill",
    "mobile_topup",
    "internet_bill",
    "school_fees",
    "university_fees",
    "traffic_fines",
    "telephone_bill",
    "credit_card_payment",
    "insurance_premium",
    "pos_purchase",
    "ecommerce_payment",
    "government_fees",
    "donations",
]

GOVERNORATES = [
    "Cairo",
    "Giza",
    "Alexandria",
    "Qalyubia",
    "Sharqia",
    "Dakahlia",
    "Beheira",
    "Minya",
    "Assiut",
    "Sohag",
    "Luxor",
    "Aswan",
    "Port Said",
    "Suez",
    "Ismailia",
]

PAYMENT_METHODS = [
    "wallet",
    "debit_card",
    "credit_card",
    "bank_transfer",
    "cash_at_agent",
]

STATUSES = ["success", "success", "success", "success", "failed", "pending"]

# Realistic amount ranges per service type in EGP
AMOUNT_RANGES = {
    "electricity_bill":    (50,   800),
    "water_bill":          (20,   300),
    "gas_bill":            (30,   400),
    "mobile_topup":        (10,   500),
    "internet_bill":       (100,  600),
    "school_fees":         (500,  5000),
    "university_fees":     (2000, 20000),
    "traffic_fines":       (100,  1000),
    "telephone_bill":      (50,   400),
    "credit_card_payment": (200,  30000),
    "insurance_premium":   (300,  5000),
    "pos_purchase":        (20,   3000),
    "ecommerce_payment":   (50,   5000),
    "government_fees":     (50,   2000),
    "donations":           (10,   1000),
}

# ---------------------------------------------------------------------------
# Fraud injection logic
# ---------------------------------------------------------------------------
def inject_fraud(transaction: dict) -> dict:
    """
    Randomly flag ~3% of transactions as fraudulent and alter
    them to look suspicious (very high amount, odd hour, etc.)
    """
    if random.random() < 0.03:
        transaction["is_fraud"] = True
        # Fraud signals: unusually large amount
        transaction["amount_egp"] = round(random.uniform(15000, 50000), 2)
        # Fraud signals: transaction at an odd hour (1am - 4am)
        transaction["event_time"] = datetime.utcnow().replace(
            hour=random.randint(1, 4),
            minute=random.randint(0, 59)
        ).isoformat()
    else:
        transaction["is_fraud"] = False
    return transaction

# ---------------------------------------------------------------------------
# Transaction generator
# ---------------------------------------------------------------------------
def generate_transaction() -> dict:
    service = random.choice(SERVICE_TYPES)
    low, high = AMOUNT_RANGES[service]
    amount = round(random.uniform(low, high), 2)

    transaction = {
        "transaction_id":  str(uuid.uuid4()),
        "event_time":      datetime.utcnow().isoformat(),
        "customer_id":     str(uuid.uuid4()),
        "merchant_id":     f"MERCH-{random.randint(1000, 9999)}",
        "service_type":    service,
        "amount_egp":      amount,
        "governorate":     random.choice(GOVERNORATES),
        "payment_method":  random.choice(PAYMENT_METHODS),
        "status":          random.choice(STATUSES),
        "is_fraud":        False,
    }

    transaction = inject_fraud(transaction)
    return transaction

# ---------------------------------------------------------------------------
# Main loop — connect to Kafka and stream transactions
# ---------------------------------------------------------------------------
def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

    print(f"Connected. Streaming transactions to topic '{TOPIC}'...")
    print("Press Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            transaction = generate_transaction()
            producer.send(TOPIC, value=transaction)
            count += 1

            # Print every 10th transaction so we can see it working
            if count % 10 == 0:
                print(f"[{count}] Sent: {transaction['transaction_id']} | "
                      f"{transaction['service_type']} | "
                      f"EGP {transaction['amount_egp']} | "
                      f"{'FRAUD' if transaction['is_fraud'] else 'OK'}")

            # Send roughly 2 transactions per second
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopping simulator...")
    finally:
        producer.flush()
        producer.close()
        print("Done.")

if __name__ == "__main__":
    main()