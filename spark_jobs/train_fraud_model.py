import pandas as pd
import numpy as np
import random
import joblib
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder

# Step 1: Domain data
SERVICE_TYPES = [
    'electricity_bill', 'water_bill', 'gas_bill', 'mobile_topup',
    'internet_bill', 'school_fees', 'university_fees', 'traffic_fines',
    'telephone_bill', 'credit_card_payment', 'insurance_premium',
    'pos_purchase', 'ecommerce_payment', 'government_fees', 'donations'
]

GOVERNORATES = [
    'Cairo', 'Giza', 'Alexandria', 'Qalyubia', 'Sharqia',
    'Dakahlia', 'Beheira', 'Minya', 'Assiut', 'Sohag',
    'Luxor', 'Aswan', 'Port Said', 'Suez', 'Ismailia'
]

PAYMENT_METHODS = [
    'wallet', 'debit_card', 'credit_card', 'bank_transfer', 'cash_at_agent'
]

AMOUNT_RANGES = {
    'electricity_bill':    (50,   800),
    'water_bill':          (20,   300),
    'gas_bill':            (30,   400),
    'mobile_topup':        (10,   500),
    'internet_bill':       (100,  600),
    'school_fees':         (500,  5000),
    'university_fees':     (2000, 20000),
    'traffic_fines':       (100,  1000),
    'telephone_bill':      (50,   400),
    'credit_card_payment': (200,  30000),
    'insurance_premium':   (300,  5000),
    'pos_purchase':        (20,   3000),
    'ecommerce_payment':   (50,   5000),
    'government_fees':     (50,   2000),
    'donations':           (10,   1000),
}

# Step 2: Generate synthetic training data
def generate_dataset(n_samples=50000, fraud_rate=0.03):
    records = []
    for _ in range(n_samples):
        service  = random.choice(SERVICE_TYPES)
        low, high = AMOUNT_RANGES[service]
        is_fraud = random.random() < fraud_rate

        if is_fraud:
            amount = round(random.uniform(15000, 50000), 2)
            hour   = random.randint(1, 4)
        else:
            amount = round(random.uniform(low, high), 2)
            hour   = random.randint(6, 23)

        records.append({
            'amount_egp':     amount,
            'service_type':   service,
            'governorate':    random.choice(GOVERNORATES),
            'payment_method': random.choice(PAYMENT_METHODS),
            'hour_of_day':    hour,
            'is_fraud':       int(is_fraud),
        })
    return pd.DataFrame(records)

print("Generating dataset")
df = generate_dataset(n_samples=50000)
print(f"Dataset shape: {df.shape}")
print(f"Fraud rate:    {df['is_fraud'].mean():.2%}")

# Step 3: Feature engineering
le_service = LabelEncoder()
le_gov     = LabelEncoder()
le_payment = LabelEncoder()

df['service_enc'] = le_service.fit_transform(df['service_type'])
df['gov_enc']     = le_gov.fit_transform(df['governorate'])
df['payment_enc'] = le_payment.fit_transform(df['payment_method'])

df['amount_pct'] = df.groupby('service_type')['amount_egp'] \
                     .transform(lambda x: x.rank(pct=True))

df['odd_hour'] = df['hour_of_day'].apply(lambda h: 1 if h <= 4 else 0)

FEATURES = [
    'amount_egp', 'service_enc', 'gov_enc',
    'payment_enc', 'hour_of_day', 'amount_pct', 'odd_hour'
]

X = df[FEATURES]
y = df['is_fraud']

print("Features ready:", FEATURES)

# Step 4: Train/test split and model training
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print("Training Random Forest model")
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    class_weight='balanced',
    random_state=42,
    n_jobs=-1,
)
model.fit(X_train, y_train)
print("Model trained successfully")

# Step 5: Evaluate
y_pred  = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Legit', 'Fraud']))
print(f"ROC-AUC Score: {roc_auc_score(y_test, y_proba):.4f}")
print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

# Step 6: Save model and encoders to spark_jobs/
output_dir = os.path.join(os.path.dirname(__file__), '..', 'spark_jobs')
output_dir = os.path.abspath(output_dir)
os.makedirs(output_dir, exist_ok=True)

joblib.dump(model,      os.path.join(output_dir, 'fraud_model.pkl'))
joblib.dump(le_service, os.path.join(output_dir, 'le_service.pkl'))
joblib.dump(le_gov,     os.path.join(output_dir, 'le_gov.pkl'))
joblib.dump(le_payment, os.path.join(output_dir, 'le_payment.pkl'))

print(f"\nModel and encoders saved to: {output_dir}")
print("Files saved:")
for f in ['fraud_model.pkl', 'le_service.pkl', 'le_gov.pkl', 'le_payment.pkl']:
    full_path = os.path.join(output_dir, f)
    size_kb = os.path.getsize(full_path) / 1024
    print(f"  {f} ({size_kb:.1f} KB)")