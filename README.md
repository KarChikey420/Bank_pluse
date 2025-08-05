
# BankPulse \

This project simulates real-time detection of customer transaction patterns for banks using AWS S3, Python, and PostgreSQL.

##  Project Structure

```
bankpulse/
├── mechanism_x.py          # Uploads 10K transaction chunks to S3 every second
├── mechanism_y.py          # Listens to S3, detects patterns, and stores detections
├── s3_connect.py           # S3 client connection setup             # AWS & DB credentials
├── transactions.csv        # Full transactions dataset (input)
├── CustomerImportance.csv  # Customer importance weights (input)
├── requirements.txt
└── README.md
```

##  Detection Patterns

| Pattern ID | Description |
|------------|-------------|
| **PatId1** | UPGRADE – Top 10% txn customers for a merchant with bottom 10% weight, only if merchant has >50K txns |
| **PatId2** | CHILD – Avg txn < ₹23 and ≥80 txns for a merchant |
| **PatId3** | DEI-NEEDED – Merchant has more male than female customers and >100 females overall |

##  Setup Instructions

1. **Clone the repository**

   ```bash
   git clone <https://github.com/KarChikey420/Bank_pluse>
   cd bankpulse
   ```

2. **Create virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure credentials**

   Create a `.env` file or fill `config.env` with:

   ```env
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_REGION=ap-south-1
   S3_BUCKET=your_bucket_name

   DB_NAME=bankpulse
   DB_USER=postgres
   DB_PASSWORD=your_db_password
   DB_HOST=localhost
   DB_PORT=5432
   ```

5. **Run Mechanisms**

   - Start **Mechanism X** to push chunks:

     ```bash
     python mechanism_x.py
     ```

   - Start **Mechanism Y** in parallel to detect patterns:

     ```bash
     python mechanism_y.py
     ```

## Temp Storage

- Uses **PostgreSQL** to maintain intermediate state and avoid duplicate detections.

## ☁️ S3 Usage

- Transaction chunks from `transactions.csv` are uploaded to S3 every second.
- Detections are written back to S3 in batches of 50 per file.

## Requirements

- Python 3.8+
- AWS credentials (for S3 access)
- PostgreSQL running locally or remotely

## License

This project is for educational and demonstration purposes.