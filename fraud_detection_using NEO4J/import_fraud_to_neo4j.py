"""
FRAUD DETECTION DATA IMPORT TO NEO4J AURA
==========================================
Simple script to import all fraud detection CSV files into Neo4j Aura

CSV Files to Import:
- banks.csv (3 rows)
- clients.csv (100 rows)
- ssns.csv (33 rows)
- phones.csv (33 rows)
- emails.csv (33 rows)
- merchants.csv (10 rows)
- mules.csv (6 rows)
- transactions.csv (4,849 rows)
- client_has_ssn.csv (51 rows)
- client_has_phone.csv (100 rows)
- client_has_email.csv (100 rows)
"""

import os
from dotenv import load_dotenv
import pandas as pd
from neo4j import GraphDatabase
from datetime import datetime

print("=" * 80)
print("FRAUD DETECTION - NEO4J AURA IMPORT")
print("=" * 80)

# ============================================================================
# STEP 1: LOAD ENVIRONMENT VARIABLES
# ============================================================================

print("\n[STEP 1] Loading environment variables...")
load_dotenv()

NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USERNAME = os.getenv('NEO4J_USERNAME', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

if not all([NEO4J_URI, NEO4J_PASSWORD]):
    raise ValueError("ERROR: Missing environment variables! Check .env file")

print(f"OK! NEO4J_URI: {NEO4J_URI}")
print(f"OK! NEO4J_USERNAME: {NEO4J_USERNAME}")
print(f"OK! NEO4J_PASSWORD: {'*' * 8}")

# ============================================================================
# STEP 2: CONNECT TO NEO4J
# ============================================================================

print("\n[STEP 2] Connecting to Neo4j Aura...")
try:
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )
    
    with driver.session() as session:
        result = session.run("RETURN 1 AS test")
        result.single()
    print("OK! Connected successfully!")
    
except Exception as e:
    print(f"✗ Connection failed: {str(e)}")
    print("\nTroubleshooting:")
    print("  1. Check database is running in Neo4j Aura console")
    print("  2. Verify password in .env file")
    print("  3. Ensure URI starts with 'neo4j+s://'")
    exit(1)

# ============================================================================
# STEP 3: CHECK EXISTING DATA
# ============================================================================

print("\n[STEP 3] Checking existing data...")
with driver.session() as session:
    result = session.run("MATCH (n) RETURN count(n) AS count")
    node_count = result.single()['count']
    
    if node_count > 0:
        print(f"⚠ WARNING: Database has {node_count:,} nodes!")
        response = input("Delete ALL data and start fresh? (yes/no): ")
        
        if response.lower() == 'yes':
            print("\n[DELETING] Clearing database...")
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
            print("OK! Database cleared!")
        else:
            print("✗ Import cancelled to avoid duplicates")
            print("  Tip: Delete existing data first or use MERGE instead of CREATE")
            driver.close()
            exit(0)
    else:
        print("OK! Database is empty - ready for import")

# ============================================================================
# STEP 4: CREATE CONSTRAINTS
# ============================================================================

print("\n[STEP 4] Creating constraints...")

constraints = [
    ("Client", "id"),
    ("SSN", "id"),
    ("Phone", "id"),
    ("Email", "id"),
    ("Merchant", "id"),
    ("Bank", "id"),
    ("Transaction", "id"),
    ("Mule", "id")
]

with driver.session() as session:
    for label, property in constraints:
        constraint_name = f"{label.lower()}_{property}_unique"
        query = f"""
        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
        FOR (n:{label}) REQUIRE n.{property} IS UNIQUE
        """
        try:
            session.run(query)
            print(f"  OK! {label}.{property} unique constraint")
        except Exception as e:
            print(f"  ⚠ {label}.{property} constraint: {str(e)[:50]}")

# ============================================================================
# STEP 5: LOAD CSV FILES
# ============================================================================

print("\n[STEP 5] Loading CSV files...")

csv_files = {
    'banks': 'banks.csv',
    'clients': 'clients.csv',
    'ssns': 'ssns.csv',
    'phones': 'phones.csv',
    'emails': 'emails.csv',
    'merchants': 'merchants.csv',
    'mules': 'mules.csv',
    'transactions': 'transactions.csv',
    'client_has_ssn': 'client_has_ssn.csv',
    'client_has_phone': 'client_has_phone.csv',
    'client_has_email': 'client_has_email.csv'
}

data = {}
for name, filename in csv_files.items():
    if not os.path.exists(filename):
        print(f"ERROR: {filename} not found!")
        print(f"  Current directory: {os.getcwd()}")
        driver.close()
        exit(1)
    
    data[name] = pd.read_csv(filename)
    print(f"  OK! {filename}: {len(data[name]):,} rows")

print("OK! All CSV files loaded successfully!")

# ============================================================================
# STEP 6: IMPORT BANKS
# ============================================================================

print("\n[STEP 6] Importing Banks...")

def create_banks(tx, banks_df):
    query = """
    UNWIND $banks AS bank
    CREATE (b:Bank {
        id: bank.bank_id,
        name: bank.name,
        routingNumber: bank.routing_number
    })
    """
    banks_list = banks_df.to_dict('records')
    tx.run(query, banks=banks_list)

with driver.session() as session:
    session.execute_write(create_banks, data['banks'])
    print(f"OK! Created {len(data['banks'])} Bank nodes")

# ============================================================================
# STEP 7: IMPORT CLIENTS
# ============================================================================

print("\n[STEP 7] Importing Clients...")

def create_clients(tx, clients_df):
    query = """
    UNWIND $clients AS client
    CREATE (c:Client {
        id: client.client_id,
        name: client.name,
        accountNumber: client.account_number,
        bankId: client.bank_id,
        registrationDate: date(client.registration_date)
    })
    """
    clients_list = clients_df.to_dict('records')
    tx.run(query, clients=clients_list)

with driver.session() as session:
    session.execute_write(create_clients, data['clients'])
    print(f"OK! Created {len(data['clients'])} Client nodes")

# ============================================================================
# STEP 8: IMPORT SSNs
# ============================================================================

print("\n[STEP 8] Importing SSNs...")

def create_ssns(tx, ssns_df):
    query = """
    UNWIND $ssns AS ssn
    CREATE (s:SSN {
        id: ssn.ssn_id,
        value: ssn.ssn_value
    })
    """
    ssns_list = ssns_df.to_dict('records')
    tx.run(query, ssns=ssns_list)

with driver.session() as session:
    session.execute_write(create_ssns, data['ssns'])
    print(f"OK! Created {len(data['ssns'])} SSN nodes")

# ============================================================================
# STEP 9: IMPORT PHONES
# ============================================================================

print("\n[STEP 9] Importing Phones...")

def create_phones(tx, phones_df):
    query = """
    UNWIND $phones AS phone
    CREATE (p:Phone {
        id: phone.phone_id,
        number: phone.phone_number
    })
    """
    phones_list = phones_df.to_dict('records')
    tx.run(query, phones=phones_list)

with driver.session() as session:
    session.execute_write(create_phones, data['phones'])
    print(f"OK! Created {len(data['phones'])} Phone nodes")

# ============================================================================
# STEP 10: IMPORT EMAILS
# ============================================================================

print("\n[STEP 10] Importing Emails...")

def create_emails(tx, emails_df):
    query = """
    UNWIND $emails AS email
    CREATE (e:Email {
        id: email.email_id,
        address: email.email_address
    })
    """
    emails_list = emails_df.to_dict('records')
    tx.run(query, emails=emails_list)

with driver.session() as session:
    session.execute_write(create_emails, data['emails'])
    print(f"OK! Created {len(data['emails'])} Email nodes")

# ============================================================================
# STEP 11: IMPORT MERCHANTS
# ============================================================================

print("\n[STEP 11] Importing Merchants...")

def create_merchants(tx, merchants_df):
    query = """
    UNWIND $merchants AS merchant
    CREATE (m:Merchant {
        id: merchant.merchant_id,
        name: merchant.name,
        category: merchant.category
    })
    """
    merchants_list = merchants_df.to_dict('records')
    tx.run(query, merchants=merchants_list)

with driver.session() as session:
    session.execute_write(create_merchants, data['merchants'])
    print(f"OK! Created {len(data['merchants'])} Merchant nodes")

# ============================================================================
# STEP 12: IMPORT MULES
# ============================================================================

print("\n[STEP 12] Importing Mules (flagged accounts)...")

def create_mules(tx, mules_df):
    query = """
    UNWIND $mules AS mule
    CREATE (mu:Mule {
        id: mule.mule_id,
        clientId: mule.client_id,
        flaggedDate: date(mule.flagged_date),
        riskScore: toFloat(mule.risk_score)
    })
    """
    mules_list = mules_df.to_dict('records')
    tx.run(query, mules=mules_list)

with driver.session() as session:
    session.execute_write(create_mules, data['mules'])
    print(f"OK! Created {len(data['mules'])} Mule nodes")

# ============================================================================
# STEP 13: IMPORT TRANSACTIONS (Large Dataset)
# ============================================================================

print("\n[STEP 13] Importing Transactions...")
print("  This may take 30-60 seconds for 4,849 transactions...")

def create_transactions_batch(tx, transactions_batch):
    query = """
    UNWIND $transactions AS trans
    CREATE (t:Transaction {
        id: trans.transaction_id,
        type: trans.type,
        amount: toFloat(trans.amount),
        timestamp: datetime(replace(trans.timestamp, ' ', 'T')),
        clientId: trans.client_id,
        destination: trans.destination,
        status: trans.status
    })
    """
    tx.run(query, transactions=transactions_batch)

# Process in batches of 500
batch_size = 500
transactions_list = data['transactions'].to_dict('records')
num_batches = (len(transactions_list) + batch_size - 1) // batch_size

with driver.session() as session:
    for i in range(0, len(transactions_list), batch_size):
        batch = transactions_list[i:i+batch_size]
        session.execute_write(create_transactions_batch, batch)
        
        batch_num = (i // batch_size) + 1
        completed = min(i + batch_size, len(transactions_list))
        print(f"  Progress: {completed:,}/{len(transactions_list):,} "
              f"({batch_num}/{num_batches} batches)")

print(f"OK! Created {len(transactions_list):,} Transaction nodes")

# ============================================================================
# STEP 14: CREATE RELATIONSHIPS - HAS_SSN
# ============================================================================

print("\n[STEP 14] Creating HAS_SSN relationships...")

def create_has_ssn(tx, relationships_df):
    query = """
    UNWIND $rels AS rel
    MATCH (c:Client {id: rel.client_id})
    MATCH (s:SSN {id: rel.ssn_id})
    MERGE (c)-[:HAS_SSN]->(s)
    """
    rels_list = relationships_df.to_dict('records')
    tx.run(query, rels=rels_list)

with driver.session() as session:
    session.execute_write(create_has_ssn, data['client_has_ssn'])
    print(f"OK! Created {len(data['client_has_ssn'])} HAS_SSN relationships")

# ============================================================================
# STEP 15: CREATE RELATIONSHIPS - HAS_PHONE
# ============================================================================

print("\n[STEP 15] Creating HAS_PHONE relationships...")

def create_has_phone(tx, relationships_df):
    query = """
    UNWIND $rels AS rel
    MATCH (c:Client {id: rel.client_id})
    MATCH (p:Phone {id: rel.phone_id})
    MERGE (c)-[:HAS_PHONE]->(p)
    """
    rels_list = relationships_df.to_dict('records')
    tx.run(query, rels=rels_list)

with driver.session() as session:
    session.execute_write(create_has_phone, data['client_has_phone'])
    print(f"OK! Created {len(data['client_has_phone'])} HAS_PHONE relationships")

# ============================================================================
# STEP 16: CREATE RELATIONSHIPS - HAS_EMAIL
# ============================================================================

print("\n[STEP 16] Creating HAS_EMAIL relationships...")

def create_has_email(tx, relationships_df):
    query = """
    UNWIND $rels AS rel
    MATCH (c:Client {id: rel.client_id})
    MATCH (e:Email {id: rel.email_id})
    MERGE (c)-[:HAS_EMAIL]->(e)
    """
    rels_list = relationships_df.to_dict('records')
    tx.run(query, rels=rels_list)

with driver.session() as session:
    session.execute_write(create_has_email, data['client_has_email'])
    print(f"OK! Created {len(data['client_has_email'])} HAS_EMAIL relationships")

# ============================================================================
# STEP 17: CREATE RELATIONSHIPS - PERFORMED (Client → Transaction)
# ============================================================================

print("\n[STEP 17] Creating PERFORMED relationships...")
print("  Linking transactions to clients...")

def create_performed(tx):
    query = """
    MATCH (t:Transaction)
    MATCH (c:Client {id: t.clientId})
    MERGE (c)-[:PERFORMED]->(t)
    """
    tx.run(query)

with driver.session() as session:
    session.execute_write(create_performed)
    
    # Count created relationships
    result = session.run("MATCH ()-[r:PERFORMED]->() RETURN count(r) AS count")
    count = result.single()['count']
    print(f"OK! Created {count:,} PERFORMED relationships")

# ============================================================================
# STEP 18: CREATE RELATIONSHIPS - FLAGGED_AS (Client → Mule)
# ============================================================================

print("\n[STEP 18] Creating FLAGGED_AS relationships...")

def create_flagged_as(tx):
    query = """
    MATCH (mu:Mule)
    MATCH (c:Client {id: mu.clientId})
    MERGE (c)-[:FLAGGED_AS]->(mu)
    """
    tx.run(query)

with driver.session() as session:
    session.execute_write(create_flagged_as)
    
    # Count created relationships
    result = session.run("MATCH ()-[r:FLAGGED_AS]->() RETURN count(r) AS count")
    count = result.single()['count']
    print(f"OK! Created {count} FLAGGED_AS relationships")

# ============================================================================
# STEP 19: VERIFY IMPORT
# ============================================================================

print("\n[STEP 19] Verifying import...")

with driver.session() as session:
    # Count nodes
    result = session.run("""
        MATCH (n)
        RETURN labels(n)[0] AS NodeType, count(n) AS Count
        ORDER BY Count DESC
    """)
    
    print("\n  Node Counts:")
    total_nodes = 0
    for record in result:
        count = record['Count']
        total_nodes += count
        print(f"    {record['NodeType']:<15} {count:>5,}")
    print(f"    {'TOTAL':<15} {total_nodes:>5,}")
    
    # Count relationships
    result = session.run("""
        MATCH ()-[r]->()
        RETURN type(r) AS RelType, count(r) AS Count
        ORDER BY Count DESC
    """)
    
    print("\n  Relationship Counts:")
    total_rels = 0
    for record in result:
        count = record['Count']
        total_rels += count
        print(f"    {record['RelType']:<20} {count:>5,}")
    print(f"    {'TOTAL':<20} {total_rels:>5,}")

# ============================================================================
# STEP 20: CREATE FRAUD ANALYSIS PROPERTIES
# ============================================================================

print("\n[STEP 20] Calculating node degrees...")

with driver.session() as session:
    query = """
    MATCH (n)
    WITH n, 
         COUNT { (n)-->() } as out_degree, 
         COUNT { (n)<--() } as in_degree
    SET n.out_degree = out_degree, n.in_degree = in_degree
    RETURN count(n) as nodes_updated
    """
    result = session.run(query)
    nodes_updated = result.single()['nodes_updated']
    print(f"OK! Updated {nodes_updated:,} nodes with degree properties")

# ============================================================================
# STEP 21: CREATE SHARED_IDENTIFIERS RELATIONSHIPS
# ============================================================================

print("\n[STEP 21] Creating SHARED_IDENTIFIERS fraud network...")
print("  This identifies clients sharing SSN/Phone/Email...")

with driver.session() as session:
    query = """
    MATCH (c1:Client)-[r:HAS_EMAIL|HAS_PHONE|HAS_SSN]->(n)
          <-[r2:HAS_EMAIL|HAS_PHONE|HAS_SSN]-(c2:Client)
    WHERE id(c1) < id(c2)
    WITH c1, c2, count(*) as cnt,
         SUM(
           CASE WHEN type(r) = 'HAS_EMAIL' THEN 1.0
                WHEN type(r) = 'HAS_PHONE' THEN 1.5
                WHEN type(r) = 'HAS_SSN' THEN 5.0
                ELSE 0
           END
         ) AS weight
    MERGE (c1)-[:SHARED_IDENTIFIERS {count: cnt, weight: weight}]->(c2)
    """
    result = session.run(query)
    summary = result.consume()
    print(f"OK! Created {summary.counters.relationships_created} fraud connections")

# ============================================================================
# STEP 22: FINAL VERIFICATION
# ============================================================================

print("\n[STEP 22] Final fraud detection analysis...")

with driver.session() as session:
    # Find shared SSNs
    result = session.run("""
        MATCH (s:SSN)<-[:HAS_SSN]-(c:Client)
        WITH s, count(c) as client_count
        WHERE client_count > 1
        RETURN count(s) as shared_ssns, max(client_count) as max_clients
    """)
    record = result.single()
    print(f"\n  Shared SSNs (Identity Theft Indicator):")
    print(f"    SSNs shared by multiple clients: {record['shared_ssns']}")
    print(f"    Maximum clients per SSN: {record['max_clients']}")
    
    # Find fraud network size
    result = session.run("""
        MATCH ()-[r:SHARED_IDENTIFIERS]->()
        RETURN count(r) as fraud_connections
    """)
    fraud_conn = result.single()['fraud_connections']
    print(f"\n  Fraud Network:")
    print(f"    Total suspicious connections: {fraud_conn}")
    
    # Find largest fraud ring
    result = session.run("""
        MATCH (c:Client)-[:SHARED_IDENTIFIERS*]-(connected:Client)
        WITH c, collect(DISTINCT connected) + [c] as fraud_ring
        RETURN max(size(fraud_ring)) as largest_ring_size
    """)
    ring_size = result.single()['largest_ring_size']
    print(f"    Largest fraud ring: {ring_size} clients")

# Close connection
driver.close()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("IMPORT COMPLETE! OK!")
print("=" * 80)

print(f"""
Successfully imported fraud detection dataset:

NODES:
  OK! {len(data['clients']):,} Clients
  OK! {len(data['transactions']):,} Transactions
  OK! {len(data['ssns']):,} SSNs
  OK! {len(data['phones']):,} Phones
  OK! {len(data['emails']):,} Emails
  OK! {len(data['merchants']):,} Merchants
  OK! {len(data['mules']):,} Mules (flagged accounts)
  OK! {len(data['banks']):,} Banks

RELATIONSHIPS:
  OK! {len(data['client_has_ssn']):,} HAS_SSN
  OK! {len(data['client_has_phone']):,} HAS_PHONE
  OK! {len(data['client_has_email']):,} HAS_EMAIL
  OK! {total_rels:,} Total relationships (including PERFORMED, FLAGGED_AS, SHARED_IDENTIFIERS)

FRAUD INDICATORS:
  OK! {record['shared_ssns']} SSNs shared by multiple clients (identity theft!)
  OK! {fraud_conn} suspicious fraud connections detected
  OK! {ring_size} clients in largest fraud ring

NEXT STEPS:
1. Open Neo4j Browser and visualize fraud rings:
   MATCH (c:Client)-[r:SHARED_IDENTIFIERS]-(c2:Client)
   RETURN c, r, c2
   LIMIT 50

2. Find identity theft cases:
   MATCH (s:SSN)<-[:HAS_SSN]-(c:Client)
   WITH s, collect(c.name) as clients
   WHERE size(clients) > 1
   RETURN s.value, clients
   ORDER BY size(clients) DESC

3. Identify key fraudsters:
   MATCH (c:Client)
   WHERE exists((c)-[:SHARED_IDENTIFIERS]-())
   WITH c, size((c)-[:SHARED_IDENTIFIERS]-()) as connections
   RETURN c.name, connections
   ORDER BY connections DESC
   LIMIT 10

""")

print("=" * 80)
