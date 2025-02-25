# Airbyte Postgres Connector Setup

## Table of Contents
- [Introduction](#introduction)
- [Setting Up Airbyte](#setting-up-airbyte)
- [Configuring PostgreSQL as a Source](#configuring-postgresql-as-a-source)
- [Advanced Configuration (CDC)](#advanced-configuration-cdc)


## Introduction
This guide provides a step-by-step walkthrough to install Airbyte and configure PostgreSQL as a source for data replication using the Airbyte Postgres Connector.

## Running a postgres container

```sql
docker run --name postgres-container -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=mydatabase -p 5432:5432 -d postgres
```
psql -U user -d mydatabase

docker exec -it postgres_cdc psql -U airbyte_use
createing a table named student
```sql
CREATE TABLE student (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
```

## Setting Up Airbyte
1. Open the Airbyte UI (`http://localhost:8000`).
2. Navigate to **Sources** and click **Create New Source**.
3. Search for **Postgres** and select the **Postgres Connector**.

## Configuring PostgreSQL as a Source
### Step 1: Create a Read-Only PostgreSQL User
Run the following SQL commands on your PostgreSQL database:
```sql
CREATE USER user1 PASSWORD 'password1';
GRANT USAGE ON SCHEMA public TO user1;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO user1;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO user1;
```

### Step 2: Configure the Airbyte UI
- Enter the **hostname**, **port (5432 by default)**, **database name**, **username**, and **password**.
If you are using docker the hostname will be host.docker.internal
- Select the schema(s) to sync (default: `public`).
- Choose an **SSL mode** (recommended: `require` or `verify-ca`).
- Select **Standard (xmin)** as the replication method.

## Advanced Configuration (CDC)
If your database is large (>500GB) or you need a record of deletions, use **Change Data Capture (CDC)**.

### Step 1: Enable Logical Replication
#### For Self-Hosted PostgreSQL:
Edit `postgresql.conf` and set:
```ini
wal_level = logical
max_wal_senders = 1
max_replication_slots = 1
```
Restart PostgreSQL.


### Step 2: Grant Replication Permissions
```sql
ALTER USER airbyte_user REPLICATION;
```

### Step 3: Create Replication Slot & Publication
```sql
SELECT pg_create_logical_replication_slot('airbyte_slot', 'pgoutput');
CREATE PUBLICATION airbyte_publication FOR TABLE my_table;
ALTER TABLE my_table REPLICA IDENTITY DEFAULT;
```


