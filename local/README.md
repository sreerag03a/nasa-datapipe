# Local Emulation

Emulation of the data pipeline locally.

### Prerequisites

- Docker

### Manual Emulation

1. Install required python libraries

```
pip install -r requirements.txt
```

2. Create a .env file and put your NASA Near Earth Objects API KEY with the key : `NASA_API`.

Eg:

```
NASA_API = YOUR_API_KEY
```

3. Create a Postgres DB for storing the processed data.

```
docker run -d --name nasa-postgres -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -e POSTGRES_DB=nasa_db -p 5432:5432 -v postgres_data:/var/lib/PostgreSQL/data postgres:15
```

4. Run `data_ingest_transform.py` to fetch and save data to table and also locally. This could also be automated using a scheduler.

5. You can connect PowerBI to the Postgres db or the local files to analyze data.
