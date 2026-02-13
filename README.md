# NYC Leading Causes of Death API

A simple Flask API built on the NYC "Leading Causes of Death" dataset.  
Supports listing records, filtering by column, pagination (limit/offset), retrieving a single record by id, and output in JSON or CSV. Remember to use the pretty-print button otherwise it would look a bit messy.


## Connecting to the API

This API runs locally. Access it at:

http://127.0.0.1:5000

Make sure the server is running before sending requests.


## How to Run

1. Create virtual environment

python -m venv venv

2. Activate environment

Mac/Linux:
source venv/bin/activate

3. Install requirements

pip install -r requirements.txt

4. Run the server

python project.py

---

### 1. Welcome

- Method: GET
- Path: /
- Query parameters: None

Returns a welcome message.

Example:
GET /

Response:
"NYC Death Causes API is running."

---

### 2. List Records

- Method: GET
- Path: /records
- Query parameters:
  - format (optional): json or csv (default: json)
  - limit (optional): number of results to return
  - offset (optional): starting index for pagination
  - filters (optional): any column name in the dataset, e.g. Year=2021, Sex=Female

Example query:
http://127.0.0.1:5000/records?Sex=Female&Year=2021&limit=5&offset=0&format=json

Sample output (JSON):
[
  {
    "Year": 2021,
    "Leading Cause": "Diseases of Heart (I00-I09, I11, I13, I20-I51)",
    "Sex": "Female",
    "Race Ethnicity": "Hispanic",
    "Deaths": "1409",
    "Death Rate": "111",
    "Age Adjusted Death Rate": null,
    "id": 0
  }
]

### Query Parameters

You can filter by any column name in the dataset.

Examples:

- /records?Sex=Female
- /records?Year=2021
- /records?Race Ethnicity=Hispanic

---

### Pagination

- limit: number of records to return
- offset: starting index

Examples:

- /records?limit=5
- /records?limit=5&offset=10
- /records?Sex=Female&limit=3

---

### Output Format

- format=json (default)
- format=csv

Example:

- /records?limit=5&format=csv

---

### 3. Retrieve Single Record

- Method: GET
- Path: /records/<id>

Example:

- /records/10

Returns one record in JSON format.

If not found:

{
  "error": "Record not found"
}

