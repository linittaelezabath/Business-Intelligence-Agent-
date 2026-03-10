## Business Intelligence Agent

A Natural Language Business Intelligence system that connects to Monday.com, fetches operational data (Work Orders, Deals, etc.), and allows leadership to ask plain-English questions to generate insights.
Unlike many BI tools, this system uses a pure NLP pipeline (no external AI APIs) to interpret questions and convert them into data queries executed with pandas.
This enables founders, managers, and operations teams to quickly get answers like:
"What is the revenue this month?"
"Show deals in the healthcare sector."
"How many work orders closed last week?"
"Generate a leadership update."

## Live Demo URL
https://bma2xfjtdprxiozg4bbffb.streamlit.app/


 ## Features

✔ Connects directly to Monday.com GraphQL API
✔ Automatically discovers boards and items
✔ Cleans and normalizes messy operational data
✔ Supports natural language queries
✔ Generates leadership-level summaries
✔ Fully local NLP pipeline (no OpenAI / external APIs)
✔ Easy to extend with new business metrics

## Architecture Overview
The system follows a modular Business Intelligence pipelin

                   +----------------------+
                   |  Monday.com API      |
                   |  (GraphQL Endpoint)  |
                   +----------+-----------+
                              |
                              v
                 +-------------------------+
                 |   Data Fetcher Module   |
                 |  - Board discovery      |
                 |  - Item extraction      |
                 +-----------+-------------+
                             |
                             v
                +---------------------------+
                | Data Cleaning & Normalisation |
                | - Date parsing               |
                | - Currency conversion       |
                | - Status normalization      |
                +-------------+--------------+
                              |
                              v
                  +------------------------+
                  |  Data Store (Pandas)   |
                  |  Structured DataFrame  |
                  +-----------+------------+
                              |
                              v
                    +---------------------+
                    |  NLP BI Agent       |
                    |                     |
                    | Tokenization        |
                    | Intent Detection    |
                    | Entity Extraction   |
                    | Query Dispatcher    |
                    +-----------+---------+
                                |
                                v
                     +------------------+
                     |  Insight Engine  |
                     |  - KPI metrics   |
                     |  - summaries     |
                     |  - leadership    |
                     |    reports       |
                     +------------------
                     
                     
## NLP Query Pipeline

The BI agent interprets natural language using a deterministic NLP pipeline:

1️⃣ Tokenisation

-Converts input to lowercase

-Removes punctuation

-Filters stopwords

Example:

"What is the revenue this month?"
→ ["revenue", "month"]
2️⃣ Intent Classification

Intent is determined using keyword scoring across predefined categories:

Example intents:

-Revenue analysis

-Deal pipeline

-Sector breakdown

-Time-period summaries

-Work order metrics

-Leadership reports

3️⃣ Entity Extraction

Regex patterns identify important parameters:

-Entity	Example
-Time Period	this month, last week
-Sector	healthcare, energy
-Deal Stage	won, lost
-Metric	revenue, deals
4️⃣ Query Execution

The parsed intent is translated into pandas operations such as:

df.groupby()
df.sum()
df.filter()
df.date_range()

The result is returned as:

KPI numbers

tables

summaries

leadership updates

##  Project Structure
business-intelligence-agent/
│
├── Business_Intelligence_Agent.ipynb
│
├── README.md
│
└── requirements.txt

Notebook Sections:

Setup & Dependencies

### Monday.com Configuration

Data Fetcher

Data Cleaning

Exploratory Analysis

NLP BI Agent

Leadership Update Generator

Utility Functions

⚙️ Setup Instructions
1️⃣ Clone the Repository
git clone https://github.com/yourusername/business-intelligence-agent.git
cd business-intelligence-agent
2️⃣ Install Dependencies
pip install requests pandas python-dateutil

Optional:

pip install jupyter
3️⃣ Launch the Notebook
jupyter notebook

Open:

Business_Intelligence_Agent.ipynb

Run cells sequentially.

🔑 Monday.com Configuration
Step 1 — Generate API Token

Log into Monday.com

Click Profile → Admin → API

Generate an API Token

Documentation:
https://developer.monday.com/api-reference/docs

Step 2 — Add Token to Notebook

Inside the Configuration section, set:

MONDAY_API_TOKEN = "your_api_token_here"

⚠️ Never commit real tokens to GitHub.

Recommended approach:

import os

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

Then set environment variable:

Windows
set MONDAY_API_TOKEN=your_token_here
Linux / Mac
export MONDAY_API_TOKEN=your_token_here
## Data Retrieval

The system queries Monday.com using GraphQL.

Example request:

{
  boards {
    id
    name
    items {
      name
      column_values {
        text
      }
    }
  }
}

The system automatically:

discovers boards

extracts items

converts column values

builds a structured dataset

##  Example Queries

You can ask questions like:

"What is total revenue this month?"
"Show deals in healthcare sector"
"How many deals were won last quarter?"
"Which sector has the highest revenue?"
"Generate leadership update"

Example Output:

Total Revenue (This Month): $240,000

Top Sector:
Healthcare – $110,000

Deals Won:
18
## Leadership Update Generator

The system can automatically generate a weekly leadership summary:

Example output:

Weekly Business Update

Revenue This Week: $85,000
Deals Closed: 7
Top Sector: Energy
Open Opportunities: 22

Recommendation:
Focus sales efforts on Healthcare and Energy sectors.

This is useful for:

board meetings

founder updates

investor reports

 ## Security Notes

Do NOT commit:

API tokens
customer data
private Monday boards

Use:

.env files
environment variables
secret managers

 ## Future Improvements

Possible extensions:

Streamlit dashboard

Slack / Teams chatbot

Real-time BI dashboards

ML forecasting

anomaly detection

automated KPI alerts


🧑‍💻 Author

author @linittaelezabath
