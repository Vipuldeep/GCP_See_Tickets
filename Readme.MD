
# Google Cloud Functions for See Tickets API Integration

## Overview

This project is designed to fetch and store data from the See Tickets API into Google BigQuery using Google Cloud Functions. It includes two primary Cloud Functions: one for retrieving sales data and another for event data. The project leverages Google Cloud Run, Google Secret Manager, and BigQuery for seamless integration and data storage.

## Components
- Cloud Functions
see_tickets_to_bigquery: Fetches sales data from the See Tickets API and inserts new records into a BigQuery table.
events: Fetches event data from the See Tickets API and inserts new records into a BigQuery table.
- Google Cloud Services
Google Cloud Run: Manages and runs the Cloud Functions in a serverless environment.
Google Cloud Secret Manager: Securely stores and manages the API key for accessing the See Tickets API.
Google BigQuery: Serves as the data warehouse where the fetched sales and event data are stored.
Cloud Function: see_tickets_to_bigquery
Functionality
This function performs the following steps:

Initializes the BigQuery client and Secret Manager client.
Retrieves the API key for the See Tickets API from Google Secret Manager.
Fetches the current unique codes from the BigQuery table to avoid inserting duplicate records.
Sends a POST request to the See Tickets API to fetch sales data.
Parses the API response and prepares the data for insertion into BigQuery.
Inserts the new sales data into the BigQuery table.
BigQuery Table Schema
The data is inserted into a table with the following structure:

- affiliate: STRING
- currency: STRING
- date: STRING
- deliveryMethod: STRING
- deviceModel: STRING
- deviceType: STRING
- eventDate: STRING
- eventId: STRING
- eventName: STRING
- faceValue: FLOAT
- grossCost: FLOAT
- grossFace: FLOAT
- grossSales: FLOAT
- isInternetOrder: BOOLEAN
- isPending: BOOLEAN
- country: STRING
- countryIso: STRING
- postCode: STRING
- region1: STRING
- region2: STRING
- region3: STRING
- region4: STRING
- offerCode: STRING
- partnerSite: STRING
- paymentMethod: STRING
- priceId: STRING
- priceName: STRING
- salesChannel: STRING
- sold: INTEGER
- source: STRING
- taxTotal: FLOAT
- ticketPrice: FLOAT
- uniqueCode: STRING (Primary Key)

### Error Handling
The function includes error handling for the following scenarios:

- Errors in accessing the API key from Google Secret Manager.
- Errors in fetching existing records from BigQuery.
- Errors in sending the API request or processing the API response.
- Errors in inserting data into BigQuery.
- Cloud Function: events
- Functionality

### This function performs the following steps:

Initializes the BigQuery client and Secret Manager client.
Retrieves the API key for the See Tickets API from Google Secret Manager.
Fetches the current event IDs from the BigQuery table to avoid inserting duplicate records.
Sends a GET request to the See Tickets API to fetch event data.
Parses the API response and prepares the data for insertion into BigQuery.
Inserts the new event data into the BigQuery table.
BigQuery Table Schema
The data is inserted into a table with the following structure:

- doorsOpen: STRING
- id: STRING (Primary Key)
- name: STRING
- promoterName: STRING
- starts: STRING
- timeslotEnabled: BOOLEAN
- tourId: STRING
- tourName: STRING
- venueName: STRING
- Error Handling

### The function includes error handling for the following scenarios:

- Errors in accessing the API key from Google Secret Manager.
- Errors in fetching existing records from BigQuery.
- Errors in sending the API request or processing the API response.
- Errors in inserting data into BigQuery.
- Deployment
- Prerequisites
- Google Cloud SDK installed and authenticated.
- A Google Cloud Project with BigQuery, Secret Manager, and Cloud Run enabled.
- A service account with the necessary permissions to access - - BigQuery and Secret Manager.

### Security Considerations
- API Key Management: The API key for accessing the See Tickets API is securely stored in Google Secret Manager.
- Access Control: Ensure that the service account used by your Cloud Functions has the least privilege necessary to access BigQuery and Secret Manager.
