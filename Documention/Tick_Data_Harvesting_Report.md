# Tick Data Harvesting Report

Date: April 7, 2026
Project: Stock Predictor v1.1.0

## 1. Overview
This report summarizes the Tick Data Harvesting setup used in the project. The system collects and stores market tick data for both NSE and BSE segments, including futures, options, and stock instruments.

## 2. Objective
The objective of the Tick Data Harvesting process is to:
- Continuously collect exchange tick data.
- Organize data by market segment and date.
- Store harvested data for downstream analytics and prediction workflows.

## 3. Current Deployment Status
- The Tick Data Harvesting pipeline has been pushed and deployed on AWS EC2.
- The services are running in production-oriented mode through configured service scripts.

## 4. AWS Services Used
The implementation uses the following AWS services:
- EC2: Used to host and run the harvesting services and automation scripts.
- S3: Used for storage and backup of harvested tick data files.

## 5. How We Have Done It
We have implemented Tick Data Harvesting in the following way:
- Step 1: Prepared symbol master files for required NSE and BSE instruments (futures, options, and stocks).
- Step 2: Configured environment settings through JSON configuration files, including paths, runtime settings, and holiday calendars.
- Step 3: Implemented authentication helpers to establish secure broker/API sessions before harvesting starts.
- Step 4: Built dedicated harvester scripts for each segment (NSE FO, BSE FO, and BSE/NSE stock data) to collect live tick streams.
- Step 5: Persisted incoming tick data into date-wise CSV files under structured folders for easy downstream processing.
- Step 6: Deployed the harvesting codebase on AWS EC2 and executed it as managed background services for continuous running.
- Step 7: Integrated AWS S3 to store and back up harvested files so data remains available and durable.
- Step 8: Performed local dry-run and diagnostics scripts to validate data flow and service stability.

## 6. User Statement
I am the user/owner of this Tick Data Harvesting setup.

## 7. Notes
- The codebase contains separate harvesters for BSE and NSE data flows.
- Configuration files and symbol lists are maintained for segment-wise execution.
- Data is organized in date-based folders for easier retrieval and processing.

## 8. Conclusion
The Tick Data Harvesting system is active and operational, deployed on AWS EC2, and integrated with AWS S3 for data storage support. This setup provides a stable foundation for analytics and stock prediction workloads.
