
Project Title:	A Comprehensive ETL Workflow with Python for Data Engineers
Technologies	Python
Data Format	CSV , JSON , XML

Introduction:
The Extract, Transform, Load (ETL) process is essential for data engineers, enabling them to manage data from various formats and transform it for further use. In this project, we will demonstrate how to extract data from CSV, JSON, and XML formats, transform it, and load the transformed data into a structured format for further processing.

Objectives:
By the end of this project, you will be able to:
1.	Extract data from CSV, JSON, and XML files.
2.	Transform the extracted data into a desired format, including unit conversions.
3.	Load the transformed data into a CSV file for future use in databases.
4.	Log the progress of ETL operations for monitoring purposes.

Dataset :wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
Steps:
Step 1: Gather Data Files
1.	Open a terminal and download the dataset:
○	Use the wget command to download the dataset containing multiple file formats.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
2.	Unzip the downloaded file:
Expand-Archive -Path source.zip -DestinationPath ./unzipped_folder

Note :  You may use the unzip command to extract the contents of the downloaded zip file.
After completing this step, the project folder will have CSV, JSON, and XML files to work with.

Step 2: Import Libraries and Set Paths
You need to import necessary libraries like:
●	glob to handle file formats.
●	pandas to read CSV and JSON files.
●	xml.etree.ElementTree to parse XML data.
●	datetime to track the progress of each phase through logging.
Install the pandas library if it's not already installed.
Set up paths for:
●	log_file.txt to record the logs.
●	transformed_data.csv to save the final output.
________________________________________
Step 3:Define functions for each step of  ETL as follows:
1.	Extract Data:
○	Three different functions to extract data from CSV, JSON, and XML files respectively.
○	A master function will call the relevant function based on the file type and combine the extracted data into a single DataFrame.
2.	Transform Data:
○	The transformation process involves converting:
■	Heights from inches to meters.
■	Weights from pounds to kilograms.
○	This step ensures the data is in the desired format for further analysis or storage.
3.	Load Data:
○	The transformed data is saved to a CSV file, which can later be loaded into a relational database or used for further processing.
4.	Logging:
○	Throughout the ETL process, each phase (Extraction, Transformation, Loading) is logged with a timestamp to ensure traceability and monitoring.
○	Logs are saved in a text file for auditing or troubleshooting purposes.
________________________________________
Step 4: ETL Execution
The ETL process follows this sequence:
1.	Extraction Phase:
○	The project extracts data from all CSV, JSON, and XML files located in the project directory.
○	Each file type is processed, and the results are combined into one DataFrame.
2.	Transformation Phase:
○	The extracted data undergoes transformation to convert the measurements to standard units (e.g., height to meters, weight to kilograms).
3.	Loading Phase:
○	The transformed data is written into a CSV file, which can be imported into a database for further use.
4.	Logging:
○	The start and end of each phase (Extraction, Transformation, Loading) are logged to track progress and ensure everything runs smoothly.

Conclusion:
This project highlights the practical implementation of ETL processes using Python. The data extraction from multiple file formats, transformation of units, and loading of the final data into a structured CSV format demonstrate essential data engineering skills. Additionally, by logging each step of the process, you can monitor the progress and debug issues if they arise

Official Documentation:
●	https://www.python.org/doc/
