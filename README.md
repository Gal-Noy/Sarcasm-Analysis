# Sarcasm Analysis

## Overview
This project implements a distributed application for sentiment analysis, named-entity recognition, and sarcasm detection on Amazon reviews. The application consists of a local application, a manager, and multiple worker nodes running on the Amazon cloud.

## System Architecture
The system is composed of three main components:
1. **Local Application**: Runs on a local machine and interacts with the manager and workers on AWS.
2. **Manager**: Manages task distribution and coordination among worker nodes.
3. **Workers**: Perform sentiment analysis, named-entity recognition, and sarcasm detection on reviews.

## Running the Application:
To run the application, follow these steps:
1. **Compile the Code**: Compile the Java code into a JAR file using Maven.
2. **Run the Local Application**: Execute the following command on your local machine:
   
   ```
   java -jar SarcasmAnalysis.jar inputFileName1... inputFileNameN outputFileName1... outputFileNameN n [terminate]
   ```
- `inputFileNameI`: Name of the input file containing Amazon reviews.
- `outputFileNameI`: Name of the output file containing analysis results.
- `n`: Number of reviews/messages per worker.
- `terminate` (optional): Indicates that the application should terminate the manager at the end.

## Details
### Instance Details
- **Instance type**: M4.Large
- **AMI**: Amazon Linux with Java JDK 1.8, Amazon AWS CLI, and necessary JAR files.

### Time Taken (including manager and workers initiation)
- localapp1: `input1.txt output1 200` => 490 reviews, 980 jobs, 5 workers - **3:32**
- localapp2: `input1.txt output1 100` => 490 reviews, 980 jobs, 8 workers (max) - **3:07**
- localapp3: `input1.txt input2.txt output1 output2 245` => 980 reviews, 1960 jobs, 8 workers (max) - **6:14**
- localapp4: `input1.txt input2.txt input3.txt input4.txt input5.txt 610` => 2450 reviews, 4900 jobs, 8 workers (max) - **16:57**

### Other Details:
- The system uses Amazon SQS for message queuing and Amazon S3 for storage.
- Sentiment analysis and named-entity recognition are performed using Stanford CoreNLP.
- Sarcasm detection is based on the sentiment score and review ratings.

## System Summary:
1. Local Application uploads the input files to S3.
2. Local Application sends a message indicating the location of the input files on S3.
3. Manager distributes tasks to workers and bootstraps nodes as needed.
4. Worker nodes perform the requested tasks and return results.
5. Manager creates summary file, uploads it to S3 and notifies the local application.
6. Local application downloads the summary file, generates an HTML output file, and terminates the manager if specified.

## References:
- AWS documentation for AWS SDK and services.
- Stanford CoreNLP for sentiment analysis and named-entity recognition.
