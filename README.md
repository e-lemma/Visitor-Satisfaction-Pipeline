# Visitor Satisfaction Pipeline

## Overview
The Visitor Satisfaction Pipeline is a project for analysing and improving a museum's visitor satisfaction through data collection, processing, and analysis.

## Features
- **Data Collection**: Gather visitor feedback through surveys and other channels.
- **Data Processing**: Clean and preprocess the collected data.
- **Data Analysis**: Analyse the data to identify trends and patterns.

## Installation
To get started with the Visitor Satisfaction Pipeline, follow these steps:

1. **Clone the repository**:
    ```bash
    git clone https://github.com/e-lemma/Visitor-Satisfaction-Pipeline.git
    ```
2. **Navigate to the project directory**:
    ```bash
    cd Visitor-Satisfaction-Pipeline
    ```
3. **Install the required dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage
There are two pipelines: one for processing **JSON** data and another for consuming messages from a **Kafka topic**. For detailed instructions on each, refer to the respective directories:

- [JSON Pipeline](./json_pipeline/README.md)
- [Kafka Pipeline](./kafka-pipeline/README.md)