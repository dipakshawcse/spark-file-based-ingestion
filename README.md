# Spark File-based Ingestion

This project demonstrates the ingestion of data from various file-based sources (CSV, JSON, Parquet, Avro, XML, Excel) using Apache Spark.

## Project Structure

The project repository contains the following structure:

- `data/`: Directory to store sample data files for ingestion.
- `src/`: Directory to store the project source code.
  - `main/`: Main source code directory.
    - `scala/`: Scala code directory.
      - `FileIngestionApp.scala`: Main Scala application for file-based data ingestion using Spark.
    - `python/`: Python code directory.
      - `file_ingestion_app.py`: Main Python application for file-based data ingestion using Spark.
  - `test/`: Directory to store test code (if applicable).
- `README.md`: Project documentation with instructions, usage examples, and explanations.
- `requirements.txt`: List of project dependencies (if applicable).

## Getting Started

### Prerequisites

- Apache Spark (version 3.1.X)
- Python (version 3.0.X) for Python application
- Scala (version 2.11.X) for Scala application
  

### Usage

1. Clone the repository:

   ```shell
   git clone https://github.com/your-username/spark-file-based-ingestion.git

2. Navigate to the appropriate directory based on your choice of programming language:

   For Scala application:

   ```shell
   cd src/main/scala
   ```

   For Python application:

   ```shell
   cd src/main/python
   ```

4. Set the path to the input files in the application file (FileIngestionApp.scala or file_ingestion_app.py).
5. Run the application:
   For Scala application:

   ```shell
   spark-submit FileIngestionApp.scala
   ```

   For Python application:

   ```shell
   spark-submit file_ingestion_app.py
   ```

6. The application will read the data from file-based sources using Spark, perform necessary operations or transformations, and produce the desired output.

### Dependencies
If your project has any dependencies, list them in the requirements.txt file. This file can be used to install the dependencies with the following command:

```shell
pip install -r requirements.txt
```

## Contributing

Contributions are welcome! If you have any ideas, improvements, or additional file formats you would like to contribute, please feel free to submit a pull request.

