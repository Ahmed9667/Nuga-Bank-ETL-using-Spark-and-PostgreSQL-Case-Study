# Nuga Bank ETL Case Study

## Summary:
● Nuga Bank, a leading financial institution, embarked on a strategic initiative
to enhance its data exploration and cleaning processes using PySpark. This
case study delves into the project, outlining how Nuga Bank leveraged
PySpark to streamline data preparation, enabling better insights and
decision-making.

## Objectives:
### ● The primary objectives for this project are to:
● Implement an automated data exploration and cleaning solution using
PySpark to streamline the process.

● Normalize the dataset into a suitable database format (2NF or 3NF) for
improved data integrity and consistency.

● Load the cleaned and normalized dataset into a PostgreSQL server for
further analysis and reporting

![image](https://github.com/user-attachments/assets/70753503-a8dd-433b-b460-ac5827e799dd)

## Project Scope:
#### ● Data Extraction:
● Spark Context Engine: Setup a Spark Context Engine for distributed computing.
● Load CSV: Use PySpark to load the CSV file into a Spark DataFrame.

#### ● Data Transformation:
● Data Cleaning: Utilize PySpark to clean and preprocess the dataset, handling missing values, duplicates, and
inconsistencies.
● Normalization: Transform the dataset into a suitable normalized form (2NF or 3NF) for database storage.

#### ● Data Loading:
● PostgreSQL Server: Load the cleaned and normalized dataset into a PostgreSQL server for further analysis and
reporting.
```
db_params =  {
    'username' : 'postgres',
    'password' : 'ahly9667',
    'host' : 'localhost',
    'port' : '5432',
    'database' : 'nuga'
}

# define the database connection url with db parameters
db_url = f"postgresql://{db_params['username']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# Create the database engine with the  db url
engine = create_engine(db_url)

# Connect to PostgreSQL server
with engine.connect() as connection:
    # Create tables and load the data
    transaction.to_sql('transaction', connection, index=False, if_exists='replace')
    customer.to_sql('customer', connection, index=False, if_exists='replace')
    employee.to_sql('employee', connection, index=False, if_exists='replace')
    fact_table.to_sql('fact_table', connection, index=False, if_exists='replace')

print('tables and data loaded successfully ')
```

## ERD Schema:
![image](https://github.com/user-attachments/assets/6a163fce-3a64-44b2-a476-1a13d05e8f55)
