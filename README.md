# IMDB Movie Analysis
This project processes and analyzing IMDb movie data using Apache Spark for distributed data processing and Google BigQuery for database operations. 
The goal is to provide insights for stakeholders, such as movie enthusiasts, production companies, and streaming platforms, through visualizations created in Looker Studio.

Key Features:
- Scalable Data Processing: Spark pipelines can efficiently handle large movie datasets.
- Batch and Stream Processing: We use real-time stream processing with Spark Structured Streaming to analyze low-rating movie reviews as they arrive. Additionally, batch processing is used for historical data.
- Cloud Integration: deployed on Google Cloud Platform with BigQuery for data storage and querying.
- Interactive Dashboards with visual insights tailored for stakeholders.

# Project Setup Instructions

## General Information
- Use a Virtual Machine with multiple cores (e.g. type e2-standard-8 (8 vCPUs, 32 GB memory))
- Ensure the following VM settings:
   - Ubuntu 24.04 LTS (x86/64)
   - Enable access to All Google Cloud service APIs
   - Allow both HTTP and HTTPs traffic
  
- Use **SPARK** for data processing (not pandas).
- **BigQuery** for database operations and queries.

---

## Setup Instructions

### Step 1: Clone Repository
Clone the repository to your Virtual Machine (VM) in SSH:
```bash
git clone <repository-url>
```

### Step 2: Install Docker & Docker Compose
1. Navigate to the `installation_script/` directory in the `data-engineering-2/` folder:
   ```bash
   cd installation_script/
   ```
2. Run the installation scripts:
   ```bash
   sh docker.sh
   sh docker_compose.sh
   ```

---

### Step 3: Deploy Data Architecture
1. Navigate to the home directory:
   ```bash
   cd
   ```
2. Create the required folders:
   ```bash
   mkdir notebooks
   mkdir data
   mkdir checkpoint
   sudo chmod 777 notebooks/
   sudo chmod 777 data/
   sudo chmod 777 checkpoint/
   ```

---

### Step 4: Configure Environment Variables
1. Navigate to the `deployment/` folder:
   ```bash
   cd deployment/
   ```
2. Open and edit the `.env` (hidden) file using `vim`:
   ```bash
   vi .env
   ```
   - Press `i` to type.
   - Modify the following variables:
     - **EXTERNAL_IP**: Add the external IP of your VM. *(This step needs to be repeated after restarting the machine.)*
     - **USER_HOME**: Set to your username (the name visible before `@instance` in SSH. Double check if it's correct! Otherwise it can be hard to undo this).

   - Save changes:
     - Press `Esc` to stop typing.
     - Type `:wq!` to save and exit.

---

### Step 5: Build and Start Docker Services
1. From the `deployment/` folder:
   ```bash
   sudo docker compose build
   sudo docker compose up -d
   ```

---

### Step 6: Configure Firewall Rules
Run the following commands in **Cloud Shell** (at the top of the page, near your google account logo):
```bash
gcloud compute firewall-rules create jupyter-port --allow tcp:8888
gcloud compute firewall-rules create spark-master-port --allow tcp:7077
gcloud compute firewall-rules create spark-master-ui-port --allow tcp:8080
gcloud compute firewall-rules create spark-driver-ui-port --allow tcp:4040
gcloud compute firewall-rules create spark-worker-1-ui-port --allow tcp:8081
gcloud compute firewall-rules create spark-worker-2-ui-port --allow tcp:8082
gcloud compute firewall-rules create kafka-port --allow tcp:9092
```

---

### Step 7: Access JupyterLab
From the `deployment/` folder:

(Ensure to restart the containers when the VM has been stopped: 
```bash
sudo docker compose up -d
```
1. Check the logs for the JupyterLab URL:
   ```bash
   sudo docker logs spark-driver-app
   ```
2. Replace the hostname in the URL with your VM's external IP (e.g., `http://<EXTERNAL_IP>:8888/lab?token=...`).
3. Open the updated URL in your browser.

---

## BigQuery Setup

### Enable BigQuery API
1. Search for "BigQuery" in your Google Cloud Console.
2. Enable the API.

---

### Create Dataset and Table 
1. Create a dataset named **labdataset**:
   - In the BigQuery interface, click the 3 dots next to your project ID.
   - Select **Create Dataset** and name it `labdataset`.

#### The following step (2) is actually not necessary if you use BigQueryLoaderReader.ipynb as described in the Data Upload and Analysis step below. It will create a new table.
2. Create a table named **IMDB_Top_1000** by copying and running the following query (click on the `+` for a new query):
   ```sql
   CREATE TABLE labdataset.IMDB_Top_1000 (
       Poster_Link STRING,
       Series_Title STRING,
       Released_Year INT64,
       Certificate STRING,
       Runtime STRING,
       Genre STRING,
       IMDB_Rating FLOAT64,
       Overview STRING,
       Meta_score FLOAT64,
       Director STRING,
       Star1 STRING,
       Star2 STRING,
       Star3 STRING,
       Star4 STRING,
       No_of_Votes INT64,
       Gross INT64
   );
   

---

## Data Upload and Analysis

1. **Upload Files:**
   - Open JupyterLab using the updated URL.
   - Upload the following to the **notebooks** folder:
     - `BigQueryLoaderReader.ipynb` notebook.
     - Batch notebook (`pipelines/imdb_analysis_batch_additional.ipynb`).

2. **Data Folder:**
   - Upload the dataset (IMDB data) to the **data** folder.
   - Open the `BigQueryLoaderReader.ipynb` notebook.
   - Run it.

3. **Run Analysis:**
   - Open the Batch notebook in JupyterLab.
   - Verify the variables in the third cell are correctly set.
   - Run it.
   
4. **Result:**
   - New tables will appear in the `labdataset` in BigQuery.

---


## Stream Processing

To set up and execute stream processing, follow these steps in order:
1. Upload the Stream notebook to Jupyterlab on the VM.

2. **Run `kafkaadmin.py`**:
   - Ensure the correct external IP is specified in the file.

3. **Run the Stream Notebook**:
   - Open JupyterLab on the VM and execute the stream notebook.

4. **Run `producer.py`**:
   - Ensure the correct external IP and the correct path to the data file are specified.

5. **Run `multithread-consumer.py`.**

6. **Check BigQuery**:
   - View the results in the BigQuery table.

---

## Spark Jobs on Google Dataproc

### Enable Required APIs:
1. **Cloud Dataproc API**
2. **Cloud Resource Manager API**

---

### Dataproc Setup:

1. **Create Cluster**:
   - Use **Compute Engine**.
   - **Enable Component Gateway** and **Jupyter Notebook for the Cluster**.

2. **Configure Nodes**:
   - For both master and worker configurations, ensure:
     - **Machine Type**: `n2-standard-2` (2 vCPUs, 8 GB memory).
     - **Primary Disk Size**: 50 GB.

3. **Customize Cluster**:
   - Under "Internal IP Only," uncheck "Configure all instances to have only internal IP."

4. **Note**:
   - Ignore error messages if the setup works correctly.

---

### Running Spark Jobs

1. **Prepare the Spark File**:
   - Adjust the `project ID` and `bucket name` in the dataproc Stream file (`pipelines/dataproc_...`.

2. **Upload to Storage Bucket**:
   - Upload the dataproc Stream file to the Google Cloud Storage bucket.
   - Locate the **GS URL** of the file (e.g., `gs://imdb-spark-stream/<filename>.py`).

3. **Submit Spark Job**:
   - Go to **Dataproc** > **Clusters** > **Submit Job**.
   - Set **Job Type** to **PySpark**.
   - Use the **GS URL** as the main Python file.

4. **Additional Configuration for Kafka** (if utilized):
   - In **Dataproc** > **Submit Jobs**, add the following property:
     - **Key**: `spark.jars.packages`
     - **Value**: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`

---


