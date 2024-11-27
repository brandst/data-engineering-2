
# Project Setup Instructions

## General Information
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
     - **USERNAME**: Set to your username (the name visible before `@instance` in SSH).
     - **EXTERNAL_IP**: Add the external IP of your VM. *(This step needs to be repeated after restarting the machine.)*
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
Run the following commands in **Cloud Shell**:
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
2. Create a table named **IMDB_Top_1000** with the following schema:
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
     - `IMDB_analysis_batch` notebook.

2. **Data Folder:**
   - Upload the dataset (IMDB data) to the **data** folder.

3. **Run Analysis:**
   - Open the `IMDB_analysis_batch` notebook in JupyterLab.
   - Verify the variables in the third cell are correctly set.

4. **Result:**
   - New tables will appear in the `labdataset` in BigQuery.

---

