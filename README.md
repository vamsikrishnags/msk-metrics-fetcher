# MSK Cluster Performance & Configuration Reporter 📈

This Python script scans an AWS account to generate a comprehensive CSV report on all Amazon MSK clusters. It handles both **Provisioned** and **Serverless** cluster types, fetching key configuration details and performance metrics over a defined period.

## ✨ Features

-   **Dual Cluster Support**: Works seamlessly with both Provisioned and Serverless MSK clusters.
-   **Comprehensive Metrics**:
    -   For **Serverless** clusters, it captures peak and average `BytesInPerSec` and `BytesOutPerSec`.
    -   For **Provisioned** clusters, it captures a wide range of metrics, including `StorageUsedPercent`, `GlobalPartitionCount`, `BytesInPerSec`, and more.
-   **Multi-Region Scan**: Automatically discovers all regions with MSK, or allows you to specify a list of regions to scan.
-   **Simple Credentials**: Works with a configured AWS CLI profile or instance/task IAM roles.
-   **Single CSV Output**: Aggregates all data into a single, timestamped CSV file for easy analysis.

---

## 📋 Prerequisites

1.  **Python 3.6+**
2.  **AWS Account**: Credentials configured in your environment (e.g., via `aws configure` or an IAM role).
3.  **Required Python libraries**:
    -   `boto3`: The AWS SDK for Python.
    -   `pandas`: For data manipulation and CSV export.

   

---

## 🔐 IAM Permissions

The script requires a set of read-only permissions to function correctly. Create an IAM policy with the JSON below and attach it to the user or role running the script.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "MSKReportGeneratorPermissions",
            "Effect": "Allow",
            "Action": [
                "kafka:ListClusters",
                "kafka:ListClustersV2",
                "kafka:DescribeCluster",
                "kafka:DescribeClusterV2",
                "cloudwatch:GetMetricStatistics",
                "ec2:DescribeSubnets",
                "sts:GetCallerIdentity"
            ],
            "Resource": "*"
        }
    ]
}
```

---

## 🚀 How to Run

1.  **Clone the repository or save the script** to your local machine.
    ```bash
    git clone https://github.com/vamsikrishnags/msk-metrics-fetcher
    cd msk-metrics-fetcher
    ```
    
2.  **Activte Vritual Environment**
    make sure your have python version is >=3.8
    ```bash
    python3 -m venv venv
    ```
    ```bash
    source venv/bin/activate
    ```
    
3.  **Install the dependencies**:
     You can install them using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```
    
4.  **Ensure your AWS credentials are configured**. You can do this by:
    -   Running `aws configure` to set up a default profile.
    -   Running the script on an EC2 instance or Lambda with an attached IAM role.
5.  **Execute the script** from your terminal:
    ```bash
    python your_script_name.py
    ```
6.  **Follow the interactive prompts**:
    -   **AWS CLI Profile Name**: Press `Enter` to use the default profile/role, or type the name of a specific profile.
    -   **Region IDs**: Press `Enter` to scan all available MSK regions, or provide a comma-separated list (e.g., `us-east-1,eu-west-2`).

---

## 📊 Output

The script will generate a CSV file named `msk_cluster_report_YYYY-MMM-DD_HH-MM-SS.csv`. This file contains one row for each MSK cluster with columns such as:

-   `AccountID`
-   `Region`
-   `ClusterName`
-   `ClusterType` (Provisioned/Serverless)
-   `ClusterArn`
-   `KafkaVersion`
-   `NumberOfBrokerNodes` (N/A for Serverless)
-   `BrokerInstanceType` (N/A for Serverless)
-   `BytesInPerSec_Avg`
-   `BytesInPerSec_Peak`
-   `BytesOutPerSec_Avg`
-   `BytesOutPerSec_Peak`
-   ...and other relevant metrics.

---
