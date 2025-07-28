import boto3
import pandas as pd
from datetime import datetime, timedelta
import traceback

# --- Configuration ---
OUTPUT_CSV_FILE_BASE = 'msk_cluster_report'
METRICS_PERIOD_DAYS = 7  # For CloudWatch metrics (e.g., 7 days for 1 week)

def get_msk_cluster_details(msk_client, session_for_region, cluster_arn):
    """Gets detailed information for a single MSK cluster, detecting its type."""
    cluster_info = None
    cluster_type = 'Provisioned' # Default to provisioned
    kafka_version = None
    broker_instance_type = 'N/A'
    number_of_broker_nodes = 'N/A'
    storage_per_broker_gb = 'N/A'
    number_of_availability_zones = 0
    cluster_name = None
    creation_time_str = None
    auth_string = "N/A"

    try:
        # DescribeClusterV2 is preferred as it clearly separates provisioned/serverless
        try:
            response = msk_client.describe_cluster_v2(ClusterArn=cluster_arn)
            cluster_info = response.get('ClusterInfo', {})
        except Exception:
            try:
                # Fallback to V1 for older environments
                response = msk_client.describe_cluster(ClusterArn=cluster_arn)
                cluster_info = response.get('ClusterInfo', {})
            except Exception as e_v1:
                print(f"    Error describing cluster {cluster_arn} with both V2 & V1 APIs: {e_v1}")
                return None

        if not cluster_info:
            print(f"    Could not retrieve cluster_info for {cluster_arn}")
            return None

        cluster_name = cluster_info.get('ClusterName')
        creation_time_obj = cluster_info.get('CreationTime')
        if creation_time_obj and isinstance(creation_time_obj, datetime):
            creation_time_str = creation_time_obj.strftime('%Y-%m-%d %H:%M:%S')
        elif creation_time_obj:
            creation_time_str = str(creation_time_obj)

        # Determine Cluster Type and extract details accordingly
        if cluster_info.get('Provisioned'):
            cluster_type = 'Provisioned'
            provisioned_info = cluster_info['Provisioned']
            kafka_version = provisioned_info.get('CurrentBrokerSoftwareInfo', {}).get('KafkaVersion')
            number_of_broker_nodes = provisioned_info.get('NumberOfBrokerNodes')
            broker_node_group_info = provisioned_info.get('BrokerNodeGroupInfo', {})
            broker_instance_type = broker_node_group_info.get('InstanceType')
            if broker_node_group_info.get('StorageInfo', {}).get('EbsStorageInfo'):
                storage_per_broker_gb = broker_node_group_info['StorageInfo']['EbsStorageInfo'].get('VolumeSize')

            zone_ids = broker_node_group_info.get('ZoneIds')
            if zone_ids and isinstance(zone_ids, list):
                number_of_availability_zones = len(zone_ids)
            else:
                client_subnets = broker_node_group_info.get('ClientSubnets', [])
                if client_subnets:
                    try:
                        ec2_client = session_for_region.client('ec2')
                        availability_zones_set = set()
                        subnet_chunks = [client_subnets[i:i + 100] for i in range(0, len(client_subnets), 100)]
                        for chunk in subnet_chunks:
                            subnet_details_response = ec2_client.describe_subnets(SubnetIds=chunk)
                            for subnet in subnet_details_response.get('Subnets', []):
                                availability_zones_set.add(subnet['AvailabilityZone'])
                        number_of_availability_zones = len(availability_zones_set)
                    except Exception as ec2_err:
                        print(f"      Warning: Could not describe subnets for AZ count for {cluster_name}. Error: {ec2_err}")
                        number_of_availability_zones = len(client_subnets)

            client_auth_info = provisioned_info.get('ClientAuthentication')

        elif cluster_info.get('Serverless'):
            cluster_type = 'Serverless'
            serverless_info = cluster_info['Serverless']
            # Serverless doesn't expose a specific Kafka version in the same way
            kafka_version = 'Managed (Serverless)'
            client_auth_info = serverless_info.get('ClientAuthentication')

        # Generic info fallback for V1 API responses
        if not kafka_version:
             kafka_version = cluster_info.get('CurrentBrokerSoftwareInfo', {}).get('KafkaVersion')
        if number_of_broker_nodes == 'N/A' and cluster_info.get('NumberOfBrokerNodes'):
             number_of_broker_nodes = cluster_info.get('NumberOfBrokerNodes')

        # Client Authentication Logic (works for both types)
        auth_methods = []
        if client_auth_info:
            if client_auth_info.get('Sasl', {}).get('Iam', {}).get('Enabled'): auth_methods.append('IAM')
            if client_auth_info.get('Sasl', {}).get('Scram', {}).get('Enabled'): auth_methods.append('SCRAM')
            if client_auth_info.get('Tls', {}).get('Enabled'): auth_methods.append('mTLS')
            if client_auth_info.get('Unauthenticated', {}).get('Enabled'): auth_methods.append('Unauthenticated')
            auth_string = ", ".join(auth_methods) if auth_methods else "None Enabled"

        details = {
            'ClusterName': cluster_name,
            'ClusterArn': cluster_arn,
            'ClusterType': cluster_type,
            'CreationTime': creation_time_str,
            'KafkaVersion': kafka_version,
            'Authentication': auth_string,
            'NumberOfBrokerNodes': number_of_broker_nodes,
            'BrokerInstanceType': broker_instance_type,
            'StoragePerBrokerGB': storage_per_broker_gb,
            'NumberOfAvailabilityZones': number_of_availability_zones,
        }
        return details

    except Exception as e:
        print(f"    Error processing cluster details for {cluster_arn}: {e}")
        traceback.print_exc()
        return None

def get_msk_metrics(cloudwatch_client, cluster_name, cluster_type, number_of_broker_nodes, end_time_dt, period_days):
    """Gets specified CloudWatch metrics for an MSK cluster based on its type."""
    metrics_data = {}
    start_time_agg = end_time_dt - timedelta(days=period_days)
    query_period_agg_seconds = period_days * 24 * 60 * 60
    
    # --- SERVERLESS METRIC LOGIC ---
    if cluster_type == 'Serverless':
        print(f"      Fetching Serverless metrics for {cluster_name}...")
        serverless_metric_configs = [
            {"name": "BytesInPerSec", "unit": "Bytes/Second"},
            {"name": "BytesOutPerSec", "unit": "Bytes/Second"},
        ]
        for config in serverless_metric_configs:
            metric_name = config["name"]
            try:
                response = cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/Kafka-Serverless',
                    MetricName=metric_name,
                    Dimensions=[{'Name': 'Cluster Name', 'Value': cluster_name}],
                    StartTime=start_time_agg,
                    EndTime=end_time_dt,
                    Period=query_period_agg_seconds,
                    Statistics=['Average', 'Maximum'],
                    Unit=config["unit"]
                )
                avg_val, peak_val = None, None
                if response['Datapoints']:
                    datapoint = response['Datapoints'][0]
                    avg_val = datapoint.get('Average')
                    peak_val = datapoint.get('Maximum')
                
                metrics_data[f'{metric_name}_Avg'] = avg_val
                metrics_data[f'{metric_name}_Peak'] = peak_val
                print(f"        Fetched {metric_name}: Avg={avg_val}, Peak={peak_val}")
            except Exception as e:
                print(f"      Error fetching serverless metric {metric_name} for {cluster_name}: {e}")
                metrics_data[f'{metric_name}_Avg'] = None
                metrics_data[f'{metric_name}_Peak'] = None
        return metrics_data

    # --- PROVISIONED METRIC LOGIC (Original Code) ---
    elif cluster_type == 'Provisioned':
        print(f"      Fetching Provisioned metrics for {cluster_name}...")
        start_time_latest = end_time_dt - timedelta(minutes=15)
        period_latest_seconds = 60

        metric_configs = [
            {"name": "StorageUsedPercent", "namespace": "AWS/Kafka", "cw_metric_name": "KafkaDataLogsDiskUsed", "unit": "Percent", "aggregation_type": "avg_brokers_peak_max_agg"},
            {"name": "GlobalPartitionCount", "namespace": "AWS/Kafka", "cw_metric_name": "GlobalPartitionCount", "unit": "Count", "aggregation_type": "cluster_latest_value"},
            {"name": "GlobalTopicCount", "namespace": "AWS/Kafka", "cw_metric_name": "GlobalTopicCount", "unit": "Count", "aggregation_type": "cluster_latest_value"},
            {"name": "BytesInPerSec", "namespace": "AWS/Kafka", "cw_metric_name": "BytesInPerSec", "unit": "Bytes/Second", "aggregation_type": "sum_brokers_agg"},
            {"name": "BytesOutPerSec", "namespace": "AWS/Kafka", "cw_metric_name": "BytesOutPerSec", "unit": "Bytes/Second", "aggregation_type": "sum_brokers_agg"},
            {"name": "ClientConnectionCount", "namespace": "AWS/Kafka", "cw_metric_name": "ClientConnectionCount", "unit": "Count", "aggregation_type": "sum_brokers_agg"},
            {"name": "ConnectionCloseRate", "namespace": "AWS/Kafka", "cw_metric_name": "ConnectionCloseRate", "unit": "Count/Second", "aggregation_type": "sum_brokers_agg"},
            {"name": "ConnectionCreationRate", "namespace": "AWS/Kafka", "cw_metric_name": "ConnectionCreationRate", "unit": "Count/Second", "aggregation_type": "sum_brokers_agg"},
            {"name": "RequestBytesMean", "namespace": "AWS/Kafka", "cw_metric_name": "RequestBytesMean", "unit": "Bytes", "aggregation_type": "avg_brokers_peak_max_agg"},
        ]

        for config in metric_configs:
            metric_display_name = config["name"]
            cluster_avg_value, cluster_peak_value, latest_value = None, None, None

            try:
                if config["aggregation_type"] == "cluster_latest_value":
                    dimensions = [{'Name': 'Cluster Name', 'Value': cluster_name}]
                    response = cloudwatch_client.get_metric_statistics(
                        Namespace=config["namespace"], MetricName=config["cw_metric_name"], Dimensions=dimensions,
                        StartTime=start_time_latest, EndTime=end_time_dt, Period=period_latest_seconds,
                        Statistics=['Maximum'], Unit=config["unit"]
                    )
                    if response['Datapoints']:
                        latest_dp = sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[0]
                        latest_value = latest_dp.get('Maximum')
                    metrics_data[metric_display_name] = latest_value
                    if latest_value is not None:
                        print(f"        Fetched {metric_display_name}: CurrentValue={latest_value}")

                elif config["aggregation_type"] in ["sum_brokers_agg", "avg_brokers_peak_max_agg"]:
                    if not number_of_broker_nodes or int(number_of_broker_nodes) == 0: continue
                    
                    per_broker_avg_values, per_broker_peak_values = [], []

                    for i in range(1, int(number_of_broker_nodes) + 1):
                        broker_id_str = str(i)
                        dimensions = [{'Name': 'Cluster Name', 'Value': cluster_name}, {'Name': 'Broker ID', 'Value': broker_id_str}]
                        try:
                            response_broker = cloudwatch_client.get_metric_statistics(
                                Namespace=config["namespace"], MetricName=config["cw_metric_name"], Dimensions=dimensions,
                                StartTime=start_time_agg, EndTime=end_time_dt, Period=query_period_agg_seconds,
                                Statistics=['Average', 'Maximum'], Unit=config["unit"]
                            )
                            if response_broker['Datapoints']:
                                datapoint = response_broker['Datapoints'][0]
                                if 'Average' in datapoint: per_broker_avg_values.append(datapoint['Average'])
                                if 'Maximum' in datapoint: per_broker_peak_values.append(datapoint['Maximum'])
                        except Exception as broker_e:
                            print(f"        Error fetching aggregation for {metric_display_name} (Broker ID {broker_id_str}): {broker_e}")

                    if config["aggregation_type"] == "sum_brokers_agg":
                        if per_broker_avg_values: cluster_avg_value = sum(per_broker_avg_values)
                        if per_broker_peak_values: cluster_peak_value = sum(per_broker_peak_values)
                    
                    elif config["aggregation_type"] == "avg_brokers_peak_max_agg":
                        if per_broker_avg_values: cluster_avg_value = sum(per_broker_avg_values) / len(per_broker_avg_values)
                        if per_broker_peak_values: cluster_peak_value = max(per_broker_peak_values)
                    
                    metrics_data[f'{metric_display_name}_Avg'] = cluster_avg_value
                    metrics_data[f'{metric_display_name}_Peak'] = cluster_peak_value

                    if cluster_avg_value is not None or cluster_peak_value is not None:
                        print(f"        Fetched {metric_display_name}: Avg={cluster_avg_value}, Peak={cluster_peak_value}")

            except Exception as e:
                print(f"      Overall error processing metric {metric_display_name} for cluster {cluster_name}: {e}")
                if config["aggregation_type"] == "cluster_latest_value":
                    metrics_data[metric_display_name] = None
                else:
                    metrics_data[f'{metric_display_name}_Avg'] = None
                    metrics_data[f'{metric_display_name}_Peak'] = None
                
        return metrics_data
    
    else:
        print(f"      Unknown cluster type '{cluster_type}' for {cluster_name}. Skipping metrics.")
        return {}


# --- Main Logic ---
def main():
    all_cluster_data = []
    end_time_metrics = datetime.utcnow()
    start_script_time = datetime.now()

    print("--- MSK Cluster Reporting Script (Single Account) ---")

    primary_profile_input = input("Enter AWS CLI Profile Name [press Enter if using default profile, CloudShell, EC2/Lambda IAM roles]: ").strip()
    PRIMARY_PROFILE_NAME = primary_profile_input or None

    print("\nScript starting with the following configuration:")
    print(f"  AWS Profile: {PRIMARY_PROFILE_NAME if PRIMARY_PROFILE_NAME else 'Default/IAM Role'}")
    print("-------------------------------------\n")

    try:
        session_kwargs = {}
        if PRIMARY_PROFILE_NAME:
            session_kwargs['profile_name'] = PRIMARY_PROFILE_NAME
        
        primary_session = boto3.Session(**session_kwargs)
        
        sts_client = primary_session.client('sts')
        identity = sts_client.get_caller_identity()
        account_id = identity.get('Account')
        print(f"\nSuccessfully initialized session for AWS Account: {account_id}")

    except Exception as e:
        print(f"Fatal Error: Could not initialize Boto3 session. Error: {e}")
        print("Please ensure your AWS credentials (profile or IAM role) are configured correctly.")
        return
    
    # --- REGION HANDLING LOGIC ---
    REGIONS_TO_SCAN = []
    user_region_input = input("Enter comma-separated region IDs (e.g., ap-south-1,us-east-1) or press Enter for all enabled regions: ").strip()

    if user_region_input:
        print("Validating user-provided regions...")
        try:
            # Fetch all possible regions to validate against. EC2 is a good service for this as it's in all standard regions.
            all_possible_regions = primary_session.get_available_regions('kafka')
            user_regions = [r.strip() for r in user_region_input.split(',')]
            
            valid_user_regions = []
            invalid_user_regions = []

            for region in user_regions:
                if region in all_possible_regions:
                    valid_user_regions.append(region)
                else:
                    invalid_user_regions.append(region)
            
            if invalid_user_regions:
                print(f"  Warning: The following region IDs are invalid and will be skipped: {invalid_user_regions}")

            if not valid_user_regions:
                print("Fatal Error: No valid regions were provided to scan. Exiting.")
                return
            
            REGIONS_TO_SCAN = valid_user_regions
            print(f"  Will scan the following valid regions: {REGIONS_TO_SCAN}")

        except Exception as e:
            print(f"Fatal Error: Could not validate regions. Error: {e}")
            return
    else:
        print("\nNo regions specified. Discovering all regions for MSK...")
        try:
            # This discovers regions where the current account has MSK enabled (not disabled).
            REGIONS_TO_SCAN = primary_session.get_available_regions('kafka')
            if not REGIONS_TO_SCAN:
                 print("  Warning: Could not dynamically discover any enabled regions for MSK. The account may not have MSK active anywhere.")
            else:
                print(f"  Discovered MSK regions: {REGIONS_TO_SCAN}")
        except Exception as e:
            print(f"  Error discovering regions: {e}. Attempting to use a common default list.")
            REGIONS_TO_SCAN = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1', 'ap-northeast-1', 'ap-south-1']
    
    if not REGIONS_TO_SCAN:
        print("No regions to scan. Exiting script.")
        return
    # --- END REGION HANDLING ---

    for region in REGIONS_TO_SCAN:
        print(f"\nScanning Region: {region}...")
        try:
            region_session_kwargs = session_kwargs.copy()
            region_session_kwargs['region_name'] = region
            current_session = boto3.Session(**region_session_kwargs)
            
            msk_client = current_session.client('kafka')
            cloudwatch_client = current_session.client('cloudwatch')
        except Exception as client_e:
            print(f"    Error creating AWS service clients for region {region}: {client_e}")
            continue
        
        # Use list_clusters_v2 as it's the newer API
        try:
            paginator = msk_client.get_paginator('list_clusters_v2')
            pages = paginator.paginate()
            clusters_in_region = [cluster for page in pages for cluster in page.get('ClusterInfoList', [])]
        except Exception:
            # Fallback to list_clusters if v2 is not available
            try:
                paginator = msk_client.get_paginator('list_clusters')
                pages = paginator.paginate()
                # V1 just gives ARNs, so we need to wrap them for the loop
                clusters_in_region = [{'ClusterArn': arn} for page in pages for arn in page.get('ClusterInfoList', [])]
            except Exception as list_e:
                 print(f"    Could not list clusters in region {region} with either V1 or V2 API. Error: {list_e}")
                 continue

        if not clusters_in_region:
            print("    No clusters found in this region.")
            continue

        for cluster_summary in clusters_in_region:
            cluster_arn = cluster_summary['ClusterArn']
            print(f"  Processing cluster: {cluster_arn}")
            
            cluster_details = get_msk_cluster_details(msk_client, current_session, cluster_arn)
            if cluster_details:
                cluster_name_for_metrics = cluster_details.get('ClusterName')
                number_of_brokers = cluster_details.get('NumberOfBrokerNodes')
                cluster_type = cluster_details.get('ClusterType')
                
                if not cluster_name_for_metrics:
                    print(f"      Could not determine ClusterName for {cluster_arn}. Skipping metrics.")
                    continue
                
                metrics = get_msk_metrics(cloudwatch_client, cluster_name_for_metrics, cluster_type, number_of_brokers, end_time_metrics, METRICS_PERIOD_DAYS)
                
                row = {
                    'AccountID': account_id,
                    'Region': region,
                    **cluster_details,
                    **metrics
                }
                all_cluster_data.append(row)
            else:
                print(f"      Could not retrieve details for cluster {cluster_arn}. Skipping.")


    # Write to CSV
    if all_cluster_data:
        df = pd.DataFrame(all_cluster_data)
        
        metric_configs_for_cols = [
            {"name": "StorageUsedPercent", "aggregation_type": "avg_brokers_peak_max_agg"},
            {"name": "GlobalPartitionCount", "aggregation_type": "cluster_latest_value"},
            {"name": "GlobalTopicCount", "aggregation_type": "cluster_latest_value"},
            {"name": "BytesInPerSec", "aggregation_type": "sum_brokers_agg"}, # Name is same for serverless
            {"name": "BytesOutPerSec", "aggregation_type": "sum_brokers_agg"}, # Name is same for serverless
            {"name": "ClientConnectionCount", "aggregation_type": "sum_brokers_agg"},
            {"name": "ConnectionCloseRate", "aggregation_type": "sum_brokers_agg"},
            {"name": "ConnectionCreationRate", "aggregation_type": "sum_brokers_agg"},
            {"name": "RequestBytesMean", "aggregation_type": "avg_brokers_peak_max_agg"},
        ]

        latest_value_metric_names = [mc['name'] for mc in metric_configs_for_cols if mc['aggregation_type'] == 'cluster_latest_value']
        avg_peak_metric_names = [mc['name'] for mc in metric_configs_for_cols if mc['aggregation_type'] != 'cluster_latest_value']
        
        metric_keys_avg = [f'{m}_Avg' for m in avg_peak_metric_names]
        metric_keys_peak = [f'{m}_Peak' for m in avg_peak_metric_names]
        
        column_order = [
            'AccountID', 'Region', 'ClusterName', 'ClusterArn', 'ClusterType', 'CreationTime',
            'KafkaVersion', 'NumberOfBrokerNodes', 'BrokerInstanceType', 
            'NumberOfAvailabilityZones', 'StoragePerBrokerGB', 'Authentication',
            *latest_value_metric_names,
            *metric_keys_avg, 
            *metric_keys_peak
        ]

        # Ensure all columns exist in the DataFrame, adding them with None if missing
        for col in column_order:
            if col not in df.columns:
                df[col] = None

        df = df[column_order]

        current_time_for_filename = datetime.now()
        month_abbr_upper = current_time_for_filename.strftime("%b").upper()
        timestamp_str = current_time_for_filename.strftime(f"%Y-{month_abbr_upper}-%d_%H-%M-%S")
        output_file = f"{OUTPUT_CSV_FILE_BASE}_{timestamp_str}.csv"

        try:
            df.to_csv(output_file, index=False)
            print(f"\nReport successfully generated: {output_file}")
        except Exception as e:
            print(f"\nError writing CSV file: {e}")
    else:
        print("\nNo MSK cluster data was found or collected across the scanned regions.")

    end_script_time = datetime.now()
    print(f"Script finished in {end_script_time - start_script_time}")

if __name__ == '__main__':
    main()
