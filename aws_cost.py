import boto3
from datetime import datetime, timedelta, date
import json
import psycopg2


mydb = psycopg2.connect(
    host="***********",
    database="********",
    user="*********",
    password="*********"
)

today= '2023-07-25'
yesterday= '2023-07-01'
# today = datetime.now().date()
# yesterday = today - taimedelta(days=1)


aws_access_key_id = '********'
aws_secret_access_key = '********'
client = boto3.client('ce',
                      #   region_name=region_name,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)


response = client.get_cost_and_usage(
    TimePeriod={
        'Start': str(yesterday),
        'End': str(today)
    },
    GroupBy=[
        {
            "Type": "DIMENSION",
            "Key": "SERVICE"
        },
        {
            "Type": "TAG",
            "Key": "Environment"
        }
    ],
    Granularity='DAILY',
    Metrics=["BlendedCost", "UnblendedCost", "UsageQuantity"]
)


# adding appropriate service
def checkService(service):
    if service == "AWS Key Management Service":
        return 'awsKeyManagementService'
    elif service == "AWS Lambda":
        return 'awsLambda'
    elif service == "AWS Secrets Manager":
        return 'awsSecretsManager'
    elif service == "Amazon API Gateway":
        return 'amazonAPIGateway'
    elif service == 'Amazon CloudFront':
        return 'amazonCloudFront'
    elif service == "Amazon EC2 Container Registry (ECR)":
        return 'amazonEC2ContainerRegistry'
    elif service == "EC2 - Other":
        return 'ec2_Other'
    elif service == "Amazon Elastic Compute Cloud - Compute":
        return 'amazonElasticComputeCloud_Compute'
    elif service == 'Amazon Elastic Container Service':
        return 'amazonElasticContainerService'
    elif service == "Amazon Elastic Container Service for Kubernetes":
        return 'amazonElasticContainerServiceforKubernetes'
    elif service == "Amazon Elastic Load Balancing":
        return 'amazonElasticLoadBalancing'
    elif service == "Amazon Relational Database Service":
        return 'amazonRelationalDatabaseService'
    elif service == "Amazon Simple Email Service":
        return 'amazonSimpleEmailService'
    elif service == "Amazon Simple Notification Service":
        return 'amazonSimpleNotificationService'
    elif service == "Amazon Simple Storage Service":
        return 'amazonSimpleStorageService'
    elif service == "AmazonCloudWatch":
        return 'amazonCloudWatch'
    elif service == "AWS Cost Explorer":
        return 'awsCostExplorer'
    elif service == 'Tax':
        return 'tax'


def sort(response):
    requestId = response['ResponseMetadata']['RequestId']
    # Modifying based on datetime format
    currTime = datetime.now()
    dt_string = currTime.strftime("%Y-%m-%d %H:%M:%S")
    for j in range(len(response['ResultsByTime'])):
        # Time duration
        start_date = response['ResultsByTime'][j]['TimePeriod']['Start']
        end_date = response['ResultsByTime'][j]['TimePeriod']['End']

        # Development environment dictionary
        productionCost = {
            "request_Id": requestId,
            "start_date": start_date,
            "end_date": end_date,
            "dateTime": dt_string,
            "environment": 'staging-v2',
            "awsKeyManagementService": 0,
            "awsLambda": 0,
            "awsSecretsManager": 0,
            "amazonAPIGateway": 0,
            "amazonCloudFront": 0,
            "amazonEC2ContainerRegistry": 0,
            "ec2_Other": 0,
            "amazonElasticComputeCloud_Compute": 0,
            "amazonElasticContainerService": 0,
            "amazonElasticContainerServiceforKubernetes": 0,
            "amazonElasticLoadBalancing": 0,
            "amazonRelationalDatabaseService": 0,
            "amazonSimpleEmailService": 0,
            "amazonSimpleNotificationService": 0,
            "amazonSimpleStorageService": 0,
            "amazonCloudWatch": 0,
            "awsCostExplorer": 0,
            "tax": 0
        }

        for i in response['ResultsByTime'][j]['Groups']:

            # Getting the service Name
            service = i['Keys'][0]
            # getting the environment
            environment = i['Keys'][1]

            # checking for production environment
            if environment == 'Environment$production':
                ser = checkService(service)

                productionCost[ser] += float(i['Metrics']['BlendedCost']['Amount'])
    
        cursor = mydb.cursor()
        insert = 'INSERT INTO aws_production_cost (request_id,start_date,end_date,request_time,environment,aws_key_management_service,aws_lambda,aws_secrets_manager,amazon_api_gateway,amazon_cloudfront,amazon_ec2_container_registry_ecr,ec2_other,amazon_elastic_compute_cloud_compute,amazon_elastic_container_service,amazon_elastic_container_service_for_kubernetes,amazon_elastic_load_balancer,amazon_relational_database_service,amazon_simple_email_service,amazon_simple_notification_service,amazon_simple_storage_service,amazon_cloudwatch,aws_cost_explorer,tax) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        insert_production_value = (
        productionCost['request_Id'], productionCost['start_date'], productionCost['end_date'], productionCost['dateTime'], productionCost['environment'],
        productionCost['awsKeyManagementService'], productionCost['awsLambda'], productionCost['awsSecretsManager'], productionCost['amazonAPIGateway'],
        productionCost['amazonCloudFront'], productionCost['amazonEC2ContainerRegistry'], productionCost['ec2_Other'],
        productionCost['amazonElasticComputeCloud_Compute'], productionCost['amazonElasticContainerService'],
        productionCost['amazonElasticContainerServiceforKubernetes'], productionCost['amazonElasticLoadBalancing'],
        productionCost['amazonRelationalDatabaseService'], productionCost['amazonSimpleEmailService'],
        productionCost['amazonSimpleNotificationService'], productionCost['amazonSimpleStorageService'], productionCost['amazonCloudWatch'],
        productionCost['awsCostExplorer'], productionCost['tax'])
        cursor.execute(insert, (insert_production_value))
        
        mydb.commit()
        print(f"Records Inserted in table")

sort(response)
