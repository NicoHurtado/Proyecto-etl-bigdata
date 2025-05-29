import boto3
import json
import time
from datetime import datetime
import logging
import subprocess
import os
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

class ProjectOrchestrator:
    
    EMR_RELEASE_LABEL = 'emr-6.15.0'
    MASTER_INSTANCE_TYPE = 'm5.xlarge'
    CORE_INSTANCE_TYPE = 'm4.xlarge' # Changed from m5.xlarge in example to m5.large
    CORE_INSTANCE_COUNT = 2
    KEEP_JOB_FLOW_ALIVE = True # Set to False if you want the cluster to terminate after steps
    TERMINATION_PROTECTED = False

    def __init__(self, config_path='config/buckets.json', aws_region=None):
        self.aws_region = aws_region if aws_region else os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        logger.info(f"Using AWS region: {self.aws_region}")
        self.emr_client = boto3.client('emr', region_name=self.aws_region)
        self.s3_client = boto3.client('s3', region_name=self.aws_region) # Ensure S3 client also uses the region
        self.config_path = config_path
        self.buckets = self._load_config()
        self._validate_bucket_config()

    def _load_config(self):
        try:
            logger.info(f"Loading configuration from {self.config_path}")
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            logger.info("Configuration loaded successfully.")
            # Log loaded bucket paths for verification
            # for key, value in config.items():
            #     logger.debug(f"Loaded bucket config - {key}: {value}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from configuration file: {self.config_path}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while loading config: {e}")
            raise

    def _validate_bucket_config(self):
        """Validates that S3 paths in config are well-formed."""
        required_buckets = [
            'raw_api_bucket', 'raw_db_bucket', 
            'trusted_processed_bucket', 'refined_predictions_bucket', 
            'scripts_bucket', 'logs_bucket'
        ]
        for rb_key in required_buckets:
            if rb_key not in self.buckets:
                msg = f"Missing required S3 path key in config: '{rb_key}'"
                logger.error(msg)
                raise ValueError(msg)
            
            s3_path = self.buckets[rb_key]
            if not s3_path.startswith("s3://"):
                msg = f"Invalid S3 path format for '{rb_key}': '{s3_path}'. Must start with s3://"
                logger.error(msg)
                raise ValueError(msg)
            
            # Further parsing to get bucket name and prefix
            parsed_bucket, parsed_prefix = self._parse_s3_path(s3_path, rb_key)
            if not parsed_bucket:
                msg = f"Could not parse bucket name from '{rb_key}': '{s3_path}'"
                logger.error(msg)
                raise ValueError(msg)
            logger.info(f"Validated S3 path for '{rb_key}': bucket='{parsed_bucket}', prefix='{parsed_prefix}'")

    def _parse_s3_path(self, s3_path, config_key_name=""):
        """Parses s3://bucket/prefix into (bucket, prefix). Prefix includes trailing / if present."""
        if not s3_path.startswith("s3://"):
            logger.warning(f"S3 path for {config_key_name} '{s3_path}' does not start with s3://. Cannot parse.")
            return None, None
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket_name = parts[0]
        key_prefix = parts[1] if len(parts) > 1 else ""
        # if key_prefix and not key_prefix.endswith('/'):
        #     key_prefix += '/' # Ensure prefix ends with / if it's a folder-like prefix
        return bucket_name, key_prefix

    def _run_local_script(self, script_path, script_args=None, cwd=None):
        """Executes a local Python script using subprocess."""
        if script_args is None:
            script_args = []
        command = [sys.executable, script_path] + script_args
        logger.info(f"Executing local script: {' '.join(command)} {'in CWD: '+cwd if cwd else ''}")
        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd)
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                logger.info(f"Script {script_path} executed successfully.")
                logger.debug(f"Script {script_path} STDOUT:\n{stdout}")
                if stderr:
                    logger.warning(f"Script {script_path} STDERR:\n{stderr}")
                return True
            else:
                logger.error(f"Script {script_path} failed with return code {process.returncode}.")
                logger.error(f"Script {script_path} STDOUT:\n{stdout}")
                logger.error(f"Script {script_path} STDERR:\n{stderr}")
                return False
        except FileNotFoundError:
            logger.error(f"Local script not found: {script_path}")
            return False
        except Exception as e:
            logger.error(f"Error executing local script {script_path}: {e}")
            return False

    def run_ingestion_stage(self):
        """Runs the data ingestion scripts."""
        logger.info("====== Starting Ingestion Stage ======")
        
        # Get the directory of the current orchestrator script
        orchestrator_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Run Open-Meteo API fetcher
        # Assumes open_meteo_fetcher.py is in a 'capture' subdirectory relative to orchestrator
        api_fetcher_script = os.path.join(orchestrator_dir, 'capture', 'open_meteo_fetcher.py')
        logger.info("Running Open-Meteo API data ingestion...")
        api_success = self._run_local_script(api_fetcher_script, cwd=os.path.join(orchestrator_dir, 'capture'))
        if not api_success:
            logger.error("Open-Meteo API ingestion failed. Halting pipeline.")
            raise RuntimeError("Open-Meteo API ingestion failed.")
        logger.info("Open-Meteo API data ingestion completed.")

        # Run Database fetcher
        # Assumes db_fetcher.py is in a 'capture' subdirectory relative to orchestrator
        db_fetcher_script = os.path.join(orchestrator_dir, 'capture', 'db_fetcher.py')
        logger.info("Running Database data ingestion...")
        db_success = self._run_local_script(db_fetcher_script, cwd=os.path.join(orchestrator_dir, 'capture'))
        if not db_success:
            logger.error("Database ingestion failed. Halting pipeline.")
            raise RuntimeError("Database ingestion failed.")
        logger.info("Database data ingestion completed.")
        
        logger.info("====== Ingestion Stage Completed Successfully ======")
        return True

    def _create_and_upload_bootstrap_script(self):
        logger.info("Creating and uploading EMR bootstrap script...")
        scripts_s3_full_path = self.buckets['scripts_bucket']
        scripts_bucket_name, scripts_base_prefix = self._parse_s3_path(scripts_s3_full_path, 'scripts_bucket')
        
        bootstrap_script_content = """#!/bin/bash
sudo python3 -m pip install --upgrade pip
sudo pip3 install pandas boto3 mysql-connector-python scikit-learn
# Add any other packages required by your Spark jobs
"""
        # Ensure base prefix for scripts ends with a slash if it's not empty
        scripts_base_prefix = scripts_base_prefix.rstrip('/') + '/' if scripts_base_prefix else ''
        bootstrap_s3_key = f"{scripts_base_prefix}bootstrap/install_packages.sh"

        try:
            self.s3_client.put_object(
                Bucket=scripts_bucket_name,
                Key=bootstrap_s3_key,
                Body=bootstrap_script_content,
                ContentType='text/x-shellscript'
            )
            bootstrap_s3_uri = f"s3://{scripts_bucket_name}/{bootstrap_s3_key}"
            logger.info(f"Bootstrap script uploaded to {bootstrap_s3_uri}")
            return bootstrap_s3_uri
        except Exception as e:
            logger.error(f"Failed to upload bootstrap script to s3://{scripts_bucket_name}/{bootstrap_s3_key}: {e}")
            raise

    def _upload_spark_scripts_to_s3(self):
        """Uploads Spark job scripts to the S3 scripts bucket."""
        logger.info("Uploading Spark scripts to S3...")
        scripts_s3_full_path = self.buckets['scripts_bucket']
        scripts_bucket_name, scripts_base_prefix = self._parse_s3_path(scripts_s3_full_path, 'scripts_bucket')
        scripts_base_prefix = scripts_base_prefix.rstrip('/') + '/' if scripts_base_prefix else ''

        orchestrator_dir = os.path.dirname(os.path.abspath(__file__))
        local_spark_jobs_dir = os.path.join(orchestrator_dir, 'scripts', 'spark_jobs')

        if not os.path.isdir(local_spark_jobs_dir):
            logger.error(f"Local Spark scripts directory not found: {local_spark_jobs_dir}")
            raise FileNotFoundError(f"Local Spark scripts directory not found: {local_spark_jobs_dir}")

        uploaded_script_paths = {}
        for filename in os.listdir(local_spark_jobs_dir):
            if filename.endswith(".py"):
                local_file_path = os.path.join(local_spark_jobs_dir, filename)
                s3_key = f"{scripts_base_prefix}{filename}" # Upload directly under scripts_base_prefix
                try:
                    self.s3_client.upload_file(local_file_path, scripts_bucket_name, s3_key)
                    script_s3_uri = f"s3://{scripts_bucket_name}/{s3_key}"
                    logger.info(f"Uploaded {filename} to {script_s3_uri}")
                    uploaded_script_paths[filename] = script_s3_uri
                except Exception as e:
                    logger.error(f"Failed to upload {filename} to s3://{scripts_bucket_name}/{s3_key}: {e}")
                    raise
        if not uploaded_script_paths:
            logger.warning(f"No Spark scripts found or uploaded from {local_spark_jobs_dir}")
            # Depending on requirements, this could be an error
        logger.info("Spark scripts upload completed.")
        return uploaded_script_paths

    def create_emr_cluster(self, bootstrap_s3_path):
        logger.info(f"Creating EMR cluster with bootstrap script: {bootstrap_s3_path}")
        cluster_name = f"WeatherAnalyticsCluster-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        logs_s3_full_path = self.buckets['logs_bucket']
        logs_bucket_name, logs_base_prefix = self._parse_s3_path(logs_s3_full_path, 'logs_bucket')
        emr_log_uri = f"s3://{logs_bucket_name}/{logs_base_prefix.rstrip('/') + '/' if logs_base_prefix else ''}emr_logs/"
        logger.info(f"EMR Log URI will be: {emr_log_uri}")

        cluster_config = {
            'Name': cluster_name,
            'LogUri': emr_log_uri,
            'ReleaseLabel': self.EMR_RELEASE_LABEL,
            'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}, {'Name': 'Hive'}, {'Name': 'Livy'}],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': 'MasterNode',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': self.MASTER_INSTANCE_TYPE,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'CoreNodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': self.CORE_INSTANCE_TYPE,
                        'InstanceCount': self.CORE_INSTANCE_COUNT,
                    }
                ],
                'Ec2KeyName': 'nicojaco-ec2-key-pair',
                'KeepJobFlowAliveWhenNoSteps': self.KEEP_JOB_FLOW_ALIVE,
                'TerminationProtected': self.TERMINATION_PROTECTED,
            },
            'ServiceRole': 'EMR_DefaultRole', # Ensure this role exists and has permissions
            'JobFlowRole': 'EMR_EC2_DefaultRole', # Ensure this role exists and has permissions
            'VisibleToAllUsers': True,
            'BootstrapActions': [
                {
                    'Name': 'InstallCustomPackages',
                    'ScriptBootstrapAction': {'Path': bootstrap_s3_path}
                }
            ],
            'Configurations': [
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.sql.adaptive.enabled': 'true',
                        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
                        # 'spark.sql.hive.metastore.version': '2.3.9' # Usually not needed for emr-6.x+
                    }
                }
            ]
        }

        try:
            response = self.emr_client.run_job_flow(**cluster_config)
            cluster_id = response['JobFlowId']
            logger.info(f"EMR cluster creation initiated. Name: {cluster_name}, ID: {cluster_id}")
            return cluster_id
        except Exception as e:
            logger.error(f"Failed to create EMR cluster: {e}")
            raise

    def _wait_for_cluster_ready(self, cluster_id):
        logger.info(f"Waiting for EMR cluster {cluster_id} to become ready (state: WAITING)...")
        waiter = self.emr_client.get_waiter('cluster_running') # Waits for RUNNING or WAITING
        try:
            waiter.wait(
                ClusterId=cluster_id,
                WaiterConfig={'Delay': 60, 'MaxAttempts': 30} # Check every 60s, max 30 mins
            )
            cluster_description = self.emr_client.describe_cluster(ClusterId=cluster_id)
            status = cluster_description['Cluster']['Status']['State']
            logger.info(f"EMR cluster {cluster_id} is now ready. Current status: {status}")
            if status not in ['WAITING', 'RUNNING']:
                 raise RuntimeError(f"EMR cluster {cluster_id} ended up in state {status}, not WAITING/RUNNING.")
        except Exception as e:
            logger.error(f"Error waiting for EMR cluster {cluster_id} to be ready: {e}")
            try: # Log current state if waiter fails
                cluster_description = self.emr_client.describe_cluster(ClusterId=cluster_id)
                logger.error(f"Last known status of cluster {cluster_id}: {cluster_description['Cluster']['Status']['State']}")
            except Exception as desc_e:
                logger.error(f"Could not describe cluster {cluster_id} after wait error: {desc_e}")
            raise
    
    def terminate_emr_cluster(self, cluster_id):
        logger.info(f"Attempting to terminate EMR cluster: {cluster_id}")
        try:
            self.emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
            logger.info(f"EMR cluster {cluster_id} termination initiated.")
        except Exception as e:
            logger.error(f"Failed to terminate EMR cluster {cluster_id}: {e}")
            # Not raising here, as it might be called in a finally block

    def _submit_emr_step(self, cluster_id, step_name, script_s3_path, script_args):
        """Submits a Spark job as a step to an EMR cluster."""
        logger.info(f"Submitting EMR step: {step_name} (Script: {script_s3_path}, Args: {script_args})")
        full_args = ['spark-submit', '--deploy-mode', 'cluster', script_s3_path] + script_args
        step_config = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE', 
            'HadoopJarStep': {'Jar': 'command-runner.jar', 'Args': full_args}
        }
        try:
            response = self.emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step_config])
            step_id = response['StepIds'][0]
            logger.info(f"EMR step {step_name} submitted. Step ID: {step_id}")
            return step_id
        except Exception as e:
            logger.error(f"Failed to submit EMR step {step_name}: {e}")
            raise

    def _wait_for_step_completion(self, cluster_id, step_id, step_name):
        """Waits for an EMR step to complete."""
        logger.info(f"Waiting for EMR step '{step_name}' (ID: {step_id}) to complete...")
        waiter = self.emr_client.get_waiter('step_complete')
        try:
            waiter.wait(ClusterId=cluster_id, StepId=step_id, WaiterConfig={'Delay': 30, 'MaxAttempts': 120})
            status_response = self.emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
            status = status_response['Step']['Status']['State']
            logger.info(f"EMR step '{step_name}' (ID: {step_id}) finished with status: {status}")
            if status != 'COMPLETED':
                f_details = status_response['Step']['Status'].get('FailureDetails', {})
                reason = f_details.get('Reason', 'N/A')
                log_file = f_details.get('LogFile', 'N/A')
                message = f_details.get('Message', 'N/A')
                logger.error(f"Step Failure Details: Reason: {reason}, LogFile: {log_file}, Message: {message}")
                raise RuntimeError(f"EMR step {step_name} failed (Status: {status}). Reason: {reason}")
            return status
        except Exception as e:
            logger.error(f"Error waiting for EMR step '{step_name}' (ID: {step_id}): {e}")
            try:
                status_response = self.emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
                logger.error(f"Last known status for step {step_id}: {status_response['Step']['Status']['State']}")
            except Exception as desc_e:
                logger.error(f"Could not retrieve status for step {step_id} after wait error: {desc_e}")
            raise

    def run_etl_stage(self, cluster_id, spark_script_s3_paths):
        logger.info("====== Starting ETL Stage ======")
        script_filename = 'weather_etl.py'
        script_s3_path = spark_script_s3_paths.get(script_filename)
        if not script_s3_path:
            raise ValueError(f"S3 path for ETL script '{script_filename}' not found in uploaded scripts.")
        
        args = [
            self.buckets['raw_api_bucket'], 
            self.buckets['raw_db_bucket'],
            self.buckets['trusted_processed_bucket']
        ]
        etl_step_id = self._submit_emr_step(cluster_id, "WeatherETLSparkJob", script_s3_path, args)
        status = self._wait_for_step_completion(cluster_id, etl_step_id, "WeatherETLSparkJob")
        logger.info(f"====== ETL Stage Completed with status: {status} ======")
        return status == 'COMPLETED'

    def run_analysis_stage(self, cluster_id, spark_script_s3_paths):
        logger.info("====== Starting Analysis Stage ======")
        script_filename = 'weather_analysis.py'
        script_s3_path = spark_script_s3_paths.get(script_filename)
        if not script_s3_path:
            raise ValueError(f"S3 path for Analysis script '{script_filename}' not found in uploaded scripts.")

        args = [
            self.buckets['trusted_processed_bucket'],
            self.buckets['refined_predictions_bucket']
        ]
        analysis_step_id = self._submit_emr_step(cluster_id, "WeatherAnalysisSparkJob", script_s3_path, args)
        status = self._wait_for_step_completion(cluster_id, analysis_step_id, "WeatherAnalysisSparkJob")
        logger.info(f"====== Analysis Stage Completed with status: {status} ======")
        return status == 'COMPLETED'

    def run_full_pipeline(self, cluster_name_prefix="WeatherAnalytics"):
        logger.info(f"🚀🚀🚀 Starting Full Weather Data Pipeline (EMR cluster: {cluster_name_prefix}) 🚀🚀🚀")
        start_time = datetime.now()
        cluster_id = None # Initialize cluster_id
        
        try:
            # 0. Create and Upload Bootstrap Script
            bootstrap_s3_uri = self._create_and_upload_bootstrap_script()

            # 1. Run local ingestion scripts
            self.run_ingestion_stage()
            
            # 2. Upload Spark scripts to S3
            spark_s3_paths = self._upload_spark_scripts_to_s3()
            if not spark_s3_paths.get('weather_etl.py') or not spark_s3_paths.get('weather_analysis.py'):
                raise RuntimeError("Essential Spark scripts (ETL or Analysis) failed to upload or were not found.")

            # 3. Create EMR cluster
            cluster_id = self.create_emr_cluster(bootstrap_s3_path=bootstrap_s3_uri)
            self._wait_for_cluster_ready(cluster_id)

            # 4. Run ETL Spark job on EMR
            if not self.run_etl_stage(cluster_id, spark_s3_paths):
                raise RuntimeError("ETL stage failed.")

            # 5. Run Analysis Spark job on EMR
            if not self.run_analysis_stage(cluster_id, spark_s3_paths):
                raise RuntimeError("Analysis stage failed.")
            
            end_time = datetime.now()
            logger.info(f"🎉🎉🎉 Full Weather Data Pipeline Completed Successfully on EMR cluster {cluster_id}! 🎉🎉🎉")
            logger.info(f"Total execution time: {end_time - start_time}")
            return {
                "status": "SUCCESS", "cluster_id": cluster_id,
                "total_duration_seconds": (end_time - start_time).total_seconds()
            }
        except Exception as e:
            end_time = datetime.now()
            logger.error(f"❌❌❌ Pipeline FAILED: {e} ❌❌❌")
            logger.error(f"Total execution time before failure: {end_time - start_time}")
            # If cluster was created and KeepJobFlowAliveWhenNoSteps is False, it might terminate on its own
            # Otherwise, or if failure was before/during cluster setup, manual cleanup might be needed.
            # If KEEP_JOB_FLOW_ALIVE is True, cluster will remain running.
            if cluster_id and self.KEEP_JOB_FLOW_ALIVE:
                logger.warning(f"Cluster {cluster_id} was left running due to KEEP_JOB_FLOW_ALIVE=True.")
            elif cluster_id and not self.KEEP_JOB_FLOW_ALIVE :
                 logger.info(f"Cluster {cluster_id} might terminate automatically if steps failed appropriately and configured to do so, or if it failed to start correctly.")
            return {
                "status": "FAILED", "cluster_id": cluster_id, "error_message": str(e),
                "total_duration_seconds": (end_time - start_time).total_seconds()
            }
        finally:
            # Optional: Terminate cluster if KEEP_JOB_FLOW_ALIVE is False
            if cluster_id and not self.KEEP_JOB_FLOW_ALIVE:
                logger.info(f"KEEP_JOB_FLOW_ALIVE is False. Initiating termination for cluster {cluster_id}.")
                # Add a small delay to allow EMR to process last step status before termination
                time.sleep(60) 
                self.terminate_emr_cluster(cluster_id)
            elif cluster_id and self.KEEP_JOB_FLOW_ALIVE:
                logger.info(f"KEEP_JOB_FLOW_ALIVE is True. Cluster {cluster_id} will remain running.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pipeline_orchestrator.py <emr_cluster_id> [config_file_path]")
        print("Example: python pipeline_orchestrator.py j-ABC123XYZ789 config/buckets.json")
        sys.exit(1)
    
    emr_cluster_id = sys.argv[1]
    config_file = sys.argv[2] if len(sys.argv) > 2 else 'config/buckets.json'

    logger.info(f"Initializing orchestrator with EMR Cluster ID: {emr_cluster_id} and Config: {config_file}")
    
    orchestrator = ProjectOrchestrator(config_path=config_file)
    
    results = orchestrator.run_full_pipeline(emr_cluster_id)
    
    logger.info("Pipeline Execution Summary:")
    print(json.dumps(results, indent=2))

    if results["status"] == "FAILED":
        sys.exit(1) # Exit with error code if pipeline failed 