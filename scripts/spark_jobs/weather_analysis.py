from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import sys

def create_spark_session(app_name="WeatherAnalysis"):
    """Creates and returns a SparkSession."""
    print("Initializing Spark session...")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_and_prepare_initial_data(spark, input_path):
    """Loads data from input_path, selects relevant columns, and drops NA values."""
    print(f"Loading data from {input_path}...")
    df = spark.read.parquet(input_path)
    
    # Select necessary columns and drop rows with any null values in these columns
    # These are the columns we'll use for descriptive analysis and modeling
    columns_for_analysis = ["precipitation_sum", "temperature_2m_max"]
    df_prepared = df.select(*columns_for_analysis).na.drop()
    print("Data loaded and initial preparation (selection, NA drop) complete.")
    df_prepared.printSchema()
    return df_prepared

def perform_descriptive_analysis(df):
    """Performs and prints descriptive analysis on the DataFrame."""
    print("\nPerforming descriptive analysis...")
    if df.isEmpty():
        print("DataFrame is empty. Skipping descriptive analysis.")
        return

    for column_name in df.columns:
        print(f"\nDescriptive statistics for column: {column_name}")
        df.select(col(column_name).cast("float")).describe().show() # Cast to float for numeric stats
    print("Descriptive analysis complete.\n")

def prepare_data_for_ml(df, original_label_col="temperature_2m_max", original_feature_col="precipitation_sum", new_label_col="label", new_feature_col_name="rain"):
    """Renames columns for ML model training."""
    print(f"Preparing data for ML: renaming {original_label_col} to {new_label_col} and {original_feature_col} to {new_feature_col_name}...")
    df_ml = df.withColumnRenamed(original_label_col, new_label_col) \
              .withColumnRenamed(original_feature_col, new_feature_col_name)
    print("ML data preparation (renaming) complete.")
    df_ml.printSchema()
    return df_ml

def engineer_features(df, input_cols, output_col="features"):
    """Assembles feature columns into a feature vector."""
    print(f"Engineering features: assembling {input_cols} into vector column '{output_col}'...")
    assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col, handleInvalid="skip") # Added handleInvalid
    df_vectorized = assembler.transform(df)
    print("Feature engineering complete.")
    df_vectorized.printSchema()
    return df_vectorized

def train_linear_regression_model(df, features_col="features", label_col="label"):
    """Trains a Linear Regression model."""
    print(f"Training Linear Regression model with features '{features_col}' and label '{label_col}'...")
    lr = LinearRegression(featuresCol=features_col, labelCol=label_col)
    model = lr.fit(df)
    print("Linear Regression model training complete.")
    # You can print coefficients and intercept if desired
    # print(f"Coefficients: {model.coefficients} Intercept: {model.intercept}")
    return model

def make_predictions(model, df):
    """Makes predictions using the trained model."""
    print("Making predictions...")
    predictions_df = model.transform(df)
    print("Predictions complete.")
    predictions_df.select("features", "label", "prediction").show(5)
    return predictions_df

def save_predictions(predictions_df, output_path, selected_cols=["features", "label", "prediction"]):
    """Saves the predictions to the specified output_path in JSON format."""
    print(f"Saving predictions ({selected_cols}) to {output_path}...")
    predictions_df.select(*selected_cols) \
                  .write.mode("overwrite") \
                  .json(output_path)
    print(f"Predictions saved successfully to {output_path}.")

def main(input_s3_path, predictions_s3_output_path):
    """Main function to orchestrate the weather analysis pipeline."""
    spark = create_spark_session()

    # 1. Load and prepare initial data
    prepared_df = load_and_prepare_initial_data(spark, input_s3_path)

    if prepared_df.isEmpty():
        print("No data available after loading and initial preparation. Exiting.")
        spark.stop()
        return

    # 2. Perform descriptive analysis
    perform_descriptive_analysis(prepared_df)

    # 3. Prepare data for ML (renaming)
    # Original columns from prepared_df are "precipitation_sum", "temperature_2m_max"
    ml_df = prepare_data_for_ml(prepared_df, 
                                original_label_col="temperature_2m_max",
                                original_feature_col="precipitation_sum",
                                new_label_col="label",
                                new_feature_col_name="rain")

    # 4. Engineer features
    # The input for VectorAssembler should be the new feature name
    vectorized_df = engineer_features(ml_df, input_cols=["rain"], output_col="features")

    # 5. Train model
    model = train_linear_regression_model(vectorized_df, features_col="features", label_col="label")

    # 6. Make predictions
    predictions_result_df = make_predictions(model, vectorized_df)

    # 7. Save results
    save_predictions(predictions_result_df, predictions_s3_output_path)

    print("Weather analysis and prediction pipeline finished successfully.")
    spark.stop()

if __name__ == "__main__":
    # S3 Paths (ensure these are correctly configured for your environment)
    DEFAULT_INPUT_PATH = "s3://weather-etl-trusted-nicojaco/processed_weather/"
    DEFAULT_PREDICTIONS_OUTPUT_PATH = "s3://weather-etl-refined-nicojaco/weather_predictions/"

    # Allow overriding S3 paths via command-line arguments for flexibility
    # This is useful when submitting Spark jobs
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_PATH
    predictions_output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_PREDICTIONS_OUTPUT_PATH
    
    print(f"Running weather analysis with Input: {input_path}, Output: {predictions_output_path}")
    
    main(input_path, predictions_output_path)
