from pyspark.sql import DataFrame
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class SparkDataWriter:
    """
    Utility class for writing PySpark DataFrames to various data storage formats.

    It simplifies the PySpark write syntax, allowing users to specify the DataFrame,
    path, and write mode without manually chaining the .write.mode().csv() methods.
    """

    @staticmethod
    def write_to_csv(df: DataFrame, path: str, mode: str) -> None:

        """
        Writes a PySpark DataFrame to a CSV file at the specified path.

        Args:
            df (DataFrame): The PySpark DataFrame to be written.
            path (str): The destination path for the CSV file(s).
            mode (str, optional): The PySpark write mode. Valid modes include:
                'overwrite' (default): Overwrites the existing data/directory.
                'append': Adds the new data to the existing data.
                'ignore': Silently ignores the write if the path already exists.

                'error' or 'errorifexists': Throws an exception if the path exists.
        """
        # Define valid modes to provide helpful feedback if an invalid mode is passed
        valid_modes = [ "append", "ignore", "error"]

        if mode.lower() not in valid_modes:
            print(f"--- WARNING: Invalid write mode '{mode}' provided. Using default 'overwrite'.")
            mode = "overwrite"

        try:
            # The abstracted PySpark write command:
            # header=True ensures column names are included in the CSV file
            df.write.mode(mode.lower()).csv(path, header=True)
            print(f"--- SUCCESS: DataFrame successfully written to '{path}' in '{mode}' mode.")
        except Exception as e:
            print(f"--- FAILED to write DataFrame to CSV at '{path}'.")
            print(f"--- Error details: {e}")


# ==============================================================================
# Example Usage (Requires a running Spark Session)
# ==============================================================================

if __name__ == "__main__":
    # 1. Initialize Spark Session (Mock/Example setup)
    # NOTE: You would typically get the Spark session from your environment.
    try:
        spark = SparkSession.builder \
            .appName("SparkUtilityExample") \
            .master("local[*]") \
            .getOrCreate()
        print("\n--- Spark Session Initialized ---")
    except Exception as e:
        print(f"Could not initialize Spark Session. Please ensure PySpark is installed and configured. Error: {e}")
        exit()

    read_csv_df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(r"C:\Users\Kalya\PycharmProjects\SparkProjects\data\csv\employees_read_mode.csv")

    # # 2. Define Dummy Data and Schema
    # data = [("Alice", 34, "New York"), ("Bob", 45, "Los Angeles"), ("Charlie", 50, "Chicago")]
    # schema = StructType([
    #     StructField("name", StringType(), True),
    #     StructField("age", IntegerType(), True),
    #     StructField("city", StringType(), True)
    # ])

    # 3. Create Dummy DataFrame
    #employees_df = spark.createDataFrame(data, schema)
    #employees_df.printSchema()
   # employees_df.show()

    # 4. Define the target path (Use a temporary path for the example)
    output_path = r"C:\Kalyan Kumar Desktop\Documents\output_dataframe"
    write_mode = ("overwrite")

    # 5. Use the Utility Function (Write in overwrite mode)
    print("\n--- Running Write Example 1 (Overwrite) ---")
    SparkDataWriter.write_to_csv(
        df=read_csv_df,
        path=output_path,
        mode=write_mode
    )



    # 6. Use the Utility Function (Try to write in error mode when the file exists)
    # print("\n--- Running Write Example 2 (Overwrite) ---")
    # SparkDataWriter.write_to_csv(
    #     df=read_csv_df,
    #     path=output_path,
    #     mode=write_mode
    # )

    # 7. Clean up Spark Session4



    spark.stop()
