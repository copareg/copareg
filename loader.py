from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Traffic Anomaly Detection").getOrCreate()

# Read network traffic data as DataFrame
df = spark.read.csv("ip.dat")

# Define thresholds for anomaly detection
high_bytes_threshold = 4096
short_duration_threshold = 100

# Filter for potential anomalies
anomalous_traffic = df.filter((df.bytes_transferred > high_bytes_threshold) & (df.duration_ms < short_duration_threshold))

# Select relevant columns
selected_data = anomalous_traffic.select("source_ip", "destination_ip", "bytes_transferred", "duration_ms")

# Define function to format output message
def format_message(row):
  return f"High byte transfer detected from source {row.source_ip} to destination {row.destination_ip} (bytes={row.bytes_transferred}) within a very short duration ({row.duration_ms}ms)"

# Apply function to each row and display results
anomalies = selected_data.rdd.map(format_message).collect()

# Print anomaly messages
if anomalies:
  print("Detected Anomalies:")
  for message in anomalies:
    print(message)
else:
  print("No anomalies detected.")

# Stop SparkSession
spark.stop()
