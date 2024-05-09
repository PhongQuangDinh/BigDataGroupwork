import time

def simulate_stream(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            # Process the line (e.g., parse JSON, split CSV, etc.)
            data_point = line.strip()
            # Simulate processing time
            time.sleep(0.1)  # Adjust the delay as needed
            # Simulate streaming behavior
            yield data_point

# Example usage
file_path = '../data/taxi-data/part-2015-12-01-0000.csv'
stream = simulate_stream(file_path)
for data_point in stream:
    print(data_point)
