import threading
import time

# Function that each thread will execute
def task():
    print(f"Thread {threading.current_thread().name} started")
    time.sleep(5)  # Simulate some work
    print(f"Thread {threading.current_thread().name} finished")

# Function to create and run threads
def run_threads(thread_names):
    threads = []

    # Create and start threads
    for name in thread_names:
        thread = threading.Thread(target=task, name=name)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print(f"All threads in {thread_names} have completed")

# Run the first batch of 10 threads
# batch1 = [f"Batch1-Thread-{i}" for i in range(1, 11)]
# run_threads(batch1)

# # Run the second batch of 10 threads
# batch2 = [f"Batch2-Thread-{i}" for i in range(1, 11)]
# run_threads(batch2)

# print("All threads have completed")


def main():
    # List of numbers from 1 to 100
    numbers = list(range(1, 101))

    # Function to create batches
    def create_batches(data, batch_size):
        batches = []
        for i in range(0, len(data), batch_size):
            batches.append(data[i:i+batch_size])
        return batches

    # Create batches of size 10
    batches = create_batches(numbers, 10)

    # Print batches
    for batch in batches:
        run_threads(batch)

main()