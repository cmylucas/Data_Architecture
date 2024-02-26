import time
import sys
import dispy

file_path = "/usb/data1.set"
#file_path = "/usb/data1.set"
N = int(sys.argv[1]) # Total amount of numbers to sort
n = int(sys.argv[2]) # Number of divisions
nodes = ["192.168.10.1", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

def bubble(arr):
    length = len(arr)
    for i in range(length - 1):
        swapped = False
        for j in range(0, length - i - 1):
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
        if not swapped:
            return arr
    return arr

def main():
    cluster = dispy.JobCluster(bubble, nodes = nodes, host = "192.168.10.1")

    print(file_path)
    numbers = []
    start_time = time.time()
    with open(file_path, 'r') as file:
        for x in range(N):
            if x % n == 0:
                job = cluster.submit(numbers)
                numbers = []
            num = int(file.readline())
            numbers.append(num)
    print(f"Reading took {(time.time() - start_time) / 60} minutes")
    
    jobs = []
    for i in range(20):
        job = cluster.submit(random.randint(5, 20))
        jobs.append(job)
    # cluster.wait() # waits until all jobs finish
    for job in jobs:
        host, n = job()  # waits for job to finish and returns results
        print('%s executed job %s at %s with %s' % (host, job.id, job.start_time, n))
        # other fields of 'job' that may be useful:
        # job.stdout, job.stderr, job.exception, job.ip_addr, job.end_time
    cluster.print_status()  # shows which nodes executed how many jobs etc.


if __name__ == "__main__":
    main()
