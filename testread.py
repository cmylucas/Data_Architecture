import time
import sys
import dispy
import socket
import math

file_path = "/usb/data1.set"
N = int(sys.argv[1]) # Total amount of numbers to sort
n = int(sys.argv[2]) # Number of divisions
nodes = ["192.168.10.1", "192.168.10.10", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

def write_array_to_file(path, arr):
    with open(path, 'w') as file:
        for number in arr:
            file.write(f"{number}\n")

def main():
    print(file_path)
    numbers = []
    jobs = []
    start_time = time.time()
    with open(file_path, 'r') as file:
        for x in range(N):
            if x % math.ceil(N/n) == 0 and not x == 0:
                job = cluster.submit(numbers)
                jobs.append(job)
                numbers = []
            num = int(file.readline())
            numbers.append(num)
            if x == N-1:
                job = cluster.submit(numbers)
                jobs.append(job)
    print(f"Reading took {(time.time() - start_time) / 60} minutes")
    subarr =[]
    for job in jobs:
        print("new job")
        arr, host = job()
        subarr.append(arr)
        print(f'{host} executed job {job.id}')
        # print('%s executed job %s at %s with %s' % (host, job.id, job.start_time, n))
        # other fields of 'job' that may be useful:
        # job.stdout, job.stderr, job.exception, job.ip_addr, job.end_time
    cluster.print_status()
    sorting_time = time.time()
    print("Sorting separately finished")
    sortedlist = recomb(subarr)[0]
    print(f"Recombining took {(time.time() - sorting_time) / 60} minutes")
    # print(sortedlist)
    write_array_to_file("parallelsorted.txt", sortedlist)
    print(f"Sorting {N} numbers with {n} subdivisions took {(time.time() - start_time)/60} minutes")


if __name__ == "__main__":
    main()
