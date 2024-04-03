import time
import sys
import dispy
import socket
import math
import shutil
import os

file_path = "/usb/data1.set"
N = int(sys.argv[1]) # Total amount of numbers to sort
n = int(sys.argv[2]) # Number of divisions
nodes = ["192.168.10.10", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

def checkRead(num):
    if num != '':
        num = int(num)
    return num

def bubble(path):
    def checkRead2(num):
        if num != '':
            num = int(num)
        return num
    arr =[]
    with open(path, 'r') as file:
        num = checkRead2(file.readline())
        arr.append(num)
        while num:
            num = checkRead2(file.readline())
            arr.append(num)
            
    length = len(arr)
    hostname = socket.gethostname()
    for i in range(length - 1):
        swapped = False
        for j in range(0, length - i - 1):
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
        if not swapped:
            with open(path, 'w') as file:
                for num in arr:
                    file.write(f"{num}\n")
            return (path, hostname)

    with open(path, 'w') as file:
        for num in arr:
            file.write(f"{num}\n")
    return (path, hostname)

def main():
    cluster = dispy.JobCluster(bubble, nodes = nodes, host = "192.168.10.1")

    print(file_path)
    numbers = []
    jobs = []
    index = 0
    start_time = time.time()
    with open(file_path, 'r') as input:
        for x in range(N):
            if x % math.ceil(N/n) == 0 and not x == 0:
                index += 1
            num = int(input.readline())
            with open(f"/mnt/shared/sorting/segment{index}.txt", 'a') as file:
                # print(f"/mnt/shared/sorting/segment{index}.txt: {num}")
                file.write(f"{num}\n")
                file.flush()
    for i in range(index + 1):
        job = cluster.submit(f"/mnt/shared/sorting/segment{i}.txt")
        jobs.append(job)
                
    reading_time = (time.time() - start_time) / 60
    print(f"Reading took {reading_time} minutes")

    stored_path = "/mnt/shared/sorting/final.txt"
    temp_path = "/mnt/shared/sorting/temp.txt"
    for job in jobs:
        segment_path, host = job()
        print(f'{host} executed job {job.id}')
        # Recombine segment_path into stored_path
        with open(temp_path, 'r') as temp, open(segment_path, 'r') as segment, open(stored_path, 'w') as output:
            num1 = checkRead(temp.readline())
            num2 = checkRead(segment.readline())
            while num1 and num2:
                if num1 <= num2:
                    output.write(f"{num1}\n")
                    num1 = checkRead(temp.readline())
                else:
                    output.write(f"{num2}\n")
                    num2 = checkRead(segment.readline())
            while num1:
                output.write(f"{num1}\n")
                num1 = checkRead(temp.readline())
            while num2:
                output.write(f"{num2}\n")
                num2 = checkRead(segment.readline())
        shutil.copyfile(stored_path, temp_path)
    
    cluster.print_status()
    print(f"Reading took {reading_time} minutes")
    print(f"Sorting {N} numbers with {n} subdivisions took {(time.time() - start_time)/60} minutes")
    open("/mnt/shared/sorting/temp.txt", 'w').close()

if __name__ == "__main__":
    main()
