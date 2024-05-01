import time
import sys
import dispy
import socket
import math
import shutil
import os
from tqdm import tqdm

file_path = "/usb/data1.set"
N = int(sys.argv[1]) # Total amount of numbers to sort
chunk_size = int(sys.argv[2])
n = math.ceil(N/chunk_size) # number of chunks
# n = int(sys.argv[2]) # Number of divisions
nodes = ["192.168.10.10", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

# checks if end of file
def checkRead(num):
    if num != '':
        num = int(num)
    return num

# takes in chunk number, reads the chunk and bubble sorts
def chunk_and_bubble(chunk_num ,file_path):
    file_size = os.path.getsize(file_path)
    def get_chunk(chunk_number, file_path, file_size):
        start_pos = chunk_size * chunk_number
        end_pos = chunk_size * (chunk_number + 1)
        with open(file_path, 'r') as f:
            if chunk_number != 0:
                f.seek(start_pos)
                byte = f.read(1)
                while not byte == "\n":
                    start_pos += 1
                    f.seek(start_pos)
                    byte = f.read(1)
                start_pos += 1
    
            if end_pos >= file_size:
                end_pos = file_size
            else:
                f.seek(end_pos)
                byte = f.read(1)
                while not byte == "\n":
                    end_pos += 1
                    f.seek(end_pos)
                    byte = f.read(1)
                end_pos += 1
    
            f.seek(start_pos)
            arr = []
            while f.tell() <= end_pos:
                arr.append(int(f.readline()))
            return arr
    def bubble(arr, path):
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

    return bubble(arr = get_chunk(chunk_num, file_path, file_size), path = f"/mnt/shared/sorting/segment{chunk_num}.txt")

def main():
    print(file_path)
    total_numbers = 0
    jobs = []
    start_time = time.time()
    cluster = dispy.JobCluster(chunk_and_bubble, nodes = nodes, host = "192.168.10.1")
    for chunk_num in range(n):
        job = cluster.submit(chunk_num, file_path = f"/mnt{file_path}")
        jobs.append(job)

    stored_path = "/mnt/shared/sorting/final.txt"
    temp_path = "/mnt/shared/sorting/temp.txt"
    open(stored_path, 'w').close()
    open(temp_path, 'w').close()
    for job in tqdm(jobs):
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

if __name__ == "__main__":
    main()
