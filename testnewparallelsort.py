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
def get_chunk(chunk_number, path, size):
    start_pos = size * chunk_number
    end_pos = size * (chunk_number + 1)
    with open(path, 'r') as f:
        if chunk_number != 0:
            f.seek(start_pos)
            byte = f.read(1)
            while not byte == "\n":
                start_pos += 1
                f.seek(start_pos)
                byte = f.read(1)
            start_pos += 1

        if end_pos >= size:
            end_pos = size
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
        while f.tell() < end_pos:
            arr.append(int(f.readline()))
        return (chunk_number, path, size)
        return arr

def main():
    print(file_path)
    total_numbers = 0
    jobs = []
    start_time = time.time()
    cluster = dispy.JobCluster(get_chunk, nodes = nodes, host = "192.168.10.1")
    for chunk_num in range(n):
        job = cluster.submit(chunk_number = chunk_num, path = f"/mnt{file_path}", size = chunk_size)
        jobs.append(job)

    stored_path = "/mnt/shared/sorting/final.txt"
    temp_path = "/mnt/shared/sorting/temp.txt"
    open(stored_path, 'w').close()
    open(temp_path, 'w').close()
    for job in tqdm(jobs):
        a,b,c = job()
        print(a,b,c)
    
    cluster.print_status()

if __name__ == "__main__":
    main()
