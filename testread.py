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
n = int(N/chunk_size)
nodes = ["192.168.10.1", "192.168.10.10", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

def main():
    print(file_path)
    total_numbers = 0
    start_time = time.time()
    with open(file_path, 'r') as input:
        for index in range(n):
            open(f"/mnt/shared/sorting/segment{index}.txt", 'w').close()
            with open(f"/mnt/shared/sorting/segment{index}.txt", 'a') as file:
                for x in range(math.ceil(N/n)):
                    if total_numbers == N:
                        break
                    num = int(input.readline())
                    file.write(f"{num}\n")
                    total_numbers += 1
                file.close()
            index += 1
                
    reading_time = (time.time() - start_time) / 60
    print(f"Reading took {reading_time} minutes")


if __name__ == "__main__":
    main()
