import time
import sys
import dispy
import socket
import math

file_path = "/usb/data1.set"
N = int(sys.argv[1]) # Total amount of numbers to sort
nodes = ["192.168.10.1", "192.168.10.10", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

def main():
    print(file_path)
    numbers = []
    start_time = time.time()
    with open(file_path, 'r') as file:
        for x in range(N):
            num = int(file.readline())
            numbers.append(num)
    print(f"Reading took {(time.time() - start_time) / 60} minutes")
    print(numbers)


if __name__ == "__main__":
    main()
