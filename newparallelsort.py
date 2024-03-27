import time
import sys
import dispy
import socket
import math

file_path = "/usb/data1.set"
N = int(sys.argv[1]) # Total amount of numbers to sort
n = int(sys.argv[2]) # Number of divisions
nodes = ["192.168.10.1", "192.168.10.10", "192.168.10.20", "192.168.10.30", "192.168.10.40"]

def join_two(key1, key2):
    ikey1 = 0
    ikey2 = 0
    a_res = []
    for x in range(0, len(key1) + len(key2)):
        # print("{} -> {} {}".format(x, ikey1, ikey2))
        if ikey1 >= len(key1) and ikey2 >= len(key2):
            break
        elif ikey1 >= len(key1):
            a_res += key2[ikey2:]
            break
        elif ikey2 >= len(key2):
            a_res += key1[ikey1:]
            break

        if key1[ikey1] > key2[ikey2]:
            a_res.append(key2[ikey2])
            ikey2 += 1
        elif key1[ikey1] < key2[ikey2]:
            a_res.append(key1[ikey1])
            ikey1 += 1
        else:
            a_res.append(key2[ikey2])
            ikey2 += 1
            a_res.append(key1[ikey1])
            ikey1 += 1
            x += 1

    return a_res

def bubble(arr):
    length = len(arr)
    hostname = socket.gethostname()
    for i in range(length - 1):
        swapped = False
        for j in range(0, length - i - 1):
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
        if not swapped:
            return (arr, hostname)
    return (arr, hostname)


def write_array_to_file(path, arr):
    with open(path, 'w') as file:
        for number in arr:
            file.write(f"{number}\n")

def main():
    cluster = dispy.JobCluster(bubble, nodes = nodes, host = "192.168.10.1")

    print(file_path)
    numbers = []
    jobs = []
    index = 0
    start_time = time.time()
    with open(file_path, 'r') as file:
        for x in range(N):
            if x % math.ceil(N/n) == 0 and not x == 0:
                index += 1
            with open(f"/mnt/shared/segment{index}.txt", 'a') as file:
                num = int(file.readline())
                file.write(f"{num}\n")
    for i in range(index + 1):
        job = cluster.submit(f"/mnt/shared/segment{i}.txt")
        jobs.append(job)
    print(f"Reading took {(time.time() - start_time) / 60} minutes")
    stored_path = "/mnt/shared/final.txt"
    temp_path = "/mnt/shared/temp.txt"
    for job in jobs:
        segment_path, host = job()
        print(f'{host} executed job {job.id}')
        # Recombine segment_path into stored_path
        with open(temp_path, r) as temp, open(segment_path, r) as segment, open(stored_path, w) as output:
            num1 = int(temp.readline())
            num2 = int(segment.readline())
            while num1 and num2:
                if num1 <= num2:
                    output.write(f"{num1}\n")
                    num1 = int(temp.readline())
                else:
                    output.write(f"{num2}\n")
                    num2 = int(temp.readline())
            while num1:
                output.write(f"{num1}\n")
                num1 = int(temp.readline())
            while num2:
                output.write(f"{num2}\n")
                num2 = int(temp.readline())
    
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
