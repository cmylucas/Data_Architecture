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

def recomb(subarr):
    length = len(subarr)
    subarr = [subarr.copy()]
    index = 0
    for layer in range(math.ceil(math.log2(length))):
        nextlayer = []
        for x in range(0, len(subarr[index]), 2):
            if x == len(subarr[index])-1 and len(subarr[index]) % 2 == 1:
                nextlayer.append(subarr[index][x])
            else:
                key1 = subarr[index][x]
                key2 = subarr[index][x+1]
                nextlayer.append(join_two(key1, key2))
        subarr.append(nextlayer)
        index += 1
    return subarr[-1]

def write_array_to_file(path, arr):
    with open(path, 'w') as file:
        for number in arr:
            file.write(f"{number}\n")

def main():
    cluster = dispy.JobCluster(bubble, nodes = nodes, host = "192.168.10.1")

    print(file_path)
    numbers = []
    jobs = []
    start_time = time.time()
    with open(file_path, 'r') as file:
        for x in range(N):
            if x % int(N/n) == 0 and not x == 0:
                job = cluster.submit(numbers)
                jobs.append(job)
                numbers = []
            num = int(file.readline())
            numbers.append(num)
    print(f"Reading took {(time.time() - start_time) / 60} minutes")
    subarr =[]
    for job in jobs:
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
