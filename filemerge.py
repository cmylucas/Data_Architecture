def checkRead(num):
    if num != '':
        num = int(num)
    return num

def main():
    index = 11
    jobs = []
    for i in range(index + 1):
        jobs.append(f"/mnt/shared/sorting/segment{i}.txt")

    stored_path = "/mnt/shared/sorting/final.txt"
    temp_path = "/mnt/shared/sorting/temp.txt"
    open(stored_path, 'w').close()
    open(temp_path, 'w').close()
    for job in tqdm(jobs):
        segment_path = job
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

if __name__ == "__main__":
    main()
