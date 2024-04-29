import os.path
import sys


def get_chunk(filename, chunk_number, chunk_size):
    print("Getting chunk number {} from file {} with chunk size {}".format(chunk_number, filename, chunk_size))
    file_size = os.path.getsize(filename)
    start_pos = chunk_size * chunk_number
    end_pos = chunk_size * (chunk_number + 1)
    byte_offset = 0
    with open(filename, 'r') as f:
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
        while f.tell() < end_pos:
            print(f.readline(), end='')



args = sys.argv[1:]

if len(args) != 3:
    print("Usage: chunk <filename> <chunk_number> <chunk_size_in_bytes>")

filename = sys.argv[1]
if not os.path.exists(filename):
    print("File \"{}\" does not exist".format(filename))

try:
    chunk_number = int(sys.argv[2])
except ValueError:
    print("Error: chunk_number \"{}\" is not an integer".format(sys.argv[2]))

try:
    chunk_size = int(sys.argv[3])
except ValueError:
    print("Error: chunk_size \"{}\" is not an integer".format(sys.argv[3]))

if chunk_size < 10:
    print("Error: chunk_size must be > 10")

get_chunk(filename, chunk_number, chunk_size)
