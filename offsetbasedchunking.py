import os.path
import sys

def get_chunk(filename, chunk_number, chunk_size):
  print("Getting chunk number {} from file {} with chunk size {}".format(chunk_number, filename, chunk_size))

args = sys.argv[1:]

if len(args) != 3:
  print("Usage: chunk <filename> <chunk_number> <chunk_size_in_bytes>")

filename=sys.argv[1]
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
