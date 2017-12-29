import os
from os.path import isfile, join
from os import listdir
import sys
import subprocess
from joblib import Parallel, delayed



def get_different_files(files1, path1, files2, path2):
    only_1 = set(files1) - set(files2)
    only_2 = set(files2) - set(files1)
    different = []
   
    only_1 = list(only_1)
    only_2 = list(only_2)
    for f in only_1:
        different.append(join(path1, f))
    for f in only_2:
        different.append(join(path2, f))
    
    return list(set(different))

def get_common_larger_files(files1, path1, files2, path2):
    common = set(files1) & set(files2)
    larger_files = []
    for f in list(common):
        if os.stat(join(path1, f)).st_size >= os.stat(join(path2, f)).st_size:
            larger_files.append(join(path1, f))
        else:
            larger_files.append(join(path2, f))
    return list(set(larger_files))

def get_chunk_index(n, length):
    t = []
    start = 0
    s = length / n
    for i in xrange(0,n):
        stop = start + s
        if i == n-1:
            stop = length
        t.append((start, stop))
        start = stop
    return t

def run_mv_command(files, i, destination):
    
    tmp_file = "filenames_tmp_" + str(i) + ".txt"
    
    fp = open(tmp_file, 'w')
    
    for f in files:
        fp.write("%s\n" %f) 

    fp.close() 
    
    command_mv = "time cat " + tmp_file  + " " + "| " + "xargs mv -t " + str(destination)
    subprocess.call(command_mv, shell=True) 
    subprocess.call("rm -f " + tmp_file, shell=True)
    print "Exiting the program ... thread number : " + str(i)

if __name__ == "__main__":
   
    if len(sys.argv) < 4:
        raise Exception("Not enough arguments given !!\n arg1: directory_1\n arg2: directory_2\n arg3: destination")
 	sys.exit(1)

    path1 = sys.argv[1].strip()
    path2 = sys.argv[2].strip()
    
    number_chunks = 100
    destination = sys.argv[3].strip()  
    if not os.path.exists(path1):
        raise OSError(os.errno.ENOTDIR, os.strerror(os.errno.ENOTDIR), path1)
 	sys.exit(1)
    
    if not os.path.exists(path2):
        raise OSError(os.errno.ENOTDIR, os.strerror(os.errno.ENOTDIR), path2)
 	sys.exit(1)
    
    filename = "sync_tmp.txt" 
    files1 = [f for f in listdir(path1) if isfile(join(path1, f))]
    files2 = [f for f in listdir(path2) if isfile(join(path2, f))]

    all_files = []

    if len(files1) == 0 and len(files2) == 0:
	print "Nothing to Move !! "
 	sys.exit(0)
	
    
    all_files.extend( get_different_files(files1, path1, files2, path2) )
    #print len(all_files)
    all_files.extend( get_common_larger_files(files1, path1, files2, path2) )
    #print len(all_files)
    all_files = list(set(all_files))

    fp = open(filename, 'w')

    for f in all_files:
        fp.write("%s\n" %f)
    
    fp.close()
    print "Number of files to be moved : " + str(len(all_files))  
    print "Filenames to be moved saved in " + str(filename)
    print "-"*50 

    
    if not os.path.isdir(destination):
        print "Directory Not present. Creating new directory - " + destination
        subprocess.call("mkdir " + destination, shell=True)
    
    print "Running Move command. This might take a few moments. "
    print ">"*5 + "Do not Quit !! " + "<"*5
    
    index_tuples = get_chunk_index(number_chunks, len(all_files)) 

    Parallel(n_jobs=45)(delayed (run_mv_command)(all_files[t[0]:t[1]], i, destination) for i, t in enumerate(index_tuples)) 

    #command_mv = "time cat " + filename + " " + "| " + "xargs mv -t " + str(destination)

    #subprocess.call(command_mv, shell=True)

    subprocess.call("rm -f " + filename, shell=True)

    print "Exiting the program ... "
    sys.exit(0)
