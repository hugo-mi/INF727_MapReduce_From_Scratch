import os

def slave_mapper(path_file):
    
    # Using readlines()
    num_file = path_file[24:26]
    file = open(path_file, 'r', encoding='utf-8')
    lines = file.readlines()
    for line in lines:
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        words = line.split()
        # increase counters
        for word in words:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            #
            # tab-delimited; the trivial word count is 1
            output_file = "/tmp/hmichel-20/maps/UM"+num_file+".txt"
            print ('%s\t%s' % (word, 1), file=open(output_file, "a"))

 
list_file = os.listdir("/tmp/hmichel-20/splits")
filename = "/tmp/hmichel-20/splits/"+list_file[0]
slave_mapper(filename)