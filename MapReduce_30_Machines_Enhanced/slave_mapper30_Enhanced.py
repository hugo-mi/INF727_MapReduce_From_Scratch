import os
import pandas as pd

def slave_mapper(path_file):
       
    # Using readlines()
    df_mapper = pd.DataFrame(columns = ["Word", "Count"])
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
            df_mapper = df_mapper.append({'Word': word, "Count": 1}, ignore_index=True)
    output_file = "/tmp/hmichel-20/maps/UM"+num_file+".txt"
    df_mapper.to_csv(output_file, header=True, index=None, sep=',', mode='w')

list_file = os.listdir("/tmp/hmichel-20/splits")
filename = "/tmp/hmichel-20/splits/"+list_file[0]
slave_mapper(filename)
