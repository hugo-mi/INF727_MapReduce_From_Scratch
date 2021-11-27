import os
import pandas as pd
import socket

df_list_reduced = list()

# Get filename in the shufflesreceived folder
path_shuffled_folder = "/tmp/hmichel-20/shufflesreceived"
path_reduced_folder = "/tmp/hmichel-20/reduces"
list_shuffle_received_files = os.listdir(path_shuffled_folder)
for file in list_shuffle_received_files:
    df_file = pd.read_csv(path_shuffled_folder + "/" + file)
    df_file.drop(['Hashcode','Num_Machine'], axis=1, inplace=True)
    #df_file_reduced = df_file.groupby('Word', as_index=False).sum()
    df_list_reduced.append(df_file)
    
df_reduced = pd.concat(df_list_reduced)
df_reduced = df_reduced.groupby('Word', as_index=False).sum()
df_reduced.to_csv(path_reduced_folder + "/" + socket.gethostname() +".csv", header=True, index=None, sep=',', mode='w')
