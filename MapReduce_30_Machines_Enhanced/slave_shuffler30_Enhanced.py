import hashlib
import os
import pandas as pd
import subprocess
import socket

def hash_code(word):
    word_encoded = word.encode('utf-8')
    hashcode = str(int.from_bytes(hashlib.sha256(word_encoded).digest()[:4], 'little'))
    return hashcode
    
def num_machine(hashcode_int):
    nb_machines = 30
    hashcode_int = int(hashcode_int)
    no_machine = hashcode_int % nb_machines
    return no_machine

def compute_hash(df):
    df_shuffler["Hashcode"] = df_shuffler["Word"].apply(lambda x: hash_code(x))
    df_shuffler["Num_Machine"] = df_shuffler["Hashcode"].apply(lambda x: num_machine(x))
    return df_shuffler

def split_df_shuffler_per_machine(df):
   list_df_per_machine = list()
   num_mahcine_unique = df.Num_Machine.unique().tolist()
   for i in num_mahcine_unique:
       df_machine = df.loc[df["Num_Machine"] == i]
       list_df_per_machine.append(df_machine)
   return list_df_per_machine

def scp(localPath,distantPath, id_machine):
    timer=5
    login="hmichel-20"
    machine = "tp-1a201-"+str(id_machine)
    proc = subprocess.Popen(["scp",localPath,login+"@"+machine+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)

    try:
        out, err = proc.communicate(timeout=timer)
        code = proc.returncode
        print(str(machine)+" out: '{}'".format(out))
        print(str(machine)+" err: '{}'".format(err))
        print(str(machine)+" exit: {}".format(code))
    except subprocess.TimeoutExpired:
        proc.kill()
        print(str(machine)+" timeout")   

list_file = os.listdir("/tmp/hmichel-20/maps")
filename = "/tmp/hmichel-20/maps/"+list_file[0]
df_shuffler = pd.read_csv(filename)
df_shuffler_hashed = compute_hash(df_shuffler)
splitted_df = split_df_shuffler_per_machine(df_shuffler_hashed)

for df_shuffled in splitted_df:
    num_machine_df = df_shuffled.iloc[0,3]
    df_shuffled.to_csv(r"/tmp/hmichel-20/shuffles/" + str(num_machine_df) + "-" + socket.gethostname() +".csv", header=True, index=None, sep=',', mode='w')
    
local_path_file_shuffled = "/tmp/hmichel-20/shuffles"
distant_path_file_shuffled = "/tmp/hmichel-20/shufflesreceived"
list_file_shuffled = os.listdir(local_path_file_shuffled)
    
for file in list_file_shuffled:
    path_file_name_shuffled = local_path_file_shuffled + "/" + file
    id_machine = file.split("-tp")[0]
    scp(path_file_name_shuffled, distant_path_file_shuffled, id_machine)