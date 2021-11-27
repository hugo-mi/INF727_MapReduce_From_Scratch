import hashlib
import socket
import os
import subprocess


def scp(localPath,distantPath, hashcode):
    timer=5
    login="hmichel-20"
    nb_machines = 30
    hashcode_int = int(hashcode)
    num_machine = hashcode_int % nb_machines
    if num_machine == 0:
        id_machine = 10
    elif num_machine == 1:
        id_machine = 11
    elif num_machine == 2:
        id_machine = 13
    elif num_machine == 3:
        id_machine = 14
    elif num_machine == 4:
        id_machine = 15    
    elif num_machine == 5:
        id_machine = 16
    elif num_machine == 6:
        id_machine = 17    
    elif num_machine == 7:
        id_machine = 18
    elif num_machine == 8:
        id_machine = 19    
    elif num_machine == 9:
        id_machine = 20
    elif num_machine == 10:
        id_machine = 21    
    elif num_machine == 11:
        id_machine = 22
    elif num_machine == 12:
        id_machine = 23    
    elif num_machine == 13:
        id_machine = 24
    elif num_machine == 14:
        id_machine = 25    
    elif num_machine == 15:
        id_machine = 26
    elif num_machine == 16:
        id_machine = 27    
    elif num_machine == 17:
        id_machine = 28
    elif num_machine == 18:
        id_machine = 29       
    elif num_machine == 19:
        id_machine = 30
    elif num_machine == 20:
        id_machine = 31    
    elif num_machine == 21:
        id_machine = 32
    elif num_machine == 22:
        id_machine = 33    
    elif num_machine == 23:
        id_machine = 34
    elif num_machine == 24:
        id_machine = 35    
    elif num_machine == 25:
        id_machine = 36
    elif num_machine == 26:
        id_machine = 37    
    elif num_machine == 27:
        id_machine = 38
    elif num_machine == 28:
        id_machine = 39   
    elif num_machine == 29:
        id_machine = 40    
 
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

def compute_hash(filepath):
    file = open(filepath, 'r', encoding='utf-8')
    lines = file.readlines()

    # compute de hash for each word
    for word in lines:
        word_encoded = word.encode('utf-8')
        hashcode = str(int.from_bytes(hashlib.sha256(word_encoded).digest()[:2], 'little'))
        filename_shuffled = "/tmp/hmichel-20/shuffles/" + hashcode + "-" + socket.gethostname() + ".txt"
        file_shuffled = open(filename_shuffled, "a")
        file_shuffled.write(word)
        file_shuffled.close()
    
    # copy the shuffle_file to the right machine
    
    local_path_file_shuffled = "/tmp/hmichel-20/shuffles"
    distant_path_file_shuffled = "/tmp/hmichel-20/shufflesreceived"
    list_file_shuffled = os.listdir(local_path_file_shuffled)
    
    for file in list_file_shuffled:
        path_file_name_shuffled = local_path_file_shuffled + "/" + file
        hashcode = file.split("-tp")[0]
        scp(path_file_name_shuffled, distant_path_file_shuffled, hashcode)

list_file = os.listdir("/tmp/hmichel-20/maps")
filename = "/tmp/hmichel-20/maps/"+list_file[0]
compute_hash(filename)