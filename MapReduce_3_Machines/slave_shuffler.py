import hashlib
import socket
import os
import subprocess


def scp(localPath,distantPath, hashcode):
    timer=5
    login="hmichel-20"
    nb_machines = 3
    hashcode_int = int(hashcode)
    num_machine = hashcode_int % nb_machines
    if num_machine == 1:
        id_machine = 10
    elif num_machine == 2:
        id_machine = 11
    elif num_machine == 3:
        id_machine = 13
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