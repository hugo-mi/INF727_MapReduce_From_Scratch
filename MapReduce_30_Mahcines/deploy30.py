import subprocess

def ssh(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]:
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")

def scp(localPath,distantPath):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["scp",localPath,login+"@"+machine+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]:
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")    

ssh("mkdir -p /tmp/hmichel-20")
ssh("mkdir -p /tmp/hmichel-20/splits")
ssh("mkdir -p /tmp/hmichel-20/maps")
ssh("mkdir -p /tmp/hmichel-20/shuffles")
ssh("mkdir -p /tmp/hmichel-20/shufflesreceived")
ssh("mkdir -p /tmp/hmichel-20/reduces")
scp("./slave.py", "/tmp/hmichel-20")
scp("./slave_mapper30.py", "/tmp/hmichel-20")
scp("./slave_shuffler30.py", "/tmp/hmichel-20")
scp("./slave_reducer30.py", "/tmp/hmichel-20")
ssh("ls /tmp/hmichel-20")

