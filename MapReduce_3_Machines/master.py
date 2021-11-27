import subprocess
import time
import pandas as pd

def split_input_file():
    df = pd.read_csv('/tmp/hmichel-20/input.txt', sep="\n", header=None)
    nb_line = df.shape[0]
    
    nb_machine = 3
    
    split_line = nb_line // nb_machine
    remaining_line = nb_line % nb_machine
    
    int1 = split_line
    int3 = split_line + split_line
    int4 = split_line + split_line + split_line + remaining_line
    
    df1 = df[0:int1]
    df2 = df[int1:int3]
    df3 = df[int3:int4]
    
    df1.to_csv(r'/tmp/hmichel-20/splits/s10.txt', header=None, index=None, sep='\n', mode='w')
    df2.to_csv(r'/tmp/hmichel-20/splits/s11.txt', header=None, index=None, sep='\n', mode='w')
    df3.to_csv(r'/tmp/hmichel-20/splits/s13.txt', header=None, index=None, sep='\n', mode='w')

def copy_split_files():
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        localPath = "/tmp/hmichel-20/splits/s"+str(i)+".txt"
        distantPath = "/tmp/hmichel-20/splits"
        proc = subprocess.Popen(["scp",localPath,login+"@"+machine+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in [0, 1, 2]:
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            
def start_map(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    print("\n------- STARTING MAPPING PHASE ... -------\n")
    
    for i in [0, 1, 2]:
        try:
            print("Start Mapping Phase for machine", str(i))
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
            print("Mapping Phase finished for machine", str(i))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            
    print("\n------- MAPPING PHASE FINISHED-------\n")
    
def start_shuffle(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    print("\n-------STARTING SHUFFLING PHASE ... -------\n")
    
    for i in [0, 1, 2]:
        try:
            print("Start Shuffling Phase for machine", str(i))
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
            print("Shuffling Phase finished for machine", str(i))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            
    print("\n------- SHUFFLING PHASE FINISHED -------\n")
    
def start_reduce(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    print("\n------- STARTING REDUCING PHASE ... -------\n")
    
    for i in [0, 1, 2]:
        try:
            print("Start Reducing Phase for machine", str(i))
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
            print("Reducing Phase finished for machine", str(i))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            
    print("\n------- REDUCING PHASE FINISHED -------\n")
    
def scp_reduce_result():
    listproc = []
    timer=5
    login="hmichel-20"
    path_file_reduce = "/tmp/hmichel-20/reduces"
    current_machine = "tp-1a201-12"
    local_path = "/tmp/hmichel-20/result"
    for i in [10, 11, 13]:
        distant_machine = "tp-1a201-"+str(i) 
        command = ["ssh", f"{login}@{distant_machine}", f"scp -r {path_file_reduce} {login}@{current_machine}:{local_path}",]
        "ssh" "hmichel-20@tp-1a201-11" "scp -r /tmp/hmichel-20/reduces hmichel-20@tp-1a201-12:/tmp/hmichel-20/results"
        proc = subprocess.Popen([command[0], command[1], command[2]],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)
    
    for i in [0, 1, 2]:
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
"""            
def print_result():
    listproc = []
    timer = 5
    reduce_path = "/tmp/hmichel-20/result/reduces"
    output_path = "/tmp/hmichel-20"
    result_file_list = os.listdir(reduce_path)
    for file in result_file_list:
        proc = subprocess.Popen(["cat", reduce_path + "/" + file, ">> "+ output_path + "/" + "result.txt"],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)
    
    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
"""

def print_final_result(command):
    timer=5
    login="hmichel-20"
    machine = "tp-1a201-12"
    proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)

    try:
        out, err = proc.communicate(timeout=timer)
        code = proc.returncode
        print("\n---------------------------------------\n")
        print("RESULT ==> ", out)
        print("\n---------------------------------------\n")
        print(err)
        print(code)
    except subprocess.TimeoutExpired:
        proc.kill()
        print(timer+" timeout")

split_input_file()

copy_split_files()

start_time1 = time.time()
start_map("python3 /tmp/hmichel-20/slave_mapper.py")
print(("MAPPING --- %s seconds ---\n" % (time.time() - start_time1)))

start_time2 = time.time()
start_shuffle("python3 /tmp/hmichel-20/slave_shuffler.py")
print(("SHUFFLING --- %s seconds ---\n" % (time.time() - start_time2)))

start_time3 = time.time()
start_reduce("python3 /tmp/hmichel-20/slave_reducer.py")
print(("REDUCING --- %s seconds ---\n" % (time.time() - start_time3)))

print(("Total execution time --- %s seconds ---\n" % (time.time() - start_time1)))

scp_reduce_result()

print_final_result("find /tmp/hmichel-20/result/reduces -name '*.txt' -exec cat {} \;")