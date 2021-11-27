import subprocess
import time
import pandas as pd
import os

def split_input_file():
    df = pd.read_csv('/tmp/hmichel-20/input.txt', sep="\n", header=None)
    nb_line = df.shape[0]
    
    nb_machine = 30
    
    split_line = nb_line // nb_machine
    remaining_line = nb_line % nb_machine
    
    int1 = split_line
    int2 = 2 * split_line
    int3 = 3 * split_line 
    int4 = 4 * split_line 
    int5 = 5 * split_line
    int6 = 6 * split_line
    int7 = 7 * split_line
    int8 = 8 * split_line 
    int9 = 9 * split_line
    int10 = 10 * split_line
    int11 = 11 * split_line
    int12 = 12 * split_line
    int13 = 13 * split_line
    int14 = 14 * split_line
    int15 = 15 * split_line
    int16 = 16 * split_line
    int17 = 17 * split_line
    int18 = 18 * split_line
    int19 = 19 * split_line
    int20 = 20 * split_line
    int21 = 21 * split_line
    int22 = 22 * split_line
    int23 = 23 * split_line
    int24 = 24 * split_line
    int25 = 25 * split_line
    int26 = 26 * split_line
    int27 = 27 * split_line
    int28 = 28 * split_line
    int29 = 29 * split_line
    int30 = 30*(split_line) + remaining_line
    
    df1 = df[0:int1]
    df2 = df[int1:int2]
    df3 = df[int2:int3]
    df4 = df[int3:int4]
    df5 = df[int4:int5]
    df6 = df[int5:int6]
    df7 = df[int6:int7]
    df8 = df[int7:int8]
    df9 = df[int8:int9]
    df10 = df[int9:int10]
    df11 = df[int10:int11]
    df12 = df[int11:int12]
    df13 = df[int12:int13]
    df14 = df[int13:int14]
    df15 = df[int14:int15]
    df16 = df[int15:int16]
    df17 = df[int16:int17]
    df18 = df[int17:int18]
    df19 = df[int18:int19]
    df20 = df[int19:int20]
    df21 = df[int20:int21]
    df22 = df[int21:int22]
    df23 = df[int22:int23]
    df24 = df[int23:int24]
    df25 = df[int24:int25]
    df26 = df[int25:int26]
    df27 = df[int26:int27]
    df28 = df[int27:int28]
    df29 = df[int28:int29]
    df30 = df[int29:int30]
    
    df1.to_csv(r'/tmp/hmichel-20/splits/s10.txt', header=None, index=None, sep='\n', mode='w')
    df2.to_csv(r'/tmp/hmichel-20/splits/s11.txt', header=None, index=None, sep='\n', mode='w')
    df3.to_csv(r'/tmp/hmichel-20/splits/s13.txt', header=None, index=None, sep='\n', mode='w')
    df4.to_csv(r'/tmp/hmichel-20/splits/s14.txt', header=None, index=None, sep='\n', mode='w')
    df5.to_csv(r'/tmp/hmichel-20/splits/s15.txt', header=None, index=None, sep='\n', mode='w')
    df6.to_csv(r'/tmp/hmichel-20/splits/s16.txt', header=None, index=None, sep='\n', mode='w')
    df7.to_csv(r'/tmp/hmichel-20/splits/s17.txt', header=None, index=None, sep='\n', mode='w')
    df8.to_csv(r'/tmp/hmichel-20/splits/s18.txt', header=None, index=None, sep='\n', mode='w')
    df9.to_csv(r'/tmp/hmichel-20/splits/s19.txt', header=None, index=None, sep='\n', mode='w')
    df10.to_csv(r'/tmp/hmichel-20/splits/s20.txt', header=None, index=None, sep='\n', mode='w')
    df11.to_csv(r'/tmp/hmichel-20/splits/s21.txt', header=None, index=None, sep='\n', mode='w')
    df12.to_csv(r'/tmp/hmichel-20/splits/s22.txt', header=None, index=None, sep='\n', mode='w')    
    df13.to_csv(r'/tmp/hmichel-20/splits/s23.txt', header=None, index=None, sep='\n', mode='w')
    df14.to_csv(r'/tmp/hmichel-20/splits/s24.txt', header=None, index=None, sep='\n', mode='w')
    df15.to_csv(r'/tmp/hmichel-20/splits/s25.txt', header=None, index=None, sep='\n', mode='w')
    df16.to_csv(r'/tmp/hmichel-20/splits/s26.txt', header=None, index=None, sep='\n', mode='w')
    df17.to_csv(r'/tmp/hmichel-20/splits/s27.txt', header=None, index=None, sep='\n', mode='w')
    df18.to_csv(r'/tmp/hmichel-20/splits/s28.txt', header=None, index=None, sep='\n', mode='w')    
    df19.to_csv(r'/tmp/hmichel-20/splits/s29.txt', header=None, index=None, sep='\n', mode='w')
    df20.to_csv(r'/tmp/hmichel-20/splits/s30.txt', header=None, index=None, sep='\n', mode='w')
    df21.to_csv(r'/tmp/hmichel-20/splits/s31.txt', header=None, index=None, sep='\n', mode='w')
    df22.to_csv(r'/tmp/hmichel-20/splits/s32.txt', header=None, index=None, sep='\n', mode='w')
    df23.to_csv(r'/tmp/hmichel-20/splits/s33.txt', header=None, index=None, sep='\n', mode='w')
    df24.to_csv(r'/tmp/hmichel-20/splits/s34.txt', header=None, index=None, sep='\n', mode='w')    
    df25.to_csv(r'/tmp/hmichel-20/splits/s35.txt', header=None, index=None, sep='\n', mode='w')
    df26.to_csv(r'/tmp/hmichel-20/splits/s36.txt', header=None, index=None, sep='\n', mode='w')
    df27.to_csv(r'/tmp/hmichel-20/splits/s37.txt', header=None, index=None, sep='\n', mode='w')
    df28.to_csv(r'/tmp/hmichel-20/splits/s38.txt', header=None, index=None, sep='\n', mode='w')
    df29.to_csv(r'/tmp/hmichel-20/splits/s39.txt', header=None, index=None, sep='\n', mode='w')
    df30.to_csv(r'/tmp/hmichel-20/splits/s40.txt', header=None, index=None, sep='\n', mode='w')
    
def copy_split_files():
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        machine = "tp-1a201-"+str(i)
        localPath = "/tmp/hmichel-20/splits/s"+str(i)+".txt"
        distantPath = "/tmp/hmichel-20/splits"
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
            
def start_map(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    print("\n------- STARTING MAPPING PHASE ... -------\n")
    
    for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]:
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
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    print("\n-------STARTING SHUFFLING PHASE ... -------\n")
    
    for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]:
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
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    print("\n------- STARTING REDUCING PHASE ... -------\n")
    
    for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]:
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
    for i in [10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
        distant_machine = "tp-1a201-"+str(i) 
        command = ["ssh", f"{login}@{distant_machine}", f"scp -r {path_file_reduce} {login}@{current_machine}:{local_path}",]
        "ssh" "hmichel-20@tp-1a201-11" "scp -r /tmp/hmichel-20/reduces hmichel-20@tp-1a201-12:/tmp/hmichel-20/results"
        proc = subprocess.Popen([command[0], command[1], command[2]],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
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
           
def print_result():
    reduce_path = "/tmp/hmichel-20/result/reduces"
    output_path = "/tmp/hmichel-20/result/reduces"
    result_file_list = os.listdir(reduce_path)
    for file in result_file_list:
        with open(reduce_path + "/" + file) as f:
            with open(output_path + "/" + "WC_result.txt", "a") as f1:
                for line in f:
                    f1.write(line + "\n")   


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
start_map("python3 /tmp/hmichel-20/slave_mapper30.py")
print(("MAPPING --- %s seconds ---\n" % (time.time() - start_time1)))

start_time2 = time.time()
start_shuffle("python3 /tmp/hmichel-20/slave_shuffler30.py")
print(("SHUFFLING --- %s seconds ---\n" % (time.time() - start_time2)))

start_time3 = time.time()
start_reduce("python3 /tmp/hmichel-20/slave_reducer30.py")
print(("REDUCING --- %s seconds ---\n" % (time.time() - start_time3)))

print(("Total execution time --- %s seconds ---\n" % (time.time() - start_time1)))

scp_reduce_result()

print_result()
print_final_result("find /tmp/hmichel-20/result/reduces -name '*.txt' -exec cat {} \;")
