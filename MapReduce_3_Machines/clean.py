import subprocess

def slave_machine(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
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

def master_machine(command):
    timer=5
    login="hmichel-20"
    machine = "tp-1a201-12"
    proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)

    try:
        out, err = proc.communicate(timeout=timer)
        code = proc.returncode
        print(out)
        print(err)
        print(code)
    except subprocess.TimeoutExpired:
        proc.kill()
        print(proc+" timeout")
            
slave_machine("rm -rf /tmp/hmichel-20")
master_machine("rm -rf /tmp/hmichel-20/result/reduces")