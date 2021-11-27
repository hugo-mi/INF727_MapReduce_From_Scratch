import os

def slave_reducer_prep():
    
    # Get filename in the shufflesreceived folder
    path_shuffled_folder = "/tmp/hmichel-20/shufflesreceived"
    path_reduced_folder = "/tmp/hmichel-20/reduces"
    list_shuffle_received_files = os.listdir(path_shuffled_folder)
    
    for file in list_shuffle_received_files:
        path_file = path_shuffled_folder + "/" + file
        
        # Get the hashcode
        hashcode = file.split('-tp')[0]
        path_file_reduced = path_reduced_folder + "/" + hashcode + ".txt"
        
        file_shuffled_received = open(path_file, 'r', encoding='utf-8')
        lines_file = file_shuffled_received.readlines()
        
        for line_ in lines_file:
            file_reduced = open(path_file_reduced, "a")
            file_reduced.write(line_)
        file_reduced.write("\n")
        
def slave_reducer_finalize():
    path_reduced_folder = "/tmp/hmichel-20/reduces"

    list_reduced_files = os.listdir(path_reduced_folder)

    for file_reduced_ in list_reduced_files:
        
        path_file_reduced_bis = path_reduced_folder + "/" + file_reduced_
        
        # Counting the number of lines
        current_reduced_file = open(path_file_reduced_bis, 'r', encoding='utf-8')
        first_line = current_reduced_file.readline().rstrip()
        first_line_splitted = first_line.split("1")
        word = first_line_splitted[0].rstrip()
            
        with open(path_file_reduced_bis) as f:
            line_stripped = f.readlines()
            nb_lines = 0
            for line_stripped in line_stripped:
                if line_stripped.strip():
                    nb_lines = nb_lines + 1
        
        file_reduced_bis = open(path_file_reduced_bis, "w")
        file_reduced_bis.write(word + " " + str(nb_lines))
        file_reduced_bis.close()

slave_reducer_prep()
slave_reducer_finalize()