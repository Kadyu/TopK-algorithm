#!/usr/bin/python3
from os.path import exists
import subprocess
import random
import resource 
from threading import Timer

start_time = 1645491600
kill = lambda process: process.kill()

random.seed(1996) 

MAX_VIRTUAL_MEMORY = 50 * 1024 * 1024 # 50 MB

def limit_virtual_memory():
    # The tuple below is of the form (soft limit, hard limit). Limit only
    # the soft part so that the limit can be increased later (setting also
    # the hard limit would prevent that).
    # When the limit cannot be changed, setrlimit() raises ValueError.
    resource.setrlimit(resource.RLIMIT_AS, (MAX_VIRTUAL_MEMORY, resource.RLIM_INFINITY))



correct_case = 0
   

false_txt = open("execution_result","w") 
for i in [1,2,3,4,5,6]:
    k = random.randint(50,100)
    case_name="case%d/"%i

    example_cmd = ['./test.example', case_name,str(start_time),str(k)]
    student_cmd = ["./test.o", case_name,str(start_time),str(k)]

    example_output = subprocess.Popen(example_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,preexec_fn=limit_virtual_memory)
    student_output = subprocess.Popen(student_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,preexec_fn=limit_virtual_memory)

    time_out_sec = 5


    try:
        exm_stdout, exm_stderr = example_output.communicate(timeout=time_out_sec)
        std_stdout, std_stderr = student_output.communicate(timeout=time_out_sec)

        if exm_stdout == std_stdout:
            correct_case+=1
            print("test case %d passed"%i)
            pass
        elif len(std_stdout)==0:
            print("There's no output in case: ./test.o %s %d %d"%(case_name,start_time,k))
            print("Possible cases: the output has not been flushed, or your program exceed the mmeory limitation")
        else:
            error_case = "failed in test case " + str(i)+"\n"+"with input parameter: %s %d %d"%(case_name,start_time,k)+ "\n Reference output should be: "+exm_stdout.decode('latin-1')+"\n"+"Your output is: "+std_stdout.decode('latin-1')
            false_txt.write(error_case)
    except subprocess.TimeoutExpired:
        print("failed in test case " + str(i) +" due to : Time Out")
        example_output.kill()
        student_output.kill()

false_txt.flush()
false_txt.close()

