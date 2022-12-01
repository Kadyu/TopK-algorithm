#!/usr/bin/python3
import string
import random
from pathlib import Path

SEED = 2

random.seed(SEED)
start_time = 1645491600
end_time = start_time + 2**25

possible_codes=[100,200,201,204,206,301,302,304,307,308,401,403,404,406,407,409,410,412,416,418,425,451,500,501,502,503,504]
possible_codes=[str(x) for x in possible_codes]

def generate_random_data(time_stamps):
    line=""
    line+=str(time_stamps)
    payload = ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(5,10)))       
    line+=","+payload
    return line


def payload_generator(entry_count,target_dir,order=0,walk_through=False):
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    output_file = open(target_dir+"/input0","w")
    if not walk_through:
        if order ==0: 
            for i in range(entry_count):
                if i % 1000000 == 0:
                    print(i)

                time_stamps = random.randrange(start_time,end_time)
                line=generate_random_data(time_stamps)
                output_file.write(line+"\n")
        else:
            time_stamps=random.sample(range(start_time,end_time),entry_count)
            if order==1:
                for time_stamp in time_stamps:
                    line = generate_random_data(time_stamp)
                    output_file.write(line+"\n")
            else:
                for index in range(entry_count):
                    time_stamp = time_stamps[entry_count-index-1]
                    line = generate_random_data(time_stamp)
                    output_file.write(line+"\n")

    else:
        for i in range(start_time,end_time):
            line = ""
            if i % 1000000 == 0:
                print(i)
            time_stamps = i 
            line=generate_random_data(time_stamps)

            output_file.write(line+"\n")

    output_file.flush()
    output_file.close()


if __name__ == "__main__":
    # just for a taste
    payload_generator(10000,"case1",0)
#    print("case 1 finished")
    payload_generator(10000,"case2",1)
#    print("case 2 finished")
    payload_generator(10000,"case3",2)
#    print("case 3 finished")
#    payload_generator(100,"case4",1,True)
#    print("case 4 finished")
#    payload_generator(int((end_time-start_time)),"case5")
#    print("case 5 finished")



