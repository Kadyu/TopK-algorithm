#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
int max_entry = 2<<25;
long start_time = 1644167168;
int* counter_array; 

struct thread_arg{
FILE* input_file;
int start;
int end;
//int slice_size;
char header_frag[40];
char tail_frag[40];
};

typedef struct thread_arg ThreadArg;

long get_file_length(const char* file_name){
	struct stat sb;
	if (stat(file_name,&sb)==-1){exit(-1);}
	return sb.st_size;
}

void* ReadChunk(void* arg){
ThreadArg* arg_value = (ThreadArg*)arg;
// jump to target location

fseek(arg_value->input_file,arg_value->start,SEEK_SET);

int buffer_size=40;
char buffer[buffer_size];
FILE* input = arg_value->input_file;

fgets(buffer,buffer_size,input);
char* temp;
long time_stamp = strtol(buffer,&temp,10);

if (buffer[10]==',' && buffer[10-4]!=',') {
	counter_array[time_stamp-start_time]+=1;
	// skip the frag
}else{

	strcpy(arg_value->header_frag,buffer);
}
int readed = strlen(buffer);
while(arg_value->start+readed<arg_value->end && fgets(buffer,buffer_size,input)!=NULL){
	readed += strlen(buffer);
	time_stamp = strtol(buffer,&temp,10);
	counter_array[time_stamp-start_time]+=1;
}

if (arg_value->start+readed > arg_value->end){
// a fragment is readed, put it into the tail_frag
strcpy(arg_value->tail_frag,buffer);
}
}

int main(int argc, char** argv){
	errno = 0;
	const char* file_name = "case2";
	FILE* input = fopen(file_name,"r");

	if(!input){
	printf("err:%d",errno);
	exit(errno);
	} 
	
	counter_array = (int*)malloc(max_entry*sizeof(int));

	int thread_num = 4;
	long file_len = get_file_length(file_name); 
	long size_for_each_thread = file_len/ thread_num;

	ThreadArg arg_list[thread_num];
	pthread_t readers[thread_num];

	clock_t start = clock();
	// fill up the args
	int i=0;
	int start_posi = 0;
	for(i=0;i<thread_num-1;i++){
		arg_list[i].input_file=fopen(file_name,"r");
		arg_list[i].start = start_posi;

		start_posi+=size_for_each_thread;
		
		arg_list[i].end = start_posi;
	} 

	arg_list[i].input_file = fopen(file_name,"r");

	arg_list[i].start=start_posi;
	arg_list[i].end=file_len;

	for (i=0;i<thread_num;i++){
		pthread_create(&readers[i],NULL,ReadChunk,&arg_list[i]);
	}
	for (i=0;i<thread_num;i++){
		pthread_join(readers[i],NULL);
	}

	for (i=0;i<thread_num;i++){
		fclose(arg_list[i].input_file);
	}
	int max =0;	
	for (i =0;i<max_entry;i++){
	    if (counter_array[i]>max) max =counter_array[i];
	}
	clock_t end = clock();
	printf("time take: %ld\n",end-start);
    fclose(input);
    return 0;
}
