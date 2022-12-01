#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

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
if (buffer[10]!=',') {
	strcpy(arg_value->header_frag,buffer);
	// skip the frag
}

int readed = strlen(buffer);
while(arg_value->start+readed<arg_value->end && fgets(buffer,buffer_size,input)!=NULL){
// do nothing, for we only read and align the frames	
//
readed += strlen(buffer);

}

if (arg_value->start+readed > arg_value->end){
// a fragment is readed, put it into the tail_frag
strcpy(arg_value->tail_frag,buffer);
}
}

int main(int argc, char** argv){

	clock_t start = clock();
	errno = 0;
	const char* file_name = "case2";
	FILE* input = fopen(file_name,"r");

	if(!input){
	printf("err:%d",errno);
	exit(errno);
	} 
	
	int thread_num = 10;
	long file_len = get_file_length(file_name); 
	long size_for_each_thread = file_len/ thread_num;

	ThreadArg arg_list[thread_num];
	pthread_t readers[thread_num];
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

	clock_t end = clock();
	printf("time take: %ld\n",end-start);
    fclose(input);
    return 0;
}
