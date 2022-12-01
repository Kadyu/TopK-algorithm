#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
int max_entry = 2<<25;
long start_time = 1645491600;
int* counter_array; 
int block_count=0;

struct thread_arg{
FILE* input_file;
int start;
int end;
int slice_size;
char header_frag[40];
char tail_frag[40];
};

typedef struct thread_arg ThreadArg;

long get_file_length(const char* file_name){
	struct stat sb;
	if (stat(file_name,&sb)==-1){exit(-1);}
	return sb.st_size;
}

void parse_block(char* buffer, char* tail,int block_size,char* value_string){
	int parsed_posi = 0;
	int val_po=0;
	long time_stamp;
	// parse the first entry
	while(tail[val_po]!='\0'){
		value_string[val_po]=tail[val_po];
		val_po++;
	}
	while(buffer[parsed_posi]!=','){
		value_string[val_po]=buffer[parsed_posi];
		parsed_posi++;
		val_po++;
	}

	time_stamp=atol(value_string);
	value_string[val_po]='\0';
	counter_array[time_stamp-start_time]++;
	val_po=0;
	parsed_posi++;

	char current;
	while(parsed_posi<block_size){
		current = buffer[parsed_posi];
		if (current==','){
		// former part is the target_value
		buffer[parsed_posi]='\0';
		time_stamp=atol(buffer+parsed_posi-10);
		counter_array[time_stamp-start_time]++;
		}
		tail[val_po]=current;
		if(current=='\n'){
		// when it swtich the line
			val_po=-1;// reset it again
		}
		// move the index
		val_po++;
		parsed_posi++;
	}
	tail[val_po]='\0';
}

void* ReadChunk(void* arg){
	ThreadArg* arg_value = (ThreadArg*)arg;
	// jump to target location

	fseek(arg_value->input_file,arg_value->start,SEEK_SET);
	int start = arg_value->start;
	int end = arg_value->end;
	int buffer_size=arg_value->slice_size;
	char buffer[buffer_size];
	FILE* input = arg_value->input_file;
	char value_string[40];

	int readed = 0; // fread(buffer,1,arg_value->slice_size, input);
	arg_value->tail_frag[0]='\0';// init the tail
	int current_read=0;
	while(readed+start<end){
		current_read = fread(buffer,1,arg_value->slice_size, input);
		readed+=current_read;
		block_count++;
		parse_block(buffer,arg_value->tail_frag,current_read,value_string);
	}

}

void start_threads(int thread_num,ThreadArg* arg_list,pthread_t* readers,const char* file_name){
	long file_len = get_file_length(file_name); 
	long size_for_each_thread = file_len/ thread_num;
	
	int i=0;
	int start_posi = 0;
	for(i=0;i<thread_num-1;i++){
		arg_list[i].input_file=fopen(file_name,"r");
		arg_list[i].start = start_posi;

		start_posi+=size_for_each_thread;
		
		arg_list[i].end = start_posi;
		arg_list[i].slice_size = 4096;
	} 

	arg_list[i].input_file = fopen(file_name,"r");

	arg_list[i].start=start_posi;
	arg_list[i].end=file_len;
	arg_list[i].slice_size = 4096;
	
	// start the system
	for (i=0;i<thread_num;i++){
		pthread_create(&readers[i],NULL,ReadChunk,&arg_list[i]);
	}
	for (i=0;i<thread_num;i++){
		pthread_join(readers[i],NULL);
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
	
	int thread_num = 1;

	counter_array = (int*)malloc(max_entry*sizeof(int));
	
	ThreadArg arg_list[thread_num];
	pthread_t readers[thread_num];
	// fill up the args
	
	start_threads(thread_num,arg_list,readers,file_name);
	int i=0;
	for (i=0;i<thread_num;i++){
		fclose(arg_list[i].input_file);
	}

	clock_t end = clock();
	printf("time take: %ld\n",end-start);
	free(counter_array);
    fclose(input);
    return 0;
}
