#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>


int main(int argc, char** argv){

	errno = 0;

	FILE* input = fopen("case2","r");

	if(!input){
	printf("err:%d",errno);
	exit(errno);
	} 
	int buffer_size=40;
	char buffer[buffer_size+1];

	int max_entry = 2<<25;
	long start_time = 1644167168;

	int* counter_array = (int*)malloc(max_entry*sizeof(int));
	clock_t start = clock();

	int i=0;
	int line=0;
	while(fgets(buffer,buffer_size,input)!=NULL){
		char* temp;
		long time_stamp = strtol(buffer,&temp,10);
		counter_array[time_stamp-start_time]+=1;
	}
	int max = 0;
	for (i =0;i<max_entry;i++){
	    if (counter_array[i]>max) max =counter_array[i];
	}
	clock_t end = clock();
	printf("time take: %ld\n",end-start);
	free(counter_array);
    fclose(input);
    return 0;
}
