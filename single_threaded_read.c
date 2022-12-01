#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>

char** str_split(char* a_str, const char a_delim)
{
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;

    result = malloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        *(result + idx) = 0;
    }

    return result;
}


int main(int argc, char** argv){

	errno = 0;

	FILE* input = fopen("case2","r");

	if(!input){
	printf("err:%d",errno);
	exit(errno);
	} 
	int buffer_size=40;
	char buffer[buffer_size+1];


	clock_t start = clock();

	int i=0;
	int line=0;
	while(fgets(buffer,buffer_size,input)!=NULL){
	}
	
	clock_t end = clock();
	printf("time take: %ld\n",end-start);
    fclose(input);
    return 0;
}
