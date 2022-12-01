#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>

int isFile(const char* name)
{
    DIR* directory = opendir(name);

    if(directory != NULL)
    {
     closedir(directory);
     return 0;
    }

    if(errno == ENOTDIR)
    {
     return 1;
    }

    return -1;
}
int main(int argc, char **argv)
{

    DIR* FD;
    struct dirent* in_file;
    FILE    *common_file;
    char target_file[255];


    int input_is_file = isFile(argv[1]);

    if (input_is_file==1){
	   printf("it's a file\n"); 
    }

    if (input_is_file==0){
	printf("it's a dir\n"); 
    	if (NULL == (FD = opendir(argv[1]))) 
	    {
		fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
		fclose(common_file);

		return 1;
	    }
	while ((in_file = readdir(FD)) != NULL)
    	{
		if (!strcmp (in_file->d_name, "."))
		    continue;
		if (!strcmp (in_file->d_name, ".."))
		    continue;
		strcpy(target_file,argv[1]);
		strcat(target_file,in_file->d_name);

		common_file = fopen(target_file, "r");

		if (common_file == NULL)
		{
		    fprintf(stderr, "Error : Failed to open entry file - %s\n", strerror(errno));
		    fclose(common_file);
		    return 1;
		}
		fclose(common_file);
    	}   
    }
    
    return 0;
}
