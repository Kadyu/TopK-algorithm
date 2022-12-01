#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

// All possible hours in 13 months.
// Used to create an Array of all possible hours -> Simulated Hash Table size
#define TABLE_SIZE 9323

pthread_mutex_t lock;

// used to find index for simulated hash table
int start_time;

int thread_numbers;

// helps to determine which files should each thread needs to read
int thread_index;

// keeps time_stamp and its counter together when doing sorting
typedef struct {
    int key;
    int counter;
} pair;

// array of all possible hour indexed in 13 months 
pair dictionary[TABLE_SIZE];


// reference of this code was taken from GEEKsForGEEKs websites
// Heapifying the given array into the max Heap
void heapify(int n, int i)
{
    // initialize largest as root
    int largest = i;
    // left child = 2*i + 1
    int l = 2 * i + 1;
    // right child = 2*i + 2
    int r = 2 * i + 2;

    // if left child is larger than root
    // if the counters are the same then we compare them by keys
    if ((l < n && dictionary[l].counter == dictionary[largest].counter && dictionary[l].key > dictionary[largest].key) || (l < n && dictionary[l].counter > dictionary[largest].counter) )
        
        largest = l;

    // If right child is larger than largest so far
    // if the counters are the same then we compare them by keys
    if ((r < n && dictionary[r].counter == dictionary[largest].counter && dictionary[r].key > dictionary[largest].key) || (r < n && dictionary[r].counter > dictionary[largest].counter))
        largest = r;

    // If largest is not root
    if (largest != i) {
        //swap
        int tempc = dictionary[largest].counter;
        int tempk = dictionary[largest].key;
        
        dictionary[largest].counter = dictionary[i].counter;
        dictionary[i].counter = tempc;
        dictionary[largest].key = dictionary[i].key;
        dictionary[i].key = tempk;
        
        // recursively heapify the affected sub-tree
        heapify(n, largest);
    }
}

// main function to do heap sort to find largest K numbers
// reference of this code was taken from GEEKsForGEEKs websites
void heapSort(int n, int k)
{
    // build heap (rearrange array)
    for (int i = n / 2 - 1; i >= 0; i--)
        heapify(n, i);

    // one by one extract an element from heap
    for (int i = n - 1; i > 0; i--) {
        
        // Move current root to end
        int tempc = dictionary[i].counter;
        int tempk = dictionary[i].key;
        
        dictionary[i].counter = dictionary[0].counter;
        dictionary[0].counter = tempc;
        dictionary[i].key = dictionary[0].key;
        dictionary[0].key = tempk;
        

        //we are stopping the heapsort when we find K largest elements to avoid unnecessary calculations
        //instead of NlogN we are running KlogN where K is the number of elements that would be printed as a result
        for(int j = 0; j < k; j++){
            heapify(i, 0);
        }
    }
}


//initializing all possible key values in the dictionary
void initialize(){
    for(int i = 0; i < TABLE_SIZE; i++){
        dictionary[i].key = i;
    }
}

//printing the result in the right format
void printTime(int tt, int startTime, int caunter){
    
    //recalculation index to time-stamp format
    tt *= 3600;
    tt += startTime;
    
    //translating into date string and printing
    time_t t = tt;
    struct tm *tm = localtime(&t);
    char s[64];
    strftime(s, sizeof(s), "%c", tm);
    printf("%s\t%d\n", s,caunter );
}

//thread fuction to read different files by different threads
//used for reading the input files
void * thread_read(void * index){
    
    int this_thread_index = thread_index;
    int file_index = 1;
    
    thread_index++;
    
    //opening directory
    DIR* FD;
    struct dirent* in_file;
    FILE    *common_file;
    char target_file[255];

    //cheking directory
    if (NULL == (FD = opendir(index)))
    {
        fprintf(stderr, "Error : Failed to open input directory - %s\n", strerror(errno));
    }
 
    while ((in_file = readdir(FD)) != NULL)
    {
        file_index = (file_index + 1) % thread_numbers;
        
        if (!strcmp (in_file->d_name, "."))
            continue;
        if (!strcmp (in_file->d_name, ".."))
            continue;
        
        
        //each thread reads files only if their indexed match
        //for example thread 2 can read only files with index 2
        if(this_thread_index == file_index){
    
            strcpy(target_file,index);
            strcat(target_file,in_file->d_name);
            common_file = fopen(target_file, "r");


            if(!common_file){
                printf("err:%d",errno);
                exit(errno);
            }

            int buffer_size=40;
            char buffer[buffer_size+1];


            while(fgets(buffer,buffer_size,common_file)!=NULL){

                char* temp;

                int time_stamp = strtol(buffer,&temp,10);

                //calculation the time-stamp into the dictionary index
                time_stamp = (time_stamp - start_time) / 3600;
                
                
                //avoiding simultaneous access of thread to the global dictionary
                pthread_mutex_lock(&lock);
                
                //increamenting the counter for accessed hour
                dictionary[time_stamp].counter++;
                
                pthread_mutex_unlock(&lock);

            }
        fclose(common_file);
        }
    }
}


int main(int argc, char **argv)
{
        initialize();
        
        start_time = atoi(argv[2]);
    
        thread_numbers = 2;
    
        int k = atoi(argv[3]) + 1;
    
        //creating an array of threads
        pthread_t threads[thread_numbers];
    
        //running each thread
        for(int i = 0; i < thread_numbers; i++){
                pthread_create(&threads[i],NULL,thread_read,argv[1]);
        }
        
        //waiting for thread completion
        for(int i = 0; i < thread_numbers; i++){
                pthread_join(threads[i],NULL);
        }

        heapSort(TABLE_SIZE, k - 1);
    
        printf("Top K frequently accessed hour:\n");

        for(int i = TABLE_SIZE - 1; i > TABLE_SIZE - k; i--){
            printTime(dictionary[i].key, start_time, dictionary[i].counter);
        }
    
        return 0;
    }

