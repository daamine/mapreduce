#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char *get_filled_buffer(FILE* in_file) {
  char temp[32];
  static char *ret = NULL;
  size_t full_length = 0;
   
  while (fgets(temp, sizeof(temp), in_file)) {
    size_t len = strlen(temp);
    char *r_temp = realloc(ret, full_length + len + 1);
    if (r_temp == NULL) {
      break;
    }
    ret = r_temp;
    strcpy(ret + full_length, temp); /* concatenate */
    full_length += len;
   
    if (feof(in_file) || temp[len-1] == '\n') {
      return ret;
    }
  }
  
  free(ret);
  return NULL;
}

int main(int argc, char** argv) {

    
    printf("the file name is %s \n",argv[1]);
    FILE *in_file  = fopen(argv[1], "r"); 
    if (in_file == NULL) 
    {   
         printf("Error: Could not open the file\n"); 
         exit(-1); 
    }  
    printf("the number of threads: %s \n",argv[2]);
    
//    while ( fgets( line, 1000, in_file ) != NULL ) 
      char* n; 
      while ( (n = get_filled_buffer(in_file)) != NULL /*&&  (line = strdup(n)) != NULL*/ ){
            char* line = strdup(n);
            if ( (line != NULL) && (line[0] != '\n') ) {
                printf("The line is: %s %ld \n", line, strlen(line));
            }
            free(line);
       }
       fclose(in_file);
}
