#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <glib.h>


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

void iterator(gpointer key, gpointer value, gpointer user_data) {
 printf(user_data, (char *)key, *(char*) value);
}

void show(gpointer key, gpointer user_data) {
  GHashTable** hash = user_data ; 
  printf("Amine %s \n", (char *)key);
}

void reduce(gpointer key, gpointer user_data) {
   printf("reduce %s \n", (char *)key);
}

int compare_string(gpointer a, gpointer b) {
 //return strcmp((char*)a,(char*)b);
 return g_ascii_strcasecmp((char*)a,(char*) b);
}

void* call_map(void* data)
{

  printf("Thread The line is: %s %ld \n",(char *) data, strlen((char *)data));
  char* ch = (char* ) data;
  GHashTable* hash = g_hash_table_new(g_str_hash, g_str_equal);
  char *pch = strtok (ch," .,;:!?-\t"); 
  int i = 0;

  while (pch != NULL)
  {
    printf ("splitted word: %s\n",pch);   
    char* value = (char *) malloc (sizeof(char));
    if ( !g_hash_table_contains(hash,pch) ){
        *value = 1;
        g_hash_table_insert(hash, pch, value);
    }
    else {
          
            value = (char *)(g_hash_table_lookup(hash,pch));
            printf ("value: %d\n", *value);
            (*value)++;
            printf ("value: %d\n",*value);
            g_hash_table_replace (hash, pch, value);
    }
    printf ("splitted word: %d\n",  *(char *)(g_hash_table_lookup(hash,pch)));
    pch = strtok (NULL, " .,;:!?-\t");
  }
  g_hash_table_foreach(hash, (GHFunc)iterator, "in call_map The occurence of %s is %d\n");
  return hash;
}


int main(int argc, char** argv) {

    
    if (argc < 3){
         printf("USAGE: ./mapred FILE NBR_THREADS\n");
         exit(-1);
    }

    printf("the file name is %s \n",argv[1]);
    FILE *in_file  = fopen(argv[1], "r"); 
    if (in_file == NULL) 
    {   
         printf("Error: Could not open the file\n"); 
         exit(-1); 
    } 

    if(argv[2][0] == '-')
    {  
        printf("Sorry, you have negative value. \n");
        exit (-1);
    }  
    unsigned int nb_threads = atoi(argv[2]);
    if (!nb_threads){
         printf("Error: Please entre a valid number of threads\n");
         exit(-1);     
    }
    printf("the number of threads: %d \n", nb_threads);
    
    int nb_lines = 0; 

      char* n; 
      char data[nb_threads][100000];
      int index = 0;
      while ( (n = get_filled_buffer(in_file)) != NULL ){
            char* line = strdup(n);
            if (index == nb_threads) index = 0;
            if ( (line != NULL) && (line[0] != '\n') ) {
                line[strlen(line)-1] = ' ';
                strcat(data[index],line);
                printf("The line is: %s %ld \n",line, strlen(line));
                printf("Amine The line is: %d  %s %ld \n", index, data[index], strlen(data[index]));
                index++;
                nb_lines++;

            }
            free(line);
       }
       printf("the number of lines in file: %d \n", nb_lines);
       if (nb_threads>nb_lines){
           printf("WARNING: the number of threads are more than the number of lines in file\n");
printf("WARNING: we will limit then the numbers of threads to number of lines\n");
           nb_threads = nb_lines;
       }
       fclose(in_file);
        
    pthread_t task[nb_threads];    
    int i;
    for(i=0;i<nb_threads;i++)
     {
         pthread_create(&task[i],NULL,call_map,(void *)data[i]);
     }

    GHashTable* hash[nb_threads] ;  
    for(i=0;i<nb_threads;i++)
     {
       hash[i] = g_hash_table_new(g_str_hash, g_str_equal);
       pthread_join(task[i],(void**) &hash[i]); // les threads synchronisent
       g_hash_table_foreach(hash[i], (GHFunc)iterator, "The occurence of %s is %d\n");
     }
   
    GList * words[nb_threads];
    for(i=0;i<nb_threads;i++){
          words[i] = g_hash_table_get_keys (hash[i]);
          if (i>0) {
              words[0] = g_list_concat(words[0],words[i]);
          }
        
/*        if ( !g_hash_table_contains(hash[0],pch) ){
             *value = 1;
             g_hash_table_insert(hash, pch, value);
        }
        else {

             value = (char *)(g_hash_table_lookup(hash,pch));
             printf ("value: %d\n", *value);
             (*value)++;
             printf ("value: %d\n",*value);
             g_hash_table_replace (hash, pch, value);
       } */
   }
   words[0]= g_list_sort(words[0],(GCompareFunc)compare_string);
   g_list_foreach(words[0], (GFunc)show, NULL);
   
   //g_list_foreach(words[0], (GFunc)reduce, (void**) &hash);
   GList *l;
   for (l = words[0]; l != NULL; l = l->next)
   {
           int occurence = 0;
           char prev_word[1000]; 
           if (strcmp(prev_word,l->data)){
           for(i=0;i<nb_threads;i++){ 

                     if ( g_hash_table_contains(hash[i], l->data )) {
                             
                          occurence += *(char *)(g_hash_table_lookup(hash[i], l->data));
                     }
           }
           printf ("occurence of %s  %d\n", l->data , occurence);
           }
           strcpy(prev_word,l->data);
   }

}
