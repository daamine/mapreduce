/**
  *   AUTHOR: Amine Daoud
  *   @link https://github.com/daamine
*/


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <glib.h>

#ifdef DEBUG
#define DEBUG_PRINT(x) printf x
#else
#define DEBUG_PRINT(x) do {} while (0)
#endif

#define MAX_DATA_LENGTH 10000000	/* 10MB */


/* funtion to get the next line from an input file */
char *
get_next_line (FILE * input_file)
{
  char temp[32];
  static char *ret = NULL;
  size_t full_length = 0;
/* we try to get the next line and avoid the truncation on the line */
  while (fgets (temp, sizeof (temp), input_file))
    {
      size_t len = strlen (temp);
      char *r_temp = realloc (ret, full_length + len + 1);
      if (r_temp == NULL)
	{
	  break;
	}
      ret = r_temp;
      strcpy (ret + full_length, temp);	/* concatenate */
      full_length += len;

      if (feof (input_file) || temp[len - 1] == '\n')
	{
	  return ret;
	}
    }

  free (ret);
  return NULL;
}

void
iterator (gpointer key, gpointer value, gpointer user_data)
{
  DEBUG_PRINT ((user_data, (char *) key, *(char *) value));
}

void
show (gpointer key, gpointer user_data)
{
  GHashTable **hash = user_data;
  DEBUG_PRINT (("key: %s \n", (char *) key));
}

/* comparator function to sort the GList returning 0 when the 2 strings match, nagative value if a<b and positive value if a>b */
int
compare_string (gpointer a, gpointer b)
{
  return strcmp ((char *) a, (char *) b);
}


/* Thread function */
void *
call_map (void *data)
{

  DEBUG_PRINT (("Inside thread function, The line is: %s %ld \n",
		(char *) data, strlen ((char *) data)));
  char *ch = (char *) data;
  GHashTable *hash = g_hash_table_new (g_str_hash, g_str_equal);	/* using the hashtable from GLIB */
  char *pch;
  int i = 0;
/* we fetch the words seperated by the delimeters " .,;:!?-\t" using strtok_r which is thread safe */
  while (pch = strtok_r (ch, " .,;:!?-\t", &ch))
    {
      DEBUG_PRINT (("splitted word: %s\n", pch));
      unsigned char *value =
	(unsigned char *) malloc (sizeof (unsigned char));
      int v = 0;
      if (!g_hash_table_contains (hash, pch))
	{
	  v = 1;
	  sprintf (value, "%d", v);
	  g_hash_table_insert (hash, pch, value);
	}
      else
	{

	  v = atoi ((g_hash_table_lookup (hash, pch)));
	  v++;
	  sprintf (value, "%d", v);
	  g_hash_table_replace (hash, pch, value);
	}
    }
  g_hash_table_foreach (hash, (GHFunc) iterator,
			"in call_map The occurence of %s is %d\n");
  return hash;			/* each thread return a hashtable containing the words as keys and the occurences as values */
}


int
main (int argc, char **argv)
{


  if (argc < 3)
    {
      printf ("USAGE: ./mapred FILE NBR_THREADS\n");
      exit (-1);
    }

  DEBUG_PRINT (("the file name is %s \n", argv[1]));

  FILE *input_file = fopen (argv[1], "r");	/* open the input file */
  if (input_file == NULL)
    {
      printf ("Error: Could not open the file\n");
      exit (-1);
    }

  if (argv[2][0] == '-')
    {
      printf ("Sorry, you have entered negative value. \n");
      exit (-1);
    }
  unsigned int nb_threads = atoi (argv[2]);
  if (!nb_threads)
    {
      printf ("Error: Please entre a valid number of threads\n");
      exit (-1);
    }

  DEBUG_PRINT (("the number of threads: %d \n", nb_threads));

  int nb_lines = 0;

  char *n;
  char *data[nb_threads];
  int j = 0;
  for (j = 0; j < nb_threads; j++)
    {
      data[j] = (char *) calloc (MAX_DATA_LENGTH, sizeof (char));
    }
  int index = 0;

/********************* MAP step: go through the file and distribute the lines on the threads equally*************************/
  while ((n = get_next_line (input_file)) != NULL)
    {
      char *line = strdup (n);
      if (index == nb_threads)
	index = 0;
      if ((line != NULL) && (line[0] != '\n'))
	{
	  line[strlen (line) - 1] = ' ';
	  strcat (data[index], line);
	  index++;
	  nb_lines++;

	}
      free (line);
    }

  DEBUG_PRINT (("the number of lines in file: %d \n", nb_lines));

  if (nb_threads > nb_lines)
    {
      printf
	("WARNING: the number of threads are more than the number of lines in file\n");
      printf
	("WARNING: we are limiting the numbers of threads to number of lines\n");
      nb_threads = nb_lines;
    }
  fclose (input_file);		/* close the file */

  pthread_t task[nb_threads];
  int i;
  for (i = 0; i < nb_threads; i++)
    {
      pthread_create (&task[i], NULL, call_map, (void *) data[i]);	/* threads creation */
    }


  GHashTable *hash[nb_threads];
  for (i = 0; i < nb_threads; i++)
    {
      pthread_join (task[i], (void **) &hash[i]);	/* threads synchronisation: each thread should return a HashMap data structure containing the words and their occurences. */
      g_hash_table_foreach (hash[i], (GHFunc) iterator,
			    "The occurence of %s is %d\n");
    }

/********************** REDUCE step: use the results returned by threads and output the final result. */

/* We take the list of words per HashMap, we concatenate them in a single list then we sort the final list */
  GList *words[nb_threads];
  for (i = 0; i < nb_threads; i++)
    {
      words[i] = g_hash_table_get_keys (hash[i]);
      if (i > 0)
	{
	  words[0] = g_list_concat (words[0], words[i]);
	}

    }

  words[0] = g_list_sort (words[0], (GCompareFunc) compare_string);
  g_list_foreach (words[0], (GFunc) show, NULL);

  GList *l = NULL;
  for (l = words[0]; l != NULL; l = l->next)
    {
      int occurence = 0;
      char prev_word[100000];
      if (strcmp (prev_word, l->data))	/* Unfortunately, there is no Set data structure in C, so we should avoid the redundant values in the list. */
	{
	  for (i = 0; i < nb_threads; i++)
	    {

	      if (g_hash_table_contains (hash[i], l->data))
		{

		  occurence +=
		    atoi ((g_hash_table_lookup (hash[i], l->data)));
		}

	    }
	  printf ("%s=%d\n", (char *) l->data, occurence);
	}
      strcpy (prev_word, l->data);
    }

  for (j = 0; j < nb_threads; j++)
    {
      if (data[j])
	{
	  free (data[j]);
	}
      if (hash[j])
	{
	  g_hash_table_remove_all (hash[j]);
	  g_hash_table_unref (hash[j]);
	}
    }

}
