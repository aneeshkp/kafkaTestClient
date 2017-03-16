#include <stdio.h>
#include <string.h>
#include <stdlib.h>
int main(){
  long threadnum = 12;
  char *str;
  char s[]="Aneesh";
  sprintf(str, "%d", threadnum);
  printf("the value of str %s\n",str);
  printf("Thavlue of s is %s\n",s);
  strcat(s,str);
  printf("value of cat is %s\n", s);

}
