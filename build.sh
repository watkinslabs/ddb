#!/bin/bash

HEADER=builds/ddb.h
SOURCE=builds/ddb.c


# bump the version in git...
./commit.sh


echo '// ddb all in one source'>$SOURCE
echo '#include "ddb.h"'>>$SOURCE
echo ''>>$SOURCE
echo ''>>$SOURCE
for x in $(find source/ |grep '\.c')
do 
  echo '// *** BEGIN '$x>>$SOURCE
  cat $x|sed '/#include "/d'>>$SOURCE; 
  echo '// *** END'>>$SOURCE; 
  echo ''>>$SOURCE; 
done




echo '// ddb all in one header'>$HEADER
cat $SOURCE| grep include| sed 's/[ \t]*$//' | sort| uniq>>$HEADER
echo ''>>$HEADER
echo ''>>$HEADER
for x in $(cat source/include/include_order.txt)
do 
  echo '// *** BEGIN '$x>>$HEADER
  cat $x>>$HEADER; 
  echo '// *** END'>>$HEADER; 
  echo ''>>$HEADER; 
done

echo ''>>$HEADER

# delete all local includes fomr this header... 
sed -i '/#include "/d'  $HEADER

sed -i '/#include </d'  $SOURCE
