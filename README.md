# Hadoop-WordCount-PDF
Get word count from PDF file using hadoop mapreduce program

 - Create input splits for each each PDF file.
 - Create a single map task for each PDF file.
 - Cannot split PDF file, have to load decoded text if we need to split and run multiple map tasks for long files.

Usage:<br/>
~$ export LIBJARS=/localpath/fontbox.jar,/localpath/pdfbox.jar<br/>
~$ hadoop jar hadoop-examples.jar arun.hadoop.wordcount.Driver -libjars ${LIBJARS} /path/*.pdf /path/output
