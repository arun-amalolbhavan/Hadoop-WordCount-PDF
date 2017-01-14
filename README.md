# Hadoop-WordCount-PDF
Word counts from a PDF file using hadoop map reduce program 

 - Create input splits for each each PDF file.
 - Create a single map task for each PDF file.
 - Cannot split PDF file, have to load decoded text if we need to split and run multiple map tasks for long files.

Usage:<br/>
~$ export LIBJARS=/localpath/fontbox.jar,/localpath/pdfbox.jar<br/>
~$ hadoop jar hadoop-examples.jar arun.hadoop.wordcount.Driver -libjars ${LIBJARS} &nbsp;/path/*.pdf &nbsp;/path/output
