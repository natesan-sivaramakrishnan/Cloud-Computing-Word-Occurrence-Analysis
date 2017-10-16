Submitted By: Natesan Sivaramakrishnan
ucid: ns632

***************************************************************************************************************************

INSTRUCTIONS TO RUN THE APPLICATION PROGRAM

1. Make a directory to put all the input data on namenode /home/ec-user/inputdata
2. Create a directory /user in HDFS:
 $  cd /usr/local/hadoop 
 $  bin/hdfs dfs -mkdir /user

3. Copy the inputdata folder to HDFS:
 $   cd /usr/local/hadoop/ 
 $   bin/hdfs dfs -put '/home/ec2-user/inputdata' /user/

4. Create directory on namenode and copy the java application program part2a.java into the folder
 $   /home/ec2-user/part2a

5. Create directory on namenode and copy the java application program part2b.java into the folder
 $   /home/ec2-user/part2b

6. Compile the part2a.java application program:

 $   cd /home/ec2-user/part2a
 $   javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.8.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d /home/ec2-user/part2a part2a.java

7. Create a folder part2ajar inside part2a and move all class files into part2a

8. Create jar file by executing the command below:

 $    cd /home/ec2-user/part2a
 $    jar -cvf part2ajar.jar -C /home/ec2-user/part2a/part2ajar .

9. jar file named part2ajar.jar is created

10. To Run the application program

 $  cd /usr/local/hadoop 
 $  bin/hadoop jar /home/ec-2user/part2a/part2a.jar part2a

11. Output is created in the folder /user/hduser/ns632-FinalOutputPart2a

 $  cd /usr/local/hadoop 
 $  hdfs dfs -cat /user/ec2-user/ns632-FinalOutputPart2a/part-r-00000
 
12. Compile the part2b.java application program:

 $   cd /home/ec2-user/part2b
 $   javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.8.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d /home/ec2-user/part2b part2b.java

13. Create a folder part2bjar inside part2b and move all class files into part2b

14. Create jar file by executing the command below:

 $    cd /home/ec2-user/part2b
 $    jar -cvf part2bjar.jar -C /home/ec2-user/part2b/part2bjar .

15. jar file named part2bjar.jar is created

16. To Run the application program

 $  cd /usr/local/hadoop 
 $  bin/hadoop jar /home/ec-2user/part2b/part2b.jar part2b

17. Output is created in the folder /user/hduser/ns632-FinalOutputPart2b

 $  cd /usr/local/hadoop 
 $  hdfs dfs -cat /user/ec2-user/ns632-FinalOutputPart2b/part-r-00000 
 


**************************END OF DOCUMENT***********************************************************************************

