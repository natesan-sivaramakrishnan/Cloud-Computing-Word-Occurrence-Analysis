# Cloud-Computing
Project files created for CS643 Cloud Computing

Description:
1. Build your own Hadoop AMI, starting from the Amazon Linux AMI (https://aws.amazon.com/amazon-linux-ami/). You have to use latest stable Hadoop release. 
2. Write a Hadoop/Yarn MapReduce application that takes as input the 50 Wikipedia web pages dedicated to the US states (we will provide these files for consistency) and:
a) Computes how many times the words “education”, “politics”, “sports”, and “agriculture” appear in each file. Then, the program outputs the number of states for which each of these words is dominant (i.e., appears more times than the other three words). 
b) Identify all states that have the same ranking of these four words. For example, NY, NJ, PA may have the ranking 1. Politics; 2. Sports. 3. Agriculture; 4. Education (meaning “politics” appears more times than “sports” in the Wikipedia file of the state, “sports” appears more times than “agriculture”, etc.)
