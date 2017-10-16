import java.io.IOException;
import java.util.*;
import java.io.*;
import java.lang.String.*;
import java.lang.Object;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class part2b {

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text mapOutKey = new Text();
        IntWritable mapOutValue = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, " ");
		String stateName = ((FileSplit)context.getInputSplit()).getPath().getName();
		
		while (tokenizer.hasMoreTokens()){
			String nextWord=new String(tokenizer.nextToken());
			nextWord = nextWord.trim();
             		nextWord = nextWord.toLowerCase();		
			
			if(nextWord.equals("education")||
			   nextWord.equals("sports")||
			   nextWord.equals("politics")||
			   nextWord.equals("agriculture")){
				String stateAndWord = stateName + "*" + nextWord;
                		mapOutKey.set(stateAndWord);
                		context.write(mapOutKey, mapOutValue);
			}
		}
	}
}

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        Text redOutKey = new Text();
        IntWritable redOutValue = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum =0;
		for (IntWritable value : values){
                	sum = sum + value.get();
            	}
            	String stateAndWord = key.toString();
            	String[] splitStateWord = stateAndWord.split("\\*");
            	String stateName = splitStateWord[0];
            	String searchWord = splitStateWord[1];
		String Output = stateName+"-"+searchWord+":";              
            	redOutKey.set(Output);
            	redOutValue.set(sum);
            	context.write(redOutKey, redOutValue);

	}
}

public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{

	Text mapOutKey = new Text();
        Text mapOutValue = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line," ");
		
		
		while (tokenizer.hasMoreTokens()){
			String nextWord=new String(tokenizer.nextToken());
			nextWord = nextWord.trim();
             		nextWord = nextWord.toLowerCase();
			
            		String[] splitLine = nextWord.split("\\-");
            		String stateName = splitLine[0];
            		String searchWordPlusCount = splitLine[1];
			
			String[] splitLine2 = searchWordPlusCount.split("\\t");
            		String searchedWord = splitLine2[0];
            		String wordcount = splitLine2[1];
			String searchWordPlusCount1 = searchedWord+wordcount;
			

			
                		mapOutKey.set(stateName);
				mapOutValue.set(searchWordPlusCount1);
                		context.write(mapOutKey, mapOutValue);
		}
	}
}

public static class Reduce1 extends Reducer<Text, Text, Text, Text>{

        Text redOutKey = new Text();
        Text redOutValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		

		int max =0, secondMax=0, thirdMax=0, fourthMax  = 0;
		  String domWord = new String();
		   String seconddomWord = new String();
			 String thirddomWord = new String();
		 String fourthdomWord = new String();
	for (Text value : values){
		String word = value.toString();

		String[] splitWordCount = word.split("\\:");
            	String searchingWord = splitWordCount[0];
		String searchingCount = splitWordCount[1];
            	int wordCount = Integer.valueOf(searchingCount);

		if (max < wordCount) {
		    fourthMax = thirdMax;
		    fourthdomWord =thirddomWord;
		    thirdMax = secondMax;
		    thirddomWord =seconddomWord;
		    secondMax = max;
		    seconddomWord =domWord;
		    max = wordCount;
		    domWord =searchingWord;
		} else if (secondMax < wordCount) {
		    fourthMax = thirdMax;
		    fourthdomWord =thirddomWord;
		    thirdMax = secondMax;
		    thirddomWord =seconddomWord;
		    secondMax = wordCount;
		    seconddomWord =searchingWord;
		} else if (thirdMax < wordCount) {
		    fourthMax = thirdMax;
		    fourthdomWord =thirddomWord;
		    thirdMax = wordCount;
		    thirddomWord =searchingWord;
		} else if (fourthMax < wordCount) {
		    fourthMax = wordCount;
		    fourthdomWord =searchingWord;
		}
	}

		
            	String Output1 = "-"+domWord+","+seconddomWord+","+thirddomWord+","+fourthdomWord;              
            	redOutKey.set(key.toString());
            	redOutValue.set(Output1);
            	context.write(redOutKey, redOutValue);
	}
}

public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{

	Text mapOutKey = new Text();
        Text mapOutValue = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, " ");
		
		
		while (tokenizer.hasMoreTokens()){
			String nextWord=new String(tokenizer.nextToken());
			nextWord = nextWord.trim();
             		nextWord = nextWord.toLowerCase();

			String[] splitLine2 = nextWord.split("\\-");
            		String state = splitLine2[0];
			String domWordPatter = splitLine2[1];
            		
			mapOutKey.set(domWordPatter);
			mapOutValue.set(state);
                	context.write(mapOutKey, mapOutValue);
			}
		}
	}


public static class Reduce2 extends Reducer<Text, Text, Text, Text>{

        Text redOutKey = new Text();
        Text redOutValue = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String statesPattern = new String();
		for (Text value : values){
                	statesPattern = value.toString() +","+ statesPattern;
            	}
            	String OutputWord = "/"+key.toString()+"/ ->";
            	String Output = "/"+statesPattern+"/";        
            	redOutKey.set(OutputWord);
            	redOutValue.set(Output);
            	context.write(redOutKey, redOutValue);

	}
}

public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
	
	Job job =new Job(conf, "WordCount");
	job.setJarByClass(part2b.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setMapOutputKeyClass(Text.class);
       	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path("/user/inputdata"));
	FileOutputFormat.setOutputPath(job, new Path("/user/hduser/ns632-outputWordCount3"));
	job.waitForCompletion(true);

	
	Job job2 =new Job(conf, "SortCountPattern");
	job2.setJarByClass(part2b.class);
	job2.setMapperClass(Map1.class);
	job2.setReducerClass(Reduce1.class);
	job2.setMapOutputKeyClass(Text.class);
       	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);	
	job2.setInputFormatClass(TextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job2, new Path("/user/hduser/ns632-outputWordCount3"));
	FileOutputFormat.setOutputPath(job2, new Path("/user/hduser/ns632-outputSortCountPattern"));
	job2.waitForCompletion(true);


	Job job4 =new Job(conf, "FinalOutputPart2b");
	job4.setJarByClass(part2b.class);
	job4.setMapperClass(Map2.class);
	job4.setReducerClass(Reduce2.class);
	job4.setMapOutputKeyClass(Text.class);
       	job4.setMapOutputValueClass(Text.class);
	job4.setOutputKeyClass(Text.class);
	job4.setOutputValueClass(Text.class);	
	job4.setInputFormatClass(TextInputFormat.class);
	job4.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job4, new Path("/user/hduser/ns632-outputSortCountPattern"));
	FileOutputFormat.setOutputPath(job4, new Path("/user/hduser/ns632-FinalOutputPart2b"));
	job4.waitForCompletion(true);


}

}


