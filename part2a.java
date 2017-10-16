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

public class part2a {

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
		
		
		  int max = 0;
		  String domWord = new String();
	for (Text value : values){
		String word = value.toString();

		String[] splitWordCount = word.split("\\:");
            	String searchingWord = splitWordCount[0];
		String searchingCount = splitWordCount[1];
            	int wordCount = Integer.valueOf(searchingCount);
		if (wordCount>max){
			max=wordCount;
			domWord =searchingWord;
		}
	}

		
            	String Output1 = "-"+domWord+":"+String.valueOf(max);              
            	redOutKey.set(key.toString());
            	redOutValue.set(Output1);
            	context.write(redOutKey, redOutValue);

	}
}




public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text mapOutKey = new Text();
        IntWritable mapOutValue = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, " ");
		
		
		while (tokenizer.hasMoreTokens()){
			String nextWord=new String(tokenizer.nextToken());
			nextWord = nextWord.trim();
             		nextWord = nextWord.toLowerCase();

			String[] splitLine2 = nextWord.split("\\-");
            		String SearchWord = splitLine2[1];
            		String[] nextWordTmp = SearchWord.split("\\:");
			nextWord = nextWordTmp[0];
			
			if(nextWord.equals("education")||
			   nextWord.equals("sports")||
			   nextWord.equals("politics")||
			   nextWord.equals("agriculture")){
				mapOutKey.set(nextWord);
                		context.write(mapOutKey, mapOutValue);
			}
		}
	}
}

public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable>{

        Text redOutKey = new Text();
        IntWritable redOutValue = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum =0;
		for (IntWritable value : values){
                	sum = sum + value.get();
            	}
            	String OutputWord = key.toString();
            	String Output = "No. of states where word "+OutputWord+" is dominant:";        
            	redOutKey.set(Output);
            	redOutValue.set(sum);
            	context.write(redOutKey, redOutValue);

	}
}


public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
	Job job =new Job(conf, "WordCount");
	job.setJarByClass(part2a.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setMapOutputKeyClass(Text.class);
       	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job, new Path("/user/inputdata"));
	FileOutputFormat.setOutputPath(job, new Path("/user/hduser/ns632-OutputWordCount"));
	job.waitForCompletion(true);

	Job job2 =new Job(conf, "FindDomWord");
	job2.setJarByClass(part2a.class);
	job2.setMapperClass(Map1.class);
	job2.setReducerClass(Reduce1.class);
	job2.setMapOutputKeyClass(Text.class);
       	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);	
	job2.setInputFormatClass(TextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job2, new Path("/user/hduser/ns632-OutputWordCount"));
	FileOutputFormat.setOutputPath(job2, new Path("/user/hduser/ns632-FindDomWord"));
	job2.waitForCompletion(true);

	Job job3 =new Job(conf, "FinalOutputPart2a");
	job3.setJarByClass(part2a.class);
	job3.setMapperClass(Map2.class);
	job3.setReducerClass(Reduce2.class);
	job3.setMapOutputKeyClass(Text.class);
       	job3.setMapOutputValueClass(IntWritable.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(IntWritable.class);	
	job3.setInputFormatClass(TextInputFormat.class);
	job3.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job3, new Path("/user/hduser/ns632-FindDomWord"));
	FileOutputFormat.setOutputPath(job3, new Path("/user/hduser/ns632-FinalOutputPart2a"));
	job3.waitForCompletion(true);

}
}

