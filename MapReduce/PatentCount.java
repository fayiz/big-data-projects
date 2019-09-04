/*
	Apply your MapReduce programming knowledge and write a MapReduce program to process a dataset with patent records. 
	You need to calculate the number of sub - patents associated with each patent.
*/
/*
//Sample data
1 1.160
1 1.205
1 1.68
1 1.92
2 2.179
2 2.206
2 2.59
2 2.111
2 2.163
2 2.12
2 2.93
2 2.75
2 2.20
2 2.29
3 3.233
3 3.171
3 3.197
3 3.40
3 3.201
*/


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class PatentCount{
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "patentc");
    	
    	job.setJarByClass(WordCount.class);
    	job.setMapperClass(Map.class);
    	job.setReducerClass(Reduce.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	Path outputPath = new Path(args[1]);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	outputPath.getFileSystem(conf).delete(outputPath);
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class Map extends Mapper<Object, Text, Text, IntWritable>{
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
    		String record = value.toString();
			String[] parts = record.split(" ");
			context.write(new Text(parts[0]), new IntWritable(1));
    	}
    }
	
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
    		int sum = 0;
    		for(IntWritable x: values) {
    			sum += x.get();
    		}
    		context.write(key, new IntWritable(sum));
    	}
    }
    
}