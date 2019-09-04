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

public class MaxTemperature{
	
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = new Job(conf, "patentc");
    	
    	job.setJarByClass(MaxTemperature.class);
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
			context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
    	}
    }
	
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
    		int max = 0;
    		for(IntWritable x: values) {
    			if(x.get()>max)
    				max = x.get();
    		}
    		context.write(key, new IntWritable(max));
    	}
    }
    
}