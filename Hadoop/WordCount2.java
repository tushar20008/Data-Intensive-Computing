import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.util.Map.Entry;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	// Local in map aggregation (this aggregation is performed in each map task and is not global among all the map tasks)
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      Map<String, Integer> map = new HashMap<String, Integer>();// hash map for local aggregation for each map task
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
   	    String token = itr.nextToken();

   	    if(map.containsKey(token))  
   		   map.put(token, map.get(token) + 1);
        else
   		   map.put(token, 1);
    }

      Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();

      while(it.hasNext()) {
       Map.Entry<String, Integer> entry = it.next();
       context.write(new Text(entry.getKey()), new IntWritable(entry.getValue().intValue()));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String temp_path = "temp";

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper.class);

    //Setting up a combiner with the same Reducer class
    job.setCombinerClass(IntSumReducer.class);

    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
