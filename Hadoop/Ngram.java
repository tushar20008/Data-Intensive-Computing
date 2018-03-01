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

public class Ngram {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    static int cnt = 0;

    List wordList = new ArrayList();// store a global list of words
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens())
   	    wordList.add(itr.nextToken());

    }
// We run the n_gram generation code in the cleanup process because of the fact it helps run faster as we have to execute it only once and not in all map tasks.
	@Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {

  			int ngram_value = Integer.parseInt(context.getConfiguration().get("grams"));
  			StringBuffer str = new StringBuffer("");

  			for (int i = 0; i < wordList.size() - ngram_value; i++)
        {

  				int k=i;

  				for(int j=0;j<ngram_value;j++) 
          {
  					 if(j>0) 
             {
              // add a space between words
  						str = str.append(" ");
  						str = str.append(wordList.get(k));
  					} 
            else
  						str = str.append(wordList.get(k));

  					k++;
  				}

  				word.set(str.toString());
  				str=new StringBuffer("");
  				context.write(word, one);
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
    conf.set("grams", args[2]);
    String temp_path = "temp";

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Ngram.class);
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
