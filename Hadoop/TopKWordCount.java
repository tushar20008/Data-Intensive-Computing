import java.io.IOException;
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


public class TopKWordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
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

  static <K,V extends Comparable<? super V>>
  SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
    SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
      new Comparator<Map.Entry<K,V>>() {
        @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
          int res = e2.getValue().compareTo(e1.getValue());
          return res != 0 ? res : 1;
        }
      }
    );
    sortedEntries.addAll(map.entrySet());
    return sortedEntries;
  }


//2nd MAPPER

public static class IdMapper
     extends Mapper<Object, Text, Text, Text>{


  Configuration conf2;
  private Text word_freq = new Text();
  private Text k    ;


  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
   //getting the arguments from the user and passing it to the mapper
   conf2 = context.getConfiguration();
   k = new Text(conf2.get("numResults"));// take user input for the top k values to print 

    word_freq.set(value);
    context.write(k, word_freq);// set the top k as the dummy value to be read by the reducer

  }
}

//2nd Reducer

public static class IntMaxReducer
     extends Reducer<Text,Text,Text,Text> {
  private Text result = new Text();
  private String max_word;


  public void reduce(Text key, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {

    Map< String,Integer> map = new HashMap<String,Integer>();// map to store all the word frequecies 

    //Iterating over the values and filling the hashmap that is to be sorted
    for (Text val : values) {

      StringTokenizer itr = new StringTokenizer(val.toString());
      int counter = 0;
      String word = "", freq = "";

      while (itr.hasMoreTokens()) {

        if(counter == 0)
          word= itr.nextToken();
        else if (counter == 1)
          freq= itr.nextToken();
        
        counter++;
      }
      map.put( word , Integer.parseInt(freq) );

    }
    //emit only the first k entires of the SortedSet by using the entriesSortedByValues(map)
    Iterator<Entry< String,Integer>> mapIterator = entriesSortedByValues(map).iterator();
    int counter = 0;// counter variable to keep track of the number of values emitted (we need only k values/ up to k values)
    while (mapIterator.hasNext() && counter<Integer.parseInt(key.toString())) {

        Map.Entry<String,Integer> entry = mapIterator.next();
        context.write(key,new Text(entry.getKey()+"\t"+entry.getValue()));

        counter++;

      }

  }
}



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TopKWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);


    Configuration conf2 = new Configuration();
    conf2.set("numResults", args[3]);
    Job job2 = Job.getInstance(conf2, "word count");

    // Insert your Code Here
    job2.setJarByClass(TopKWordCount.class);
    job2.setMapperClass(IdMapper.class);
    job2.setCombinerClass(IntMaxReducer.class);
    job2.setReducerClass(IntMaxReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);


    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);


  }
}
/** 
Output:

10	the	47445
10	and	31898
10	to	27975
10	of	26246
10	a	16842
10	in	14250
10	was	12433
10	his	11643
10	that	11530
10	he	10868

*/

/**

	Question 1: How do Mappers and Reducers accept user-defined parameters?
	Answer 1: We need to set up the key value pair as a configuration by using the Configuration class as shown below:
            Configuration conf = new Configuration();
            conf.set("key", value from the user input);

            then when the Mapper or reducer has to acces the user input we just call the get function in the Configuration class to get the value as shown below:

            Configuration conf = context.getConfiguration();
            conf.get("key");
*/
