import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.lang.StringBuilder;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessC {

  public static class ChessFirstMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text res = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      if (input.startsWith("[Ply")) {
        Scanner sc = new Scanner(input);
        String temp = sc.findInLine(Pattern.compile("\".*\""));
        String stepString = temp.substring(1, temp.length() - 1);
        res.set(stepString);
        context.write(res, one);
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

  // second map-reduce classes

  public static class ChessSecondMapper
       extends Mapper<Object, Text, ChessPairWritable, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Scanner sc = new Scanner(value.toString());
      // skip the number of steps
      sc.next();
      int count = sc.nextInt();
      ChessPairWritable pair = new ChessPairWritable(count);
      // emit now, set the count as secondary key for sorting purpose
      context.write(pair, value);
    }
  }

  public static class ChessSecondReducer
       extends Reducer<ChessPairWritable,Text,Text,Text> {

    private Text newKey = new Text();
    private Text newValue = new Text();

    public void reduce(ChessPairWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // define a container to store all inputs
      ArrayList<String> container = new ArrayList<String>();
      int totalCount = 0;

      for (Text val : values) {
        Scanner sc = new Scanner(val.toString());
        // skip the number of steps
        sc.nextInt();
        int count = sc.nextInt();
        totalCount += count;
        container.add(val.toString());
      }

      // now emit all the value in order
      for (int i = 0; i < container.size(); ++i) {
        Scanner sc = new Scanner(container.get(i));
        String step = sc.next();
        int count = sc.nextInt();
        // calculate the percentage
        float perCount = (float)count / (float)totalCount * 100;

        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(4);
        // construct the output
        StringBuilder sb = new StringBuilder();
        sb.append(df.format(perCount));
        sb.append("%");
        newKey.set(step);
        newValue.set(sb.toString());
        context.write(newKey, newValue);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // first map-reduce
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "chess first");
    job1.setJarByClass(ChessC.class);
    job1.setMapperClass(ChessFirstMapper.class);
    job1.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(IntSumReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.waitForCompletion(true);

    // second map-reduce
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "chess second");
    job2.setJarByClass(ChessC.class);
    job2.setMapperClass(ChessSecondMapper.class);
    // self defined Partiioner, PairWritable and GroupingComparator
    job2.setMapOutputKeyClass(ChessPairWritable.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setPartitionerClass(ChessPairPartitioner.class);
    job2.setSortComparatorClass(ChessPairSortComparator.class);
    job2.setGroupingComparatorClass(ChessPairGroupingComparator.class);
    job2.setReducerClass(ChessSecondReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}