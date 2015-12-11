import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChessB {

  public static class ChessResultMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text res = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      // first search for the white user
      Pattern pt = Pattern.compile("White \".*\"");
      Matcher mc = pt.matcher(input);
      if (!mc.find()) {
        return;
      }
      String temp = mc.group();
      String whiteId = temp.substring(7, temp.length() - 1);
      // next search for the black user
      pt = Pattern.compile("Black \".*\"");
      mc = pt.matcher(input);
      if (!mc.find()) {
        return;
      }
      temp = mc.group();
      String blackId = temp.substring(7, temp.length() - 1);
      // now search for the result
      pt = Pattern.compile("Result \".*\"");
      mc = pt.matcher(input);
      if (!mc.find()) {
        return;
      }
      temp = mc.group();
      String winOrLose = temp.substring(8, temp.length() - 1);
      if (winOrLose.startsWith("0")) {
        // black wins
        StringBuilder sb = new StringBuilder(whiteId);
        sb.append(" White lose");
        res.set(sb.toString());
        context.write(res, one);
        sb = new StringBuilder(blackId);
        sb.append(" Black win");
        res.set(sb.toString());
        context.write(res, one);
      }
      else if (winOrLose.startsWith("1-")) {
        // white wins
        StringBuilder sb = new StringBuilder(whiteId);
        sb.append(" White win");
        res.set(sb.toString());
        context.write(res, one);
        sb = new StringBuilder(blackId);
        sb.append(" Black lose");
        res.set(sb.toString());
        context.write(res, one);
      }
      else {
        // draw
        StringBuilder sb = new StringBuilder(whiteId);
        sb.append(" White draw");
        res.set(sb.toString());
        context.write(res, one);
        sb = new StringBuilder(blackId);
        sb.append(" Black draw");
        res.set(sb.toString());
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("textinputformat.record.delimiter", "\n[Event");
    Job job = Job.getInstance(conf, "chess result count");
    job.setJarByClass(ChessB.class);
    job.setMapperClass(ChessResultMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}