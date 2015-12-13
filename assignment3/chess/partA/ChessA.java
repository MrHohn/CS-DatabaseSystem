import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
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

public class ChessA {

  // first map-reduce classes

  public static class ChessFirstMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text res = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      if (input.startsWith("[Result \"0")) {
        res.set("Black");
        context.write(res, one);
      }
      else if (input.startsWith("[Result \"1-")) {
        res.set("White");
        context.write(res, one);
      }
      else if (input.startsWith("[Result \"1/")) {
        res.set("Draw");
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
       extends Mapper<Object, Text, Text, Text>{

    // use the same key, let three records all goto one single reducer
    private Text sameKey = new Text("1");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // emit now
      context.write(sameKey, value);
    }
  }

  public static class ChessSecondReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text newKey = new Text();
    private Text newValue = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int numBlack = 0;
      int numWhite = 0;
      int numDraw  = 0;

      for (Text val : values) {
        Scanner sc = new Scanner(val.toString());
        String winOrLost = sc.next();
        int count = sc.nextInt();
        if (winOrLost.equals("Black")) {
          numBlack = count;
        }
        else if (winOrLost.equals("White")) {
          numWhite = count;
        }
        else {
          numDraw = count;
        }
      }

      // now calculate the percentage
      int numTotal = numBlack + numWhite + numDraw;
      float perBlack = (float)numBlack / (float)numTotal;
      float perWhite = (float)numWhite / (float)numTotal;
      float perDraw = (float)numDraw / (float)numTotal;

      DecimalFormat df = new DecimalFormat();
      df.setMaximumFractionDigits(4);
      // construct the output value
      StringBuilder sb = new StringBuilder();
      sb.append(numBlack);
      sb.append(" ");
      sb.append(df.format(perBlack));
      String emitValue = sb.toString();
      // now emitting black
      newKey.set("Black");
      newValue.set(emitValue);
      context.write(newKey, newValue);

      // construct the output value
      sb = new StringBuilder();
      sb.append(numWhite);
      sb.append(" ");
      sb.append(df.format(perWhite));
      emitValue = sb.toString();
      // now emitting black
      newKey.set("White");
      newValue.set(emitValue);
      context.write(newKey, newValue);

      // construct the output value
      sb = new StringBuilder();
      sb.append(numDraw);
      sb.append(" ");
      sb.append(df.format(perDraw));
      emitValue = sb.toString();
      // now emitting black
      newKey.set("Draw");
      newValue.set(emitValue);
      context.write(newKey, newValue);
    }
  }

  public static void main(String[] args) throws Exception {
    // first map-reduce
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "chess first");
    job1.setJarByClass(ChessA.class);
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
    job2.setJarByClass(ChessA.class);
    job2.setMapperClass(ChessSecondMapper.class);
    job2.setReducerClass(ChessSecondReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}