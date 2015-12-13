import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
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

public class ChessB {

  // first map-reduce classes

  public static class ChessFirstMapper
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

      // proceed to emit the results
      // emits as: Player1id White win  1
      //           Player2id Black win  1
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

  // second map-reduce classes

  public static class ChessSecondMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text keyT = new Text();
    private Text valueT = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String input = value.toString();
      Scanner sc = new Scanner(input);
      String userId = sc.next();
      String color = sc.next();
      String winOrLost = sc.next();
      int count = sc.nextInt();

      // now construct the key: userId + color
      StringBuilder sb = new StringBuilder(userId);
      sb.append(" ");
      sb.append(color);
      String emitKey = sb.toString();
      // now construct the value: win + count
      sb = new StringBuilder(winOrLost);
      sb.append(" ");
      sb.append(count);
      String emitValue = sb.toString();

      // emit now
      keyT.set(emitKey);
      valueT.set(emitValue);
      context.write(keyT, valueT);
    }
  }

  public static class ChessSecondReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text newKey = new Text();
    private Text newValue = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int numWin  = 0;
      int numLose = 0;
      int numDraw = 0;

      for (Text val : values) {
        Scanner sc = new Scanner(val.toString());
        String winOrLost = sc.next();
        int count = sc.nextInt();
        if (winOrLost.equals("win")) {
          numWin += count;
        }
        else if (winOrLost.equals("lose")) {
          numLose += count;
        }
        else {
          numDraw += count;
        }
      }

      // now calculate the percentage
      int numTotal = numWin + numLose + numDraw;
      float perWin = (float)numWin / (float)numTotal;
      float perLose = (float)numLose / (float)numTotal;
      float perDraw = (float)numDraw / (float)numTotal;

      DecimalFormat df = new DecimalFormat();
      df.setMaximumFractionDigits(4);
      // construct the output value
      StringBuilder sb = new StringBuilder();
      sb.append(key.toString());
      sb.append(" ");
      sb.append(df.format(perWin));
      sb.append(" ");
      sb.append(df.format(perLose));
      sb.append(" ");
      sb.append(df.format(perDraw));
      String emitValue = sb.toString();

      // now emitting
      newKey.set(emitValue);
      newValue.set("");
      context.write(newKey, newValue);
    }
  }

  public static void main(String[] args) throws Exception {
    // first map-reduce
    Configuration conf1 = new Configuration();
    // need to change the delimiter since a mapper need a whole event
    conf1.set("textinputformat.record.delimiter", "\n[Event");
    Job job1 = Job.getInstance(conf1, "chess first");
    job1.setJarByClass(ChessB.class);
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
    job2.setJarByClass(ChessB.class);
    job2.setMapperClass(ChessSecondMapper.class);
    job2.setReducerClass(ChessSecondReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}