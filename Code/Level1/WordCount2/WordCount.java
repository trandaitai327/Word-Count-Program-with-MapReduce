package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
//truy cập các arguments trên command line tại run time
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//xử lý một phần input file thay vì toàn bộ
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//thao tác với chuỗi trong hadoop
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WordCount.class);
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    //thêm lớp combiner: lớp này có tác dụng xử lý thông tin trước khi chuyển qua lớp reduce trên từng input split. Nhằm tiết kiệm băng thông và thời gian
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //thêm biến caseSensitive vào class Map
    private boolean caseSensitive = false;
    // đổi split thành \W+ :regex ngăn cách bới các kí tự non-word 1 hoặc nhiều lần
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\W+");

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      //có thể thay đổi giá trị caseSensitive ở cmd line , mặc định là false
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      // caseSensitive=false toàn bộ input split thành chứ thường
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty()) {
            continue;
          }
          currentWord = new Text(word);
          context.write(currentWord,one);
        }         
      }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}
