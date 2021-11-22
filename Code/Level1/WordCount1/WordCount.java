package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;//trích xuất từ trong file input
//thư viện cấu hình và chạy mapreduce của hadoop
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//tạo và tùy chỉnh các tiến trình Map và Reduce
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//truy cập các files trong HDFS
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//các phương thức đọc, ghi, so sánh các giá trị trong các tiền trình map và reduce
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//thông báo lỗi hoặc thành công trong các lớp mapper và reducer
import org.apache.log4j.Logger;

//tạo class WordCount
public class WordCount extends Configured implements Tool {
  //khởi tạo logger
  private static final Logger LOG = Logger.getLogger(WordCount.class);
  //tạo và chạy wordcount mới với tham số truyền vào là arguments ở command line
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res); //trả về System số nguyên res khi chương trình hoàn thành
  }
  //khởi tạo và chạy Job
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass()); // sử dụng file jar
    //lấy các đưuòng dẫn của input và output 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //đặt lớp map và reduce cho job
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    //xuất ra dưới dạng key value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
  //lớp Map
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;
    //split các từ có khoảng trắng ở giữa, word boundary này có thể tách được spaces, tab và chấm cuối dòng.
    //\b ranh giới của 1 từ, \s là ký tự trắng (xuống dòng,), \s* 0 hoặc nhiều \s
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString(); //input split là 1 dòng
      Text currentWord = new Text();  //từ (key)
      for (String word : WORD_BOUNDARY.split(line)) { //tách các từ
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word);
            context.write(currentWord,one); //được ghi lại dưới dạng(key : 1)
        }
    }
  }
  //lớp Reduce
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    //chạy trên từng cặp key-value
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      //tính tổng các value của key
      for (IntWritable count : counts) {
        sum += count.get();
      }
      //ghi cặp key-value mới vào context object
      context.write(word, new IntWritable(sum));
    }
  }
}
