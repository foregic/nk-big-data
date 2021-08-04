package com.test.nkhadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class MyWC01 {
    // 如果是 windows OS, 设置 环境变量
    // only for based windows run|debug MR
    static {
        // 判断当前执行环境是否为 windows,如果是,
        if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1) {
            // 设置远程操作 hdfs 的用户
            System.setProperty("HADOOP_USER_NAME", "vagrant");
            // 设置下载的winutils中的对应版本的 windows hadoop 可执行文件的位置
            // 位置: 可执行文件所在的 bin 目录的所在的位置
            System.setProperty("hadoop.home.dir", "d:/Dev/hadoop/hadoop3.2.2");
            // 设置没有空格和中文的路径作为 mr 的临时目录
            System.setProperty("hadoop.tmp.dir", "d:/rtmp");
        }
    }

    //
    public static class WCGenderMapper
        extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 0. 获取每一行字符串, 来源于输入的|srcMap的 value
            // value 是 Text 类型, 转换为 Java String 类型
            String line = value.toString();
            // 1. 将每一行文本,切分|拆分为 一个个 的单词
            String [] strs = line.split(" ");
            // 2-1. 逐个获取每一个单词
            for (String str : strs){
                // ==> <str, 1>
                // 2-2. 输出到 mapper 的输出的 map(midMap<行中的每一个单词, 1>)中
                word.set(str);
                context.write(word, one);
            }
        }
    }

    public static class WCGenderReducer
        extends Reducer<Text, IntWritable, Text, LongWritable>{
        private LongWritable result = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 统计 key 对应的 所有的 value 的总和
            long sum = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                IntWritable value = iterator.next();
                sum += value.get();     // sum++;
            }
            result.set(sum);
            // 写入最终结果 destMap
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 0. 初始化 MR job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "MyWordCount 1.0");
        // 指明该 mr 使用那个类
        job.setJarByClass(MyWC01.class);

        // 1. 指定输入的位置信息 & 输入文件的类型信息
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // 2. 指定 Mapper 类 & 输出的key&value的类型
        job.setMapperClass(WCGenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 3. 指定 Reducer 类 & 最终输出key&value的类型
        job.setNumReduceTasks(2);   // 默认 1, 设置为0==不执行 reducer
        job.setReducerClass(WCGenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 4. 指定输出位置信息 & 输出文件的类型信息
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // 5. 提交 mrjob
        // job.submit();
        boolean result = job.waitForCompletion(true);
        boolean isSuccess = job.isSuccessful();
        System.exit(isSuccess ? 0 : 1);
    }

}
