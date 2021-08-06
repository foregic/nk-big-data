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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SalesCount {
    static {
        // 判断当前执行环境是否为 windows,如果是,
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            // 设置远程操作 hdfs 的用户
            System.setProperty("HADOOP_USER_NAME", "vagrant");
            // 设置下载的winutils中的对应版本的 windows hadoop 可执行文件的位置
            // 位置: 可执行文件所在的 bin 目录的所在的位置
            System.setProperty("hadoop.home.dir", "D:\\Dev\\hadoop\\hadoop-3.2.2");
            // 设置没有空格和中文的路径作为 mr 的临时目录
            System.setProperty("hadoop.tmp.dir", "d:/mrtmp");
        }
    }

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            //商品、品牌、价格销量表
            Text commodity = new Text(fields[0]);
            Text brand = new Text(fields[1]);
            Text price = new Text(fields[3]);

            IntWritable sales = new IntWritable(Integer.parseInt(fields[2]));
            context.write(price, sales);
            context.write(commodity, sales);
            context.write(brand, sales);
        }
    }

    public static class SalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable in : values) {
                sum += in.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "SalesCount");

        //指定本程序的jar包所在的路径
        job1.setJarByClass(SalesCount.class);

        //指定本业务所使用的的mapper/reducer类
        job1.setMapperClass(SalesMapper.class);
        job1.setReducerClass(SalesReducer.class);

        //指定mapper输出的k&v数据类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        //指定reducer输出的k&v数据类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        //设置输入文件路径和输出文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/sc"));

        boolean result = job1.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
