package com.test.nkhadoop.mr;

import com.test.nkhadoop.mr.entity.KeyWord;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.checkerframework.checker.units.qual.K;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KeyWordCount {
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
    public static class KWCMapper extends Mapper<LongWritable,Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[]fields=line.split(",");

            Map<String, Integer> keywords=new HashMap<>();
            for(int i=4;i<fields.length;i++){
                for(Term s: NlpAnalysis.parse(fields[i])){
//                    System.out.println(s.getRealName());
                    if (keywords.containsKey(s.getRealName())){
                        keywords.put(s.getRealName(),keywords.get(s.getRealName())+1);
                    }
                    else{
                        keywords.put(s.getRealName(),1);
                    }
                }
            }

            for (String k:keywords.keySet())
                context.write(new Text(fields[0] + "\t" + k), new IntWritable(keywords.get(k)));
        }
    }

    public static class KWCReducer extends Reducer<Text,IntWritable,Text,KeyWord>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable iter:values){
                sum+=iter.get();
            }
            String[] line= key.toString().split("\t");
            String commodity=line[0];
            String keyword=line[1];
            KeyWord kw=new KeyWord(keyword,sum);
            context.write(new Text(commodity),kw);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator",",");

        Job job2 = Job.getInstance(conf, "SalesCount");

        //指定本程序的jar包所在的路径
        job2.setJarByClass(KeyWordCount.class);

        //指定本业务所使用的的mapper/reducer类
        job2.setMapperClass(KeyWordCount.KWCMapper.class);
        job2.setReducerClass(KeyWordCount.KWCReducer.class);

        //指定mapper输出的k&v数据类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        //指定reducer输出的k&v数据类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(KeyWord.class);



        //设置输入文件路径和输出文件路径
        FileInputFormat.setInputPaths(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/kwc"));

        boolean result = job2.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }

}
