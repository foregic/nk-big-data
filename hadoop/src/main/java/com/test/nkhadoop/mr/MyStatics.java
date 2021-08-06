package com.test.nkhadoop.mr;

import com.test.nkhadoop.mr.entity.Table;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MyStatics {
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
    public String getPriceRange(int price){
        if(price<=50){
            return "小于等于50";
        }
        else if(price<=100){
            return "大于50小于等于100";

        }
        else {
            return "高于100";
        }
    }

    public static class WCGenderMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String []strs=line.split(",");
            IntWritable sales = new IntWritable(Integer.parseInt(strs[2])) ;
            int price= Integer.parseInt(strs[3]);
            word.set(strs[0]);
            context.write(word,sales);
            word.set(strs[1]);
            context.write(word,sales);
            if (price<=25){
                word.set("价格不足50");
                context.write(word,sales);
            }
            else {
                word.set("价格高于50");
                context.write(word,sales);
            }
            Map<String, Integer> keywords=new HashMap<>();
            for(int i=4;i<strs.length;i++){
                for(Term s: NlpAnalysis.parse(strs[i])){
                    System.out.println(s.getRealName());
                    if (keywords.containsKey(s.getRealName())){
                        keywords.put(s.getRealName(),keywords.get(s.getRealName())+1);
                    }
                    else{
                        keywords.put(s.getRealName(),1);
                    }
                }
            }
            for (String k:keywords.keySet()){
                word.set(k);
                context.write(word,new IntWritable(keywords.get(k)));
            }
        }
    }

    public static class MSMapper extends Mapper<LongWritable,Text, Table,Text>{
        Table table=new Table();
        Text k =new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取数据
            String line=value.toString();
            String []fields=line.split(",");

            //封装对象
            table.setCommodity(fields[0]);
            table.setBrand(fields[1]);
            table.setSales(Integer.parseInt(fields[2]));
            table.setPrice(Integer.parseInt(fields[3]));
//            context.write(table,);

        }
    }



    public static class WCGenderReducer
            extends Reducer<Text, IntWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 统计 key 对应的 所有的 value 的总和
            long sum = 0;
            for (IntWritable value : values) {
                sum += value.get();     // sum++;
            }
            if (sum<=5) return;
            result.set(sum);
            // 写入最终结果 destMap

            context.write(key, result);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 0. 初始化 MR job
        Configuration configuration = new Configuration();
//        configuration.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(configuration, "statics 1.0");
        // 指明该 mr 使用那个类
        job.setJarByClass(MyStatics.class);

        // 1. 指定输入的位置信息 & 输入文件的类型信息
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // 2. 指定 Mapper 类 & 输出的key&value的类型
        job.setMapperClass(MyStatics.WCGenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 3. 指定 Reducer 类 & 最终输出key&value的类型
//        job.setNumReduceTasks(1);   // 默认 1, 设置为0==不执行 reducer
        job.setReducerClass(MyStatics.WCGenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入的小文件合并成一个大文件

        job.setInputFormatClass(CombineTextInputFormat.class);

        //设置输出的文件为一个大文件


        // 4. 指定输出位置信息 & 输出文件的类型信息
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // 5. 提交 mrjob
         job.submit();
        boolean result = job.waitForCompletion(true);
        boolean isSuccess = job.isSuccessful();
        System.exit(isSuccess ? 0 : 1);
    }
}
