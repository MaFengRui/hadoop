package com.qianfeng.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实现wordcount计数
 * KEYIN
 * VALUEIN：是框架(maptask)要传给map方法的输入参数中的value的数据类型
 * 在默认情况，框架传入的key，是文本某一行的long
 * value　是从这一行的内容　默认String
 * 但是　long 或者　string 原生态数据类型　序列化效率比较低，所以hadoop
 * 对其进行封装了　成了　Longwriable,Text
 * map处理完数据要返回一个数据（key,value）
 * KEYOUT:
 * VALUEOUT:
 *
 *
 * 解题思路
 * map阶段的处理逻辑；
 * １、用空格切分单词
 * ２、循环遍历单词
 * ３、输出结果
 *
 *
 * map方法调用规律：一行一个ｍａｐ
 *
 * 第一步；数据类型转换
 *
 * ２、切分数据
 *
 * ３、循环遍历　
 *
 * ４、输出结果　：利用context
 *
 *
 *Recuce 处理逻辑（KEYIN,VALUEIN,KEYOUT,VALUEOUT）
 * 　reduce(
 ** 1、KEYIN:要传给reduce方法输入参数的key 的数据类型
 * 2、VALUE
 *３、KEYOUT
 * 4\VALUEOUT
 *
 *
 * reduce调用的规律：框架会从map阶段输出结果的key相同，
 *为什么用迭代器？
 * reduce 端的输入数据
 *
 *最后写驱动类
 * １、创建一个配置对象
 * ２、配置连接参数
 *
 * //在高可用中
 *
 * 3、创建一个ｊｏｂ对象　获得一个实例
 * ４、描述对象
 * ５、设置job的执行路径（反射）
 *　６、设置mapTask调用的业务逻辑类
 * ７、设置map端数据输出的类型
 * 8、设置mapTask调用的业务逻辑类
 * ９、设置reduce端的数据输出类型
 *　10、设置job输入文件的路径
 * 11、设置job输出文件的路径
   12、提交job
  job.submit 客户端得不到信息
 job.waitForCompletion
  13、返回状态

 */
public class Wordcout {
    static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] strings = lines.split(" ");
            for (String s :
                    strings) {
                context.write(new Text(s), new LongWritable(1));
            }
        }
    }

    static class WCReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable i : values) {
                count+= i.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(configuration, "Wordcout");

        //5、设置Job的执行路径
        job.setJarByClass(Wordcout.class);

        //6、设置mapTask调用的业务逻辑类
        job.setMapperClass(WCMapper.class);

        //7、设置map端数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //8、设置mapTask调用的业务类
        job.setReducerClass(WCReduce.class);

        //9、设置reduce端的数据的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //10、设置Job的输入文件的路径
        FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/wc.txt"));

        //11、设置Job的输出文件的路径
        FileOutputFormat.setOutputPath(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/wcOutput"));

        Boolean b  = job.waitForCompletion(true);
        System.exit(b ? 0:1);
    }
}




