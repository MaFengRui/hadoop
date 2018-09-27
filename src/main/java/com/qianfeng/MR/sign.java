package com.qianfeng.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

public class sign {
    static  class  signMapper   extends  Mapper<LongWritable, Text, Text, Text>  {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String c = value.toString();
             int  b = c.charAt(0);

             if (b <= 'z' && b >= 'a'){
                 context.write(new Text("lower"),value);
             } else if  (b <= 'Z' && b >= 'A'){
                 context.write(new Text("upper"),value);
             } else if  (b <= '9' && b >= '0'){
                 context.write(new Text("number"),value);
             }else  {
                 context.write(new Text("other"),value);
             }
        }


    }

    static class  signreduce extends Reducer<Text, Text, Text, Text>{
        private  MultipleOutputs<Text,Text> mos = null;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           mos = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String string = key.toString();

            if (string.equalsIgnoreCase("lower")){
                for (Text text : values){
                    context.write(null,text);
                    mos.write("lower",key,text);
                }

            }else if (string.equalsIgnoreCase("upper")){
                for (Text text : values){
                    context.write(null,text);
                    mos.write("upper",null,text);
                }


            } else if (string.equalsIgnoreCase("number")){
                for (Text text : values){
                    context.write(null,text);
                    mos.write("number",key,text);
                }

            } else if (string.equalsIgnoreCase("other")){
                for (Text text : values){
                    context.write(null,text);
                    mos.write("other",key,text);
                }


            }



        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、创建一个Configuration配置项
        Configuration conf = new Configuration();

        //2、配置连接参数
        conf.set("fs.defaultFS","hdfs://192.168.37.83:9000");


        //3、创建一个job对象
        Job job = Job.getInstance(conf,"sign");

        //4、描述对象
        //5、设置Job的执行路径
        job.setJarByClass(sign.class);

        //6、设置mapTask调用的业务逻辑类
        job.setMapperClass(sign.signMapper.class);
        //7、设置map端数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //8、设置mapTask调用的业务类
        job.setReducerClass(sign.signreduce.class);

        //9、设置reduce端的数据的输出类型
        job.setOutputKeyClass(File.class);
        job.setOutputValueClass(Text.class);

        //10、设置Job的输入文件的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //11、设置Job的输出文件的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        MultipleOutputs.addNamedOutput(job, "lower", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "upper", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "number", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "other", TextOutputFormat.class, Text.class, Text.class);

        //12、提交job
//        job.submit();
        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);
    }

}
