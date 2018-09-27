package com.qianfeng.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class mode01 extends ToolRunner implements Tool {
   Configuration cnf;
    public int run(String[] args) throws Exception {
        //设置参数
        Configuration configuration = getConf();
        setConf(configuration);
        //设置job
        Job job = Job.getInstance(configuration,"model01");
        //设置job的执行路径
        job.setJarByClass(mode01.class);

        //设置map的属性

        job.setMapperClass(MapperMModel.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置reduce的属性

        job.setReducerClass(ReducerModel.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出的参数
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job ,new Path(args[1]));
        //提交job
        boolean b = job.waitForCompletion(true);

        return 0;
    }

    public void setConf(Configuration configuration) {

        configuration.set("fs.defaultFS","fiel///");
        configuration.set("mapreduceFramework.name","local");


    }

    public Configuration getConf() {
        return new Configuration();
    }
}
