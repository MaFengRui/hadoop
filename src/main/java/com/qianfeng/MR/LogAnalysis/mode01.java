package com.qianfeng.MR.LogAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class mode01 extends ToolRunner implements Tool {

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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowBean.class);
        //设置reduce的属性

        job.setReducerClass(ReducerModel.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);
        //设置输入输出的参数
        FileInputFormat.setInputPaths(job,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/logdata"));
        FileOutputFormat.setOutputPath(job ,new Path("/media/mafenrgui/办公/马锋瑞/hadoop/target/testdata/logAnalysis01"));
        //提交job
        boolean b = job.waitForCompletion(true);

        return 0;
    }

    public void setConf(Configuration configuration) {

        configuration.set("fs.defaultFS","file:///");
        configuration.set("mapreduce.framework.name","local");

    }

    public Configuration getConf() {
        return new Configuration();
    }
    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new Configuration(),new mode01(),args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
