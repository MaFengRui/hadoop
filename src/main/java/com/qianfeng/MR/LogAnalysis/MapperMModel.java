package com.qianfeng.MR.LogAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//数据：

//需求：统计用户上下行总流量　　


public class MapperMModel extends Mapper<LongWritable,Text,Text,flowBean>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        flowBean flowBean = new flowBean();
        String string = value.toString();
        String[] split = string.split(" ");
        flowBean.setDownFlow((long) Long.parseLong(split[split.length-3]));
        flowBean.setUpFlow((long) Long.parseLong(split[split.length-4]));
//        flowBean.setTotalFlow((long) Long.parseLong(split[split.length-3])+Long.parseLong(split[split.length-4]));

        context.write(new Text(split[1]),flowBean);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }


}
