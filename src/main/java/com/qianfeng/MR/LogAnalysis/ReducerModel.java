package com.qianfeng.MR.LogAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class ReducerModel extends Reducer<Text,flowBean,Text,flowBean>{
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }
    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {

            Iterator<flowBean> iterator = values.iterator();
            long up = 0;
            long down = 0;
            while (iterator.hasNext()){
                flowBean flowBean = iterator.next();
                up += flowBean.getUpFlow();
                down += flowBean.getDownFlow();
            }
            flowBean flowBean = new flowBean(up,down);

             context.write(key,flowBean);

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
