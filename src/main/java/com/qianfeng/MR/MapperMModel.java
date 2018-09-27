package com.qianfeng.MR;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperMModel extends Mapper{

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {//

    //只在一个ｍａｐ方法前调用一次

      }

    //每读一行之前调用一次
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
