package com.qianfeng.MR;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    自定义的流量类　
    数据在网络中传输必须考虑序列化与反序列化的输出
    并且这个对像要实现wriable


 */
public class flowBean implements  Writable {

    private  long totalFlow;
    private  long upFlow;
    private  long downFlow;

    public flowBean() {
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }


    public long getTotalFlow() {
        return totalFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {

    }
    public void readFields(DataInput dataInput) throws IOException {

    }
}
