package com.qianfeng.MR.groupMaxCount;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class secondSortBean implements WritableComparable<secondSortBean>{
    private int frist;
    private int second;

    public secondSortBean() {
    }
    public secondSortBean(int key, int value) {
        this.frist = key;
        this.second = value;
    }

    public int getKey() {
        return frist;
    }

    public int getValue() {
        return second;
    }

    public void setKey(int key) {
        this.frist = key;
    }

    public void setValue(int value) {
        this.second = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(frist);
        dataOutput.write(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        frist = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        int result  = frist;
        result = 31*frist+second;
        return  result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return  true;
        if (obj == null || obj.getClass()!= this.getClass()) return  false;

        secondSortBean secondSortBean = (secondSortBean) obj;

        if (this.frist != secondSortBean.frist) return false;

        return second == secondSortBean.second;
    }

    @Override
    public int compareTo(secondSortBean o) {
        int tem = this.frist - o.frist; //当前对象－传进来的对象是升序
        //int tem = o.frist - this.frist; //降序
        if (tem != 0 ){
            return tem;
        }
//        return this.second - o.second; //second 是升序　
        return  o.second - this.second; //降序
    }

    @Override
    public String toString() {
        return "secondSortBean{" +
                "frist=" + frist +
                ", second=" + second +
                '}';
    }
}
