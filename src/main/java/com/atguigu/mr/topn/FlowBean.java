package com.atguigu.mr.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现WritableComparable接口
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private String phone;
    private long upFlow;
    private long downFlow;
    private long sumFlow;


    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Override
    public String toString() {
        return phone + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 将对象数据写出到框架指定地方
     * @param dataOutput 数据的容器
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
        dataOutput.writeUTF(phone);
    }

    /**
     * 从框架指定地方读取数据填充对象
     * @param dataInput 数据的容器
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
        this.phone = dataInput.readUTF();
    }

    /**
     * 比较方法，按照总流量降序排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        return Long.compare(o.sumFlow, this.sumFlow);
    }
}
