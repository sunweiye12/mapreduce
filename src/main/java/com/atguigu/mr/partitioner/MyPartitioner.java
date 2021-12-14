package com.atguigu.mr.partitioner;

import com.atguigu.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 对每一条KV对，返回它们对应的分区号
     * @param text 手机号
     * @param flowBean 流量
     * @param numPartitions
     * @return
     */
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //去手机号前三位
        String phone_head = text.toString().substring(0, 3);

        switch (phone_head) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
