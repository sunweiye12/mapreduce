package com.atguigu.mr.compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CompareMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    private Text phone = new Text();
    private FlowBean flow = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //一行数据
        String line = value.toString();

        //切分
        String[] fields = line.split("\t");

        //封装
        phone.set(fields[0]);
        flow.setUpFlow(Long.parseLong(fields[1]));
        flow.setDownFlow(Long.parseLong(fields[2]));
        flow.setSumFlow(Long.parseLong(fields[3]));

        //写出去
        context.write(flow, phone);
    }
}
