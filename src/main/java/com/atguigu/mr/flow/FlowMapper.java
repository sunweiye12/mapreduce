package com.atguigu.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到一行数据
        String line = value.toString();
        //切分
        String[] fields = line.split("\t");

        //封装
        phone.set(fields[1]);
        flow.set(
                Long.parseLong(fields[fields.length-3]),  //upFlow
                Long.parseLong(fields[fields.length-2])   //downFlow
        );

        context.write(phone, flow);
    }
}
