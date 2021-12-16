package com.atguigu.mr.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 将数据封装成FlowBean
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        flow.setPhone(fields[0]);
        flow.set(
                Long.parseLong(fields[1]),      //上行
                Long.parseLong(fields[2])       //下行
        );

        context.write(flow, NullWritable.get());
    }
}
