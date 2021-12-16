package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean order = new OrderBean();
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取输入数据的文件名
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //切分一行内容
        String[] fields = value.toString().split("\t");

        //封装, 按照数据来源不同本别封装
        if ("order.txt".equals(filename)) {
            //封装order
            order.setId(fields[0]);
            order.setPid(fields[1]);
            order.setAmount(Integer.parseInt(fields[2]));
            order.setPname("");
        } else {
            //封装pd
            order.setPid(fields[0]);
            order.setPname(fields[1]);
            order.setAmount(0);
            order.setId("");
        }

        context.write(order, NullWritable.get());

    }
}
