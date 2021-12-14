package com.atguigu.mr.grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 封装OrderBean
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean order = new OrderBean();
    /**
     * Map用来封装OrderBean
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拆分
        String[] fields = value.toString().split("\t");

        //封装
        order.setOrderId(fields[0]);
        order.setProductId(fields[1]);
        order.setPrice(Double.parseDouble(fields[2]));

        context.write(order, NullWritable.get());
    }
}
