package com.atguigu.mr.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 取每个订单的最高价格
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    /**
     * 取每个订单的前二高价格
     * @param key 订单信息
     * @param values 啥也没有
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {
            if (iterator.hasNext()) {
                NullWritable value = iterator.next();
                context.write(key, value);
            }
        }
    }
}
