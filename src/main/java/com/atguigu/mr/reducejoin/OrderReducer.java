package com.atguigu.mr.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 数据替换工作
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    /**
     * 收到的数据，pd的一行在开头，order的紧随其后
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //拿到迭代器
        Iterator<NullWritable> iterator = values.iterator();

        //迭代第一组数据
        iterator.next();
        String pname = key.getPname();

        //迭代剩下的数据写出并输出
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key, NullWritable.get());
        }

    }
}
