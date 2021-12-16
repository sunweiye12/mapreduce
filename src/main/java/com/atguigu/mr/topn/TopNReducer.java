package com.atguigu.mr.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 取全局前十
 */
public class TopNReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {

    /**
     * 取前十
     * @param key
     * @param values 所有数据一组进来，按照流量降序排列
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();

        //取前十
        for (int i = 0; i < 10; i++) {
            if (iterator.hasNext()) {
                context.write(key, iterator.next());
            }
        }
    }
}
