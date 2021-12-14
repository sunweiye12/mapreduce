package com.atguigu.mr.compare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 收到的数据是手机号在后，输出手机号在前
 */
public class CompareReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    /**
     * Reduce收到的数据已经排完序了，我们反过来输出就完事了
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
