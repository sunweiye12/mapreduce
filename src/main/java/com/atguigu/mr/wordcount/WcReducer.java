package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    /**
     * 框架把数据按照单词分好组输入给我们，我们将同一个单词的次数相加。
     * @param key 单词
     * @param values 这个单词的所有的1
     * @param context 任务本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //做累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        //包装结果并写出去
        result.set(sum);
        context.write(key, result);
    }
}
