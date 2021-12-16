package com.atguigu.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Counter pass;
    private Counter fail;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pass = context.getCounter(ETL.PASS);
        fail = context.getCounter(ETL.FAIL);
    }

    /**
     * 判断日志需不需要清洗
     * @param key
     * @param value 一行日志
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //切分
        String[] splits = value.toString().split(" ");

        //只有长度大于11的我们才要
        if (splits.length > 11) {
            context.write(value, NullWritable.get());
            pass.increment(1);
        } else {
            fail.increment(1);
        }

    }
}
