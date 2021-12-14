package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable,Text,Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);
    /**
     * 框架将数据拆成一行一行输入进来，我们把数据变成（单词，1）的形式
     * @param key 行号
     * @param value 行内容
     * @param context 任务本身
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到一行数据
        String line = value.toString();

        //将这一行拆成很多单词
        String[] words = line.split(" ");

        //将（单词，1）写回框架
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, this.one);
        }

    }
}
