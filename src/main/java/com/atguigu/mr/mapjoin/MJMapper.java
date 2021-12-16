package com.atguigu.mr.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<>();

    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //读取pd到pMap
        //开流
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream pd = fileSystem.open(new Path(cacheFiles[0]));

        //将文件按行处理，读取到pMap中
        BufferedReader br = new BufferedReader(new InputStreamReader(pd));
        String line;
        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t");
            pMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStream(br);

    }

    /**
     * 处理order.txt的数据
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        k.set(fields[0] + "\t" +
                pMap.get(fields[1]) + "\t" + //将PID替换
                fields[2]);

        context.write(k, NullWritable.get());

    }
}
