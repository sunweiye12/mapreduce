package com.atguigu.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 将数据按照包不包含atguigu，分别输出到两个文件
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    FSDataOutputStream atguigu = null;
    FSDataOutputStream other = null;

    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration configuration = job.getConfiguration();

        String outDir = configuration.get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(configuration);


        atguigu = fileSystem.create(new Path(outDir + "/atguigu.log"));
        other = fileSystem.create(new Path(outDir + "/other.log"));

    }

    /**
     * 接收key value对，并按照值的不同写出到不通文件
     * @param key 读取的一行的偏移量
     * @param value 这一行内容
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        //获取一行内容
        String line = value.toString() + "\n";
        //判断包不包含atguigu
        if (line.contains("atguigu")) {
            //往atguigu文件写数据
            atguigu.write(line.getBytes());
        } else {
            //往other文件写数据
            other.write(line.getBytes());
        }

    }

    /**
     * 关闭资源
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
