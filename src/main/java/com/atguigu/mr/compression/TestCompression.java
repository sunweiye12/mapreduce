package com.atguigu.mr.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class TestCompression {
    public static void main(String[] args) throws IOException {
//        compress("d:/word.txt", BZip2Codec.class);
        decompress("d:/word.txt.bz2");
    }

    private static void decompress(String file) throws IOException {
        Configuration configuration = new Configuration();
        //生成压缩格式工厂对象
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);

        //根据压缩格式工厂获取压缩对象
        CompressionCodec codec = codecFactory.getCodec(new Path(file));

        //输入流
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(file));
        CompressionInputStream cis = codec.createInputStream(fsDataInputStream);

        //输出流
        String outputFile = file.substring(
                0,
                file.length() - codec.getDefaultExtension().length()
        );

        FSDataOutputStream fos = fileSystem.create(new Path(outputFile));

        IOUtils.copyBytes(cis, fos, 1024);

        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }

    //压缩
    private static void compress(String file, Class<? extends CompressionCodec> codecClass) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        //生成压缩格式对象
        CompressionCodec codec = ReflectionUtils.newInstance(codecClass, configuration);

        //开输入流
        FSDataInputStream fis = fileSystem.open(new Path(file));

        //输出流
        FSDataOutputStream fos = fileSystem.create(new Path(file + codec.getDefaultExtension()));

        //用压缩格式包装输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }

    //解压
}
