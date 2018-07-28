package com.flipkart.planning;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MRConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BootStrapDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "pds");
        conf.set("io.compression.codecs","org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapred.output.compression.codec","snappy" );

        String inputPath = args[0];
        String outputPath = args[1];


//        String inputPath = "/projects/planning/sd/pricing";
//        String outputPath = "/projects/planning/sd/output";

        FileSystem fs = FileSystem.get(conf);;

        System.out.println("Input Path = " + inputPath);
        System.out.println("Output Path = " + outputPath);

        if (fs.exists(new Path(outputPath))) {
            System.out.println("Output Path already exists. Deleting it");
            fs.delete(new Path(outputPath), true);
        }

        Job job = Job.getInstance(conf, "bootstrap_pricing");

        job.setJarByClass(BootStrapDriver.class);
        job.setMapperClass(BootStrapMapper.class);
        job.setReducerClass(BootStrapReducer.class);
        job.setNumReduceTasks(0);

        try {
            FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(inputPath));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
}
