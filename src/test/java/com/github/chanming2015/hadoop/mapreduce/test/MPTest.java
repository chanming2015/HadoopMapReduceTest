package com.github.chanming2015.hadoop.mapreduce.test;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Description:
 * Create Date:2017年10月23日
 * @author XuMaoSen
 * Version:1.0.0
 */
public class MPTest
{

    @Test
    public void test() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);
        Path input = new Path("input");
        Path output = new Path("output");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);
        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exitCode = driver.run(new String[] {input.toString(), output.toString()});
        assertEquals(exitCode, 0);
    }

}
