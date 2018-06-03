package com.jeffrey.learn.hadoop.hdfs;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        try {
            System.setProperty("hadoop.home.dir", "D:\\hadoop-3.1.0");
            HDFSApp hdfsApp = new HDFSApp();
            hdfsApp.delete();
            hdfsApp.tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
