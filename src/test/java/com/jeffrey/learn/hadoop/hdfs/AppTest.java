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
            HDFSApp hdfsApp = new HDFSApp();
            hdfsApp.cat();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
