package cn.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * Unit test for simple App.
 */
public class AppTest {
    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://test1:9000");
        fs = FileSystem.get(new URI("hdfs://cloudserver:9000"), conf, "root");
    }

    @After
    public void destroy() throws Exception {
        fs.close();
    }

    @Test
    public void testUpload() throws Exception {
        fs.copyFromLocalFile(new Path("f:/wordcount.txt"), new Path("/wordcount.txt"));
    }

    @Test
    public void testDownload() throws Exception {
        fs.copyToLocalFile(new Path("/wordcount.txt"), new Path("f:/wordcount1.txt"));
    }
}
