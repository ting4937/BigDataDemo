package cn.cloud.bigdata.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Description:
 *
 * @author: Cloud
 * Date: 2018-05-02
 * Time: 10:59
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 * 这是远程提交到yarn集群案例
 */
public class WordcountRemoteSubmit {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //跑在hadoop集群机器上可以不用设置
        // 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
        System.setProperty("HADOOP_USER_NAME", "root");
        // 1、设置job运行时要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://cloudserver:9000");
        // 2、设置job提交到哪去运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "cloudserver");
        // 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        //job.setJarByClass(WordcountRemoteSubmit.class);
        //本地调试需指定jar包
        job.setJar("E:\\03bigdata_WorkSpace\\BigDataDemo\\Hadoop\\target\\Hadoop-1.0-SNAPSHOT.jar");
        //指定本业务job使用的mapper业务类
        job.setMapperClass(WordcountMapper.class);
        //指定本业务job使用的reduce业务类
        job.setReducerClass(WordcountReducer.class);
        //指定mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定输出结果的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job输入文件或目录
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileSystem fs = FileSystem.get(conf);
        Path output = new Path("/wordcount/output");
        if(fs.exists(output)) {
            fs.delete(output, true);
        }
        fs.close();
        //指定job输出结果所在目录,输出路径必须不存在
        FileOutputFormat.setOutputPath(job, output);

        //将job中配置的相关参数以及jar包提交给yarn去运行
        //job.submit();
        //等待job执行结束
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
