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
 * 这是本地运行案例
 */
public class WordcountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
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

        //指定job输入目录
        //运行在windows环境可以写windows路径
        //运行在hadoop集群写hdfs路径
        FileInputFormat.setInputPaths(job, new Path("F:\\01_data\\hadoop\\input"));
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("F:\\01_data\\hadoop\\output"));

        //将job中配置的相关参数以及jar包提交给yarn去运行
        //job.submit();
        //等待job执行结束
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
