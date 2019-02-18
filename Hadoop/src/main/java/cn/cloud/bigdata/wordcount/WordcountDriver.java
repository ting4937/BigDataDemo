package cn.cloud.bigdata.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Description:
 *
 * @author: Cloud
 * Date: 2018-05-02
 * Time: 10:59
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 */
public class WordcountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //跑在hadoop集群机器上可以不用设置
        /*conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "test1");*/
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(WordcountDriver.class);
        //指定本业务job使用的mapper业务类
        job.setMapperClass(WordcountMapper.class);
        //指定本业务job使用的reduce业务类
        job.setReducerClass(WordcountReducer.class);
        //指定mapper输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定输出结果的输入输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job输入文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数以及jar包提交给yarn去运行
        //job.submit();
        //等待job执行结束
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
