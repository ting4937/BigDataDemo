package cn.cloud.bigdata.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Description:
 *
 * @author: Cloud
 * Date: 2018-06-22
 * Time: 9:35
 */
public class FlowCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //跑在hadoop集群机器上可以不用设置
        /*conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "test1");*/
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCount.class);
        //指定本业务job使用的mapper业务类
        job.setMapperClass(FlowCountMapper.class);
        //指定本业务job使用的reduce业务类
        job.setReducerClass(FlowCountReduce.class);
        //指定mapper输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //指定输出结果的输入输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job输入文件
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
        //使用本地文件
        FileInputFormat.setInputPaths(job, new Path("F:\\flow.log"));
        //如果outpath存在则删除
        Path outPath = new Path("F:\\out.txt");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        //指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job, outPath);

        //将job中配置的相关参数以及jar包提交给yarn去运行
        //job.submit();
        //等待job执行结束
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取一行数据
            String line = value.toString();
            String[] fields = line.split("\t");
            if(fields.length < 4) {
                return;
            }
            String phone = fields[1];
            long upflow = Long.parseLong(fields[fields.length - 3]);
            long downflow = Long.parseLong(fields[fields.length - 2]);
            context.write(new Text(phone), new FlowBean(upflow, downflow));
        }
    }

    static class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long totalUpflow = 0;
            long totalDownflow = 0;

            for (FlowBean bean : values) {
                totalUpflow += bean.getUpflow();
                totalDownflow += bean.getDownflow();
            }

            FlowBean resultBean = new FlowBean(totalUpflow, totalDownflow);
            context.write(key, resultBean);
        }
    }
}
