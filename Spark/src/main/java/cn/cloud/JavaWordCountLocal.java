package cn.cloud;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Description:
 *
 * @author: Cloud
 * Date: 2019-08-01
 * Time: 10:18
 */
public class JavaWordCountLocal {
    public static void main(String[] args) {
        //设置本地执行,需要配置hadoop本地环境
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定从哪读取数据
        JavaRDD<String> lines = jsc.textFile("F:\\bigdata\\input\\info-log.0.log");
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //讲单词和1组合在一起
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((v1, v2) -> v1 + v2);
        //调整顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());

        //将结果保存到hdfs
        result.saveAsTextFile("F:\\bigdata\\output");
        //释放资源
        jsc.stop();
    }
}
