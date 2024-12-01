package spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class SparkConfig {

    public void getSparkConfig() {
        SparkConf sc = new SparkConf().setAppName("SparkBasics").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        List<Integer> input = new ArrayList<>();
        JavaRDD<Integer> res = jsc.parallelize(input);
        jsc.close();
    }
}
