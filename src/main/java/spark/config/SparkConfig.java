package spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    public void getSparkConfig() {
        SparkConf sc = new SparkConf().setAppName("SparkBasics").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sc);
    }
}
