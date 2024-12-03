package spark.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean("sparkSession")
    public SparkSession getSparkSession() {
        return SparkSession.builder().master("local[*]")
                .appName("SparkBasics")
                .getOrCreate();
    }

    @Bean("sparkContext")
    public JavaSparkContext getSparkContext(@Qualifier("sparkSession") SparkSession ss) {
        return new JavaSparkContext(ss.sparkContext());
    }

    @Bean("clusterSparkSession")
    public SparkSession getClusteredSparkSession() {
        return SparkSession.builder()
                .appName("SparkBasics")
                .getOrCreate();
    }

    @Bean("clusterSparkContext")
    public JavaSparkContext getClusteredSparkContext(@Qualifier("clusterSparkSession") SparkSession ss) {
        return new JavaSparkContext(ss.sparkContext());
    }
}
