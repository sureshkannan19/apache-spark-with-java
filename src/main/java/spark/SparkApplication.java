package spark;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.util.List;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@Slf4j
public class SparkApplication {

    @Value("${hadoop.home.dir}")
    private String hadoopHomeDir;

    @PostConstruct
    public void setUp() {
        System.setProperty("hadoop.home.dir", hadoopHomeDir);
    }

    public static void main(String[] args) {
        SpringApplication.run(SparkApplication.class, args);

//        SparkConf sc = new SparkConf().setAppName("SparkBasics").setMaster("local[*]");
//        JavaSparkContext jsc = new JavaSparkContext(sc);
//        List<Integer> input = new ArrayList<>();
//        JavaRDD<Integer> res = jsc.parallelize(input);
//        jsc.close();

        try (SparkSession ss = SparkSession.builder().master("local[*]")
                .appName("SparkBasics")
                .getOrCreate();
             JavaSparkContext jss = new JavaSparkContext(ss.sparkContext())) {
            JavaRDD<Integer> result = jss.parallelize(List.of(1, 2, 3, 4, 5));
            log.info("Numbers count: {} ", result.count());
            log.info("Number of partitions: {} ", result.getNumPartitions());
        }


    }
}
