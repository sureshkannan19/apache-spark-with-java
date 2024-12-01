package spark;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Scanner;

@SpringBootApplication
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
        try (SparkSession ss = SparkSession.builder().master("local[*]")
                .appName("SparkBasics")
                .getOrCreate();
             JavaSparkContext jss = new JavaSparkContext(ss.sparkContext())) {
            JavaRDD<Integer> result = jss.parallelize(List.of(1, 2, 3, 4, 5));
            log.info("Numbers count: {} ", result.count());
            log.info("Number of partitions: {} ", result.getNumPartitions());

//            Dataset<Row> result = ss.sql("SELECT * FROM people WHERE age > 30");
            try (var scanner = new Scanner(System.in)) {
                scanner.nextLine();
            }
        }


    }
}
