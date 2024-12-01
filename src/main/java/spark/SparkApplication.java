package spark;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
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

            StructType structType = new StructType();
            structType = structType.add("Alphabet_Char", DataTypes.StringType, false);
            structType = structType.add("Alphabet_Pos", DataTypes.StringType, false);

            List<Row> nums = new ArrayList<>();
            nums.add(RowFactory.create("A", "1"));
            nums.add(RowFactory.create("B", "2"));
            nums.add(RowFactory.create("C", "3"));


            Dataset<Row> df = ss.createDataFrame(nums, structType);
            df.createOrReplaceTempView("Alphabets");

            Dataset<Row> temp = ss.sql("SELECT * FROM Alphabets WHERE Alphabet_Pos > 2");
            temp.show();
            try (var scanner = new Scanner(System.in)) {
                scanner.nextLine();
            }
        }


    }
}
