package spark.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@RestController
@Slf4j
public class SparkController {

    @Autowired
    @Qualifier("sparkSession")
    private SparkSession sparkSession;

    @Autowired
    @Qualifier("sparkContext")
    private JavaSparkContext sparkContext;

    @Autowired
    @Qualifier("clusterSparkContext")
    private JavaSparkContext clusteredSparkContext;

    @GetMapping("/parallelize")
    public boolean parallelize() {
        JavaRDD<Integer> result = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        log.info("Numbers count: {} ", result.count());
        log.info("Number of partitions: {} ", result.getNumPartitions());
        log.info("Max number : {} ", result.max(Comparator.naturalOrder()));
        log.info("Min number : {} ", result.min(Comparator.naturalOrder()));
        return true;
    }

    @GetMapping("/dataFrame")
    public boolean dataFrame() {
        StructType structType = new StructType().add("Character", DataTypes.StringType, false).add("Position", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<>();
        nums.add(RowFactory.create("A", "1"));
        nums.add(RowFactory.create("B", "2"));
        nums.add(RowFactory.create("C", "3"));

        Dataset<Row> df = sparkSession.createDataFrame(nums, structType);
        df.createOrReplaceTempView("Alphabets");

        Dataset<Row> temp = sparkSession.sql("SELECT * FROM Alphabets WHERE Position > 2");
        temp.show();
        return true;
    }


    @GetMapping("/dataFrameViaFile") // sparksql
    public boolean dataFrameViaFile() {
        String path = getClass().getResource("/fake_data.txt").getPath();
        Dataset<Row> df = sparkSession.read()
                .option("header", "true")        // Use the first row as header
                .option("inferSchema", "true")  // Automatically infer column data types
                .csv(path);

        df.createOrReplaceTempView("fake_data");
        Dataset<Row> temp = sparkSession.sql("select country, count(*) as Youngsters_Count from fake_data where age between 25 and 50 group by country"); // Max youngsters country vise
        temp.show(); // top 20 records

        Dataset<Row> youngstersAcrossCountriesDf =  df.filter(new Column("age").between(25, 50));
        log.info("Number of people between age 20 and 50 across the country {}",  youngstersAcrossCountriesDf.count()); // filter data by age
        youngstersAcrossCountriesDf.groupBy("country").agg(functions.count("*").alias("Youngsters_Count")).show(); // Max youngsters country vise

        return true;
    }

    @GetMapping("/dataFrameRDD") // spark-core
    public boolean dataFrameRDD() {
        String path = getClass().getResource("/fake_data.txt").getPath();
        JavaRDD<String> rdd = sparkContext.textFile(path);
        processRDD(rdd);
        return true;
    }

    @GetMapping("/externalFileSystemAsSource")
    public boolean externalFileSystemAsSource() {
        // s3a: - amazon s3 bucket, where files are fetched from s3 bucket
        try {
            clusteredSparkContext.hadoopConfiguration().set("fs.s3a.access.key", null);
            clusteredSparkContext.hadoopConfiguration().set("fs.s3a.secret.key", null);
            clusteredSparkContext.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");
            // Read a single text file
            JavaRDD<String> stringJavaRDD = clusteredSparkContext.textFile("s3a://mybucket/fake_data.txt");
            stringJavaRDD.count();
            processRDD(stringJavaRDD);
        } catch(Exception e) {
           log.error("Error occurred while fetching from external system {}", e.getMessage());
        }
        return true;
    }

    private void processRDD(JavaRDD<String> rdd) {
        String header = rdd.first();
        JavaPairRDD<String, Integer> countryCounts = rdd
                .filter(line -> !line.equals(header))// Skipping Header
                .map(line -> line.split(",")) // Split each line by comma
                .filter(fields -> {
                    int age = Integer.parseInt(fields[2]); // Assuming age is the 3rd field (index 2)
                    return age >= 25 && age <= 50;
                })
                .mapToPair(fields -> new Tuple2<>(fields[3], 1)) // Assuming country is the 4th field (index 3)
                .reduceByKey(Integer::sum); // Count occurrences per country

        // Step 5: Collect and print the results
        countryCounts.collect().forEach(result -> {
            log.info("Country: " + result._1 + ", Youngsters_Count: " + result._2);
        });
    }

}
