package spark.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
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

    @GetMapping("/parallelize")
    public boolean parallelize() {
        JavaRDD<Integer> result = sparkContext.parallelize(List.of(1, 2, 3, 4, 5));
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

    @GetMapping("/dataFrameViaFile")
    public boolean dataFrameViaFile() {
        StructType structType = new StructType().add("Character", DataTypes.StringType, false).add("Position", DataTypes.StringType, false);

//        JavaRDD<String> fakeData = sparkContext.textFile("fake_data.csv");
        String path = getClass().getResource("/fake_data.csv").getPath();
        Dataset<Row> df = sparkSession.read()
                .option("header", "true")        // Use the first row as header
                .option("inferSchema", "true")  // Automatically infer column data types
                .csv(path);

        df.createOrReplaceTempView("fake_data");
        Dataset<Row> temp = sparkSession.sql("select country, count(*) as Youngsters_Count from fake_data where age between 20 and 50 group by country"); // Max youngsters country vise
        temp.show(); // top 20 records

        Dataset<Row> youngstersAcrossCountriesDf =  df.filter(new Column("age").between(20, 50));
        log.info("Number of people between age 20 and 50 across the country {}",  youngstersAcrossCountriesDf.count()); // filter data by age
        youngstersAcrossCountriesDf.groupBy("country").agg(functions.count("*").alias("Youngsters_Count")).show(); // Max youngsters country vise

        return true;
    }
}
