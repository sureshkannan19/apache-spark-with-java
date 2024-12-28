package jar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


public class SparkJarEntryApplication {


    public static void main(String[] args) {
        try {

            SparkSession ss = SparkSession.builder().appName("SparkBasics")
                    .config(new SparkConf()
                            .setJars(new String[] {"apache-spark-with-java8-1.0-SNAPSHOT.jar"}))
                    .getOrCreate();
            JavaSparkContext clusteredSparkContext = new JavaSparkContext(ss.sparkContext());
            clusteredSparkContext.hadoopConfiguration().set("fs.s3a.access.key", null);
            clusteredSparkContext.hadoopConfiguration().set("fs.s3a.secret.key", null);
            clusteredSparkContext.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");
            // Read a single text file
            JavaRDD<String> rdd = clusteredSparkContext.textFile("s3a://mybucket//fake_data.txt");
            System.out.println("Overall Count from file stored in S3 Bucket: "+ rdd.count());
            String header = rdd.first();
            JavaPairRDD<String, Integer> countryCounts = rdd.filter(line -> !line.equals(header))// Skipping Header
                    .map(line -> line.split(",")) // Split each line by comma
                    .filter(fields -> {
                        int age = Integer.parseInt(fields[2]); // Assuming age is the 3rd field (index 2)
                        return age >= 25 && age <= 50;
                    }).mapToPair(fields -> new Tuple2<>(fields[3], 1)) // Assuming country is the 4th field (index 3)
                    .reduceByKey(Integer::sum); // Count occurrences per country
            // Step 5: Collect and print the results
            countryCounts.collect().forEach(result -> {
                System.out.println("Country: " + result._1 + ", Youngsters_Count: "+ result._2);
            });
        } catch (Exception e) {
            System.err.println("Error occurred while fetching from external system " + e.getMessage());
        }
        System.exit(0);
    }
}
