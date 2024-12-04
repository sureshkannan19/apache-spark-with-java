package war;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PostConstruct;

@SpringBootApplication
@ComponentScan("spark.common")
@Slf4j
public class SparkWebApplication {

    @Value("${hadoop.home.dir}")
    private String hadoopHomeDir;


    @PostConstruct
    public void setUp() {
        System.setProperty("hadoop.home.dir", hadoopHomeDir);
    }

    public static void main(String[] args) {
        SpringApplication.run(SparkWebApplication.class, args);
    }
}
