package spark;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
    }
}
