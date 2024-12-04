package jar;

import lombok.extern.slf4j.Slf4j;
import spark.controller.SparkController;

@Slf4j
public class SparkJarApplication {

    public static void main(String[] args) {
        SparkController sparkController = new SparkController();
        sparkController.externalFileSystemAsSource();
    }
}
