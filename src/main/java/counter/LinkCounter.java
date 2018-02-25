package counter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LinkCounter {

    public static void main(String[] args) throws IOException {
        String url = "https://en.wikipedia.org/wiki/Europe";

        FileUtils.writeStringToFile(new File("test.txt"), readURLToString(url));

        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.setProperty("hadoop.home.dir", "C:\\hadoop-3.0.0");
        try {
            JavaRDD<String> input = sc.textFile("test.txt");
            JavaRDD<String> words = input.flatMap(
                    new FlatMapFunction<String, String>() {
                        public Iterable<String> call(String x) {
                            return Arrays.asList(x.split(" "));
                        }
                    });

            JavaPairRDD<String, Integer> counts = words.mapToPair(
                    new PairFunction<String, String, Integer>() {
                        public Tuple2<String, Integer> call(String x) {
                            return new Tuple2(x, 1);
                        }
                    }).filter(s -> s.toString().contains("href")).reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer x, Integer y) {
                    return x + y;
                }
            });
            counts.saveAsTextFile("outputFile");
        }  finally {
            sc.close();
        }
    }

    public static String readURLToString(String url) throws IOException {
        try (InputStream inputStream = new URL(url).openStream()) {
            return IOUtils.toString(inputStream, String.valueOf(StandardCharsets.UTF_8));
        }
    }
}

