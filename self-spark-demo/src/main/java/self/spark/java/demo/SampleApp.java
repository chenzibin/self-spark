package self.spark.java.demo;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * SampleApp
 *
 * @author chenzb
 * @date 2020/8/11
 */
public class SampleApp {

	public static void main(String[] args) {
//		String logFile = "/opt/uyun/databank/spark/spark/README.md"; // Should be some file on your system
		String logFile = "C:\\Users\\chenzb\\Desktop\\文档\\维度管理\\流程.txt"; // Should be some file on your system
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		Dataset<String> logData = spark.read().textFile(logFile).cache();

		long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
		long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		spark.stop();
	}
}
