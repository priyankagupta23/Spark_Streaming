package spark;

import java.util.Arrays;
import java.util.Collections;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class Rough {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException {

		System.setProperty("HADOOP_HOME", System.getenv("HADOOP_HOME"));
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("SparkApplication").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(60));
		
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		
		JavaPairDStream<String, Long> countByValue = words.countByValue();
		
		JavaPairDStream<Long, String> swap = countByValue.mapToPair(x -> x.swap());
		
		JavaPairDStream<Long, String> sort = swap.transformToPair(x -> x.sortByKey(false));
		
		JavaPairDStream<String, Long> swap_again = sort.mapToPair(x -> x.swap());
		
		
		//JavaDStream<String> filter = words.filter(x -> !x.equals("hello"));
		
		//JavaPairDStream<String, Integer> pairs = filter.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		
		//JavaPairDStream<String, Integer> sum = pairs.reduceByKey((a,b) -> a + b);
		
		/*
		
		JavaDStream<String> reduce = words.reduce((a,b) -> {
			
			String max_length_word; 
			
			if(a.length() >= b.length()) {
				max_length_word = a;
			}else {
				max_length_word = b;
			}
			
			return max_length_word;
			
		});
		
		*/
		
		swap_again.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		
		
	}

}
