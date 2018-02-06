import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

public final class Ranking {
	private static String filePath = "AssignmentData/datafiles";
	private static String stopWordFilePath = "AssignmentData/stopwords.txt";
	private static Character[] specialCharacters = {',', '.', '!', '[', ']'};

	public static void main(String[] args) throws Exception {
		List<String> stopwords = stopwordsAtFilePath(stopWordFilePath);

		//create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("lab1")); 

        JavaRDD<String> files = sc.wholeTextFiles(filePath);

        // Step 1: Count frequency of each word.
        JavaPairRDD<String, Integer> counts = files
        .flatMap((filename, content) -> {
        	String[] words = content.split(" ");
        	List<String> words = new ArrayList<String>();

        	for (String word : words) {
        		newWord = removeSpecialCharacters(word, specialCharacters);
        		words.add(filename+"@"+newWord.toLowercase());
        	}
        	return words.iterator();
        })
        .filter(word -> stopwords.contains(word.split("@")[1]))
        .mapToPair(word -> new Tuple2<>(word, 1))
    	.reduceByKey((a, b) -> a + b);

        //set the output folder
        counts.saveAsTextFile("outfile");
        //stop spark
	}

	private String removeSpecialCharacters(String word, Char[] specialCharacters) {
		while(specialCharacters.contains(word.charAt(word.length()) -1)) {
			word = word.substring(0, word.length()-1);
		}

		return word;
	}

	private List<String> stopwordsAtFilePath(String filePath) {
		ArrayList<String>stopwords = new ArrayList<String>();
		Scanner scanner = new Scanner(new File(filePath));
		while (scanner.hasNextLine()) {
			stopwords.add(scanner.nextLine());
		}
		scanner.close();
		return stopwords;

	}
}