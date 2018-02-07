import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Collection;
import java.util.Scanner;
import java.io.File;

public final class Ranking {
	private static String filePath = "AssignmentData/datafiles";
	private static String stopWordFilePath = "AssignmentData/stopwords.txt";
	private static Character[] specialCharacters = {',', '.', '!', '[', ']'};
	private static double numberOfDoc = 10;

	public static void main(String[] args) throws Exception {
		Ranking ranking = new Ranking();

		Set<String> stopwords = ranking.stopwordsAtFilePath(stopWordFilePath);

		//create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ranking")); 

        JavaPairRDD<String, String> files = sc.wholeTextFiles(filePath);

        // Step 1: Count frequency of each word.
        JavaPairRDD<String, Integer> countsOfWords = files
        .flatMap(filenameAndcontent -> {
        	String filename = filenameAndcontent._1();
        	String content = filenameAndcontent._2();

        	String[] words = content.split("\\W+");
        	List<String> newWords = new ArrayList<String>();

        	for (String word : words) {
        		if (word.length() == 0) {
        			continue;
        		}

        		String newWord = word.toLowerCase();
        		newWords.add(filename+"@"+newWord);
        	}
        	return newWords.iterator();
        })
        .filter(word -> stopwords.contains(word.split("@")[1]))
        .mapToPair(word -> new Tuple2<>(word, 1))
    	.reduceByKey((a, b) -> a + b);

    	// Step 2: Calculate the TF-IDF of the words.
    	Map<String, Long>tfOfWords = countsOfWords
    	.mapToPair(keyAndCount -> new Tuple2<>(keyAndCount._1().split("@")[1], 1))
    	.countByKey();

    	JavaPairRDD<String, Double> tfIdfOfWords = countsOfWords
    	.mapToPair(keyAndCount -> {
    		String key = keyAndCount._1();
    		String word = key.split("@")[1];
    		Integer count = keyAndCount._2();

    		Double tfidf = (1 + Math.log(count)) * Math.log(numberOfDoc / tfOfWords.get(word));
    		return new Tuple2<>(key, tfidf);
    	});

    	JavaPairRDD<String, Double> sumOfTfIdfO = tfIdfOfWords
    	.map(keyAndCount -> {
    		String key = keyAndCount._1();
    		String doc = key.split("@")[0];
    		Double tfidf = keyAndCount._2();

    		return new Tuple2<>(doc, tfidf * tfidf);
    	})
    	.reduceByKey((a, b) -> a+b);

        //set the output folder
        sumOfTfIdfO.saveAsTextFile("outfile");
        //stop spark
	}

/*
	private String removeSpecialCharacters(String word, Character[] specialCharacters) {
		while(specialCharacters.contains(word.charAt(word.length()) -1)) {
			word = word.substring(0, word.length()-1);
		}

		return word;
	}
*/
	private Set<String> stopwordsAtFilePath(String filePath) throws Exception{
		Set<String>stopwords = new HashSet<String>();
		Scanner scanner = new Scanner(new File(filePath));
		while (scanner.hasNextLine()) {
			stopwords.add(scanner.nextLine());
		}
		scanner.close();
		return stopwords;

	}
}