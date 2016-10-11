/** 
With movielense dataset of 100k from http://grouplens.org/datasets/movielens/ 

The u.data has the following data 
user id | item id | rating | timestamp. 
 
Write a map-reduce code to get the minimum time between two ratings of a movie by a user, maximum time between two rating of movies. 
 
A user can rate a moving only once and there are many users giving rating to a movie at different point in time. 
We interested in finding:

1) At user level: For each user, what is minimum & maximum time gap between the ratings given for the movies
i.e.: (U1, min(t2-t1, t3-t2, t4-t3,t5-t4), max(t2-t1, t3-t2, t4-t3,t5-t4))
 
2) At movie level: For each movie, what is minimum & maximum time gap between the ratings given by the users
i.e.: (M1, min(t2-t1, t3-t2, t4-t3,t5-t4), max(t2-t1, t3-t2, t4-t3,t5-t4)) 

**/


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

 
public class MovieRating {
  public static void main(String[] args) {
	JavaSparkContext sc = new JavaSparkContext(
		      "local", "ratingexample", System.getenv("SPARK_HOME"), System.getenv("JARS"));  
  
	if (args[0] == null || args[1] == null || args[2] == null){
		System.out.println("Usage MovieRating <inputFile> " +
				"<outputDir> <integer choice 1: User Rating 2: Movie Rating>");
	}
    String inputFile = args[0];
    String outputDir = args[1];
    final int choice = Integer.parseInt(args[2]); // 1 is User Rating 2 is Movie Rating
    // Read file
    JavaRDD lines = sc.textFile(inputFile);
    
    // Create Pair RDD of the form <<userid> <timestamp>> OR <<itemid> <timestamp>> depending on user choice
    PairFunction<String, String, String> pairDataFunc =
    	  new PairFunction<String, String, String>() {
    	  public Tuple2<String, String> call(String x) {
    		String[] tokens = x.split("\\s+");
    		String item = tokens[0] ;
    		if (choice == 2){
    			item = tokens[1];
    		}
    	    return new Tuple2(item,tokens[3]);
    	  }
    };
    
    JavaPairRDD<String, String> pairData = lines.mapToPair(pairDataFunc);
    
    // Reduce the pairRDD by key so that we get <userid>,<rating1,rating2,...>
    Function2<String, String,String> reduceByKeyFunc =
  	  new Function2<String,String,String>() {
  	  public String call(String x,String y) {  		
  	    return x + "," + y;
  	  }
    };
    
    JavaPairRDD<String, String> pairDataByKey =  pairData.reduceByKey(reduceByKeyFunc);
    
    pairDataByKey.saveAsTextFile(outputDir + "/tmp");
    
    // Save to file
    JavaRDD newPairDataByKey = sc.textFile(outputDir + "/tmp");
    
    // Read from file and map through the lines
    Function<String,String> mapFunction = new Function<String,String>(){

		@Override
		public String call(String line) throws Exception {
			line = line.substring(1,line.length()-1);
			String [] tokens = line.split(",");
			int MIN = Integer.MAX_VALUE;
			int MAX = Integer.MIN_VALUE;
	    	if (tokens.length == 2) MIN = Integer.parseInt(tokens[1]);
	    	for (int i=2;i<tokens.length;i++){
	    		int f = Integer.parseInt(tokens[i]);
	    		int s = Integer.parseInt(tokens[i-1]);
	    		int diff = Math.abs(f-s);
	    		if (diff < MIN) MIN = diff;
	    		if (diff > MAX) MAX = diff;
	    	}
	    	return tokens[0] + ":" + MIN + ":" + MAX;
		}
    	
    };
    
    JavaRDD<String> result = newPairDataByKey.map(mapFunction);
    
    // Store Results
    result.saveAsTextFile(outputDir + "/results");
    
   
  }
}