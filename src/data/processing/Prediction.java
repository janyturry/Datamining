package data.processing;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Prediction {

	private static String TRAININGSET = "trainingset";

	private static String TESTSET = "testset";

	private static String CORRELATIONSET = "correlation";

//	private static String PREDICTION = "prediction";
	/**
	 * @param args
	 * @throws SQLException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		// TODO Auto-generated method stub
		String[] pramas1 = { "C:/temp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", TRAININGSET };
		String[] pramas2 = { "C:/testSetTemp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", TESTSET };

		String[] pramas3 = { "C:/testSetTemp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", CORRELATIONSET };
		
//		String[] pramas4 = { "C:/testSetTemp.txt",
//				"jdbc:postgresql://localhost:5432/testDB", "postgres",
//				"123456", PREDICTION };
		
		DataAccess trainingSet = new DataAccess(pramas1);
		DataAccess testSet = new DataAccess(pramas2);
		DataAccess correlationSet = new DataAccess(pramas3);
		//DataAccess predictionSet = new DataAccess(pramas4);
		
		
		File file=new File("Result0.1.txt");
		if(file.exists()){
			System.exit(0);
			
		}
		PrintWriter output=new PrintWriter(file);
		output.println("用户\t\t电影\t\t预测rating\t\t实际rating");

		double prediction = 0.0;
		double denominator = 0.0, numerator = 0.0;
		int a = 0, u = 0, movie = 0;
		double i = 0;

		ResultSet trainingUser;
		ResultSet testUser = testSet.findUsersANDMoviesANDRatings();
		double MAE = 0.0;
		double RMSE=0.0;
		double sum1=0.0;
		double sum2=0.0;
		int count = 0;
		double temp;
		
		while (testUser.next()) {
			//if(count==7) break;
			movie = testUser.getInt(2);
			trainingUser = trainingSet.findUsersByMovieid(movie);
			double rating = testUser.getDouble(3);
			a = testUser.getInt(1);
			while (trainingUser.next()) {
				u = trainingUser.getInt(1);
				if (a == u)
					continue;
				i = correlationSet.findCorrelation(a, u);
				if (Math.abs(i) > 0.1) {
					denominator += Math.abs(i);
					numerator += i
							* (trainingSet.findRatingByUserIdAndMovieId(u,
									movie) - trainingSet.getUserAvgRating(u));
				}
			}
			if (denominator != 0) {
				prediction = trainingSet.getUserAvgRating(a) + numerator
						/ denominator;
			} else {
				prediction = trainingSet.getUserAvgRating(a);
			}

			temp = prediction - Math.floor(prediction);

			if (temp < 0.25) {
				prediction = Math.floor(prediction);
			} else if (temp > 0.75) {
				prediction = Math.floor(prediction) + 1;
			} else {
				prediction = Math.floor(prediction) + 0.5;
			}

			if (prediction > 5) {
				prediction = 5.0;
			}
			if(prediction==0){
				prediction=2.5;
			}
			//predictionSet.storePredictionsIntoDB(a, movie, prediction,rating);
//			System.out.println(a + "对电影" + movie + "评分是:" + prediction
//					+ ": 真实评分是：" + rating);
			output.printf("%d\t\t%d\t\t%.1f\t\t%.1f ", a,movie,prediction,rating);
			output.println();
			count++;
			sum1 += Math.abs(prediction - rating);
			//System.out.println(sum1);
			sum2 +=  Math.pow(prediction - rating,2);
			denominator = numerator = 0.0;
		}
		MAE=sum1 / count;
		RMSE=Math.sqrt(sum2/count);
		output.println("Overall MAE="+MAE);
		output.println("Over RMSE="+RMSE);
		output.close();
		System.out.println(MAE);
		System.out.println(RMSE);
	}

}
