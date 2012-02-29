package data.processing;

import java.sql.ResultSet;
import java.sql.SQLException;


public class Preprocessing {
	
	private static String TRAININGSET = "trainingset";

	private static String TESTSET = "testset";
	
	private static String userbaseSet = "correlation";
	
	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		String[] pramas1 = { "C:/temp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", TRAININGSET };
		
		String[] pramas2 = { "C:/testSetTemp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", TESTSET };
		
		String[] pramas3 = { "C:/testSetTemp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", userbaseSet };
		
		DataAccess trainingSet = new DataAccess(pramas1);
		DataAccess testSet = new DataAccess(pramas2);
		DataAccess userbaseSet = new DataAccess(pramas3);
		int a = 0, u = 0,movie=0;
		double i = 0;
		
		ResultSet trainingUser;
		ResultSet testUser = testSet.findUsersANDMoviesANDRatings();
		while (testUser.next()) {
			movie=testUser.getInt(2);
			trainingUser = trainingSet.findUsersByMovieid(movie);
			a = testUser.getInt(1);
			while (trainingUser.next()) {
				u = trainingUser.getInt(1);
				if(userbaseSet.checkDuplication(a, u)){
					continue;
				}
				i = trainingSet.getPearsonCorrelationbetweenUsers(a, u);
				if (i != 0){
					userbaseSet.storeUserBaseCorrelationIntoDB(a, u, i);
				}				
			}
		}

	    testSet.destroy();
		trainingSet.destroy();
		
	}

}
