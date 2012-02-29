package data.processing;


import java.sql.SQLException;

public class PearsonCorrelation {

	
	private static String TRAININGSET = "testset";
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		String[] pramas1 = { "C:/temp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", TRAININGSET };
		DataAccess trainingSet = new DataAccess(pramas1);
		System.out.println(trainingSet.countDistinctRecord());
		System.out.println();
		
	}

}
