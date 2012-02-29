package rubbish;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import data.processing.DataAccess;

public class ItemBase {

	private static String TRAININGSET = "trainingset";

	private static String ITEMBASE = "itembase";

	private static String PREDICTION = "prediction";
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
				"123456", PREDICTION };
		
		String[] pramas3 = { "C:/testSetTemp.txt",
				"jdbc:postgresql://localhost:5432/testDB", "postgres",
				"123456", ITEMBASE };
		
		DataAccess trainingSet = new DataAccess(pramas1);
		DataAccess correlationSet = new DataAccess(pramas2);
		DataAccess itembaseSet=new DataAccess(pramas3);
		
		
		ResultSet movieSet=correlationSet.findMoviesWithUserInTrainingset();
		ResultSet userSet=null;
		ResultSet compareMovieset=null;

		HashSet<Integer> checkDuplicated=new HashSet<Integer>();
		double denominator = 0.0, numerator = 0.0;
		double denominator_1 = 0.0, denominator_2 = 0.0;
		double correlation=0.0;
		while(movieSet.next()){
			int i_movie=movieSet.getInt(1);
			//System.out.println(i_movie);
			userSet=trainingSet.findUsersByMovieid(i_movie);//找到了同样看过该部电影的所有的users
			while(userSet.next()){
				int u=userSet.getInt(1);//遍历每一个user
				//System.out.println("用户："+u);
				double i_rating=trainingSet.findRatingByUserIdAndMovieId(u, i_movie); // 找到user对这部电影的评分
				compareMovieset=trainingSet.findMoviesANDRatingsByUserId(u);
				while(compareMovieset.next()){
					int j_movie=compareMovieset.getInt(1);
					if(j_movie==i_movie||checkDuplicated.contains(j_movie)){
						continue;
					}
					checkDuplicated.add(j_movie);
					double j_rating=compareMovieset.getDouble(2);
					double u_avg=trainingSet.getUserAvgRating(u);
					numerator+=(i_rating-u_avg)*(j_rating-u_avg);
					
					denominator_1 += Math.pow((i_rating-u_avg), 2);
					denominator_2 += Math.pow((j_rating-u_avg), 2);
					//到这里只找到第一个用户i和j的打分的对比
					
					ResultSet otherUserSet=trainingSet.findUsersByMovieid(j_movie);
					while(otherUserSet.next()){
						int _u=otherUserSet.getInt(1);
						
						if(_u==u)
							continue;
						i_rating=trainingSet.findRatingByUserIdAndMovieId(_u, i_movie);
						if(i_rating==0.0){
							continue;
						}else{
							j_rating=trainingSet.findRatingByUserIdAndMovieId(_u, j_movie);
						}
						//System.out.println(_u);
						u_avg=trainingSet.getUserAvgRating(_u);
						//System.out.println(_u+":"+j_rating);
						numerator+=(i_rating-u_avg)*(j_rating-u_avg);
						
						denominator_1 += Math.pow((i_rating-u_avg), 2);
						denominator_2 += Math.pow((j_rating-u_avg), 2);
					}
					//要添加是否重复计算的操作
					denominator = Math.sqrt(denominator_2 * denominator_1);
					if (denominator == 0)
						correlation=0.0;
					else
						correlation=numerator / denominator;
					System.out.println("电影"+i_movie+"电影"+j_movie+"的相似度是"+correlation);
					itembaseSet.storeItemBaseCorrelationIntoDB(i_movie, j_movie, correlation);
					denominator = 0.0; numerator = 0.0;
					denominator_1 = 0.0; denominator_2 = 0.0;
				}
				
				
			}
			checkDuplicated=new HashSet<Integer>();
			System.out.println("---------------------华丽的分割线-----------------------------");
			
		}
		
	}

}
