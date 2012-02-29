package data.processing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 这个类用来处理文本的数据 将文本数据存放到数据库 计算user的数目 计算movie的数目 训练集 total user:1870 total
 * movie:4797 测试集 total user:1043 total movie:2955 读取数据库的数据构造user-item矩阵
 */
public class DataAccess {

	private String fileName;

	private String url;

	private String passWord;

	private String table;

	private String userName;

	private Connection con;

	private PreparedStatement prestatement;

	/**
	 * @param pramas
	 *            pramas[0]:fileName 文本文件名 pramas[1]:url 数据库url地址
	 *            pramas[2]:userName 数据库用户名 pramas[3]：password 数据库密码
	 */
	public DataAccess(String[] pramas) {
		fileName = pramas[0];
		url = pramas[1];
		userName = pramas[2];
		passWord = pramas[3];
		table = pramas[4];
		try {
			con = DriverManager.getConnection(url, userName, passWord);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 释放资源
	 * */
	public void destroy() {
		try {
			prestatement.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 用在数据预处理时候计算皮尔森相关系数的时候判断是否已经计算过的函数
	 * 在数据库中进行查询，查询结果为true则表明已经计算过，不比存放到数据库和不比进行相关系数计算操作
	 * 
	 * @param a
	 *            用户a的userID
	 * @param u
	 *            用户u的userID
	 * @exception 抛出sql操作异常
	 * */
	public boolean checkDuplication(int a, int u) throws SQLException {
		String check = "SELECT * FROM " + table
				+ " where userid_a=? AND userid_u=?";
		prestatement = con.prepareStatement(check);
		prestatement.setInt(1, a);
		prestatement.setInt(2, u);
		ResultSet set = prestatement.executeQuery();
		if (set.next()) {
			return true;
		} else
			return false;
	}

	/**
	 * 用在查找已经存放在数据库里的皮尔森相关系数，既查找用户之间的相似度
	 * 
	 * @param a
	 *            用户a的userID
	 * @param u
	 *            用户u的userID
	 * @exception 抛出sql操作异常
	 * */
	public double findCorrelation(int a, int u) throws SQLException {
		String check = "SELECT correlation FROM " + table
				+ " where userid_a=? AND userid_u=?";
		prestatement = con.prepareStatement(check);
		prestatement.setInt(1, a);
		prestatement.setInt(2, u);
		ResultSet set = prestatement.executeQuery();
		while (set.next())
			return set.getDouble(1);
		return 0.0;
	}

	/**
	 * 执行存放皮尔森相关系数的操作，将计算得到的用户之间的相关系数同两个用户的userID
	 * 和correlation一同存放到数据库里面，方便后面进行评分预测
	 * 
	 * @param a
	 *            用户a的userID
	 * @param u
	 *            用户u的userID
	 * @param correlation
	 *            计算得到的皮尔森相关系数
	 * @exception 抛出sql操作异常
	 * */
	public void storeUserBaseCorrelationIntoDB(int a, int u, double correlation)
			throws SQLException {
		String sql = "INSERT INTO " + table
				+ "(USERID_A,USERID_U,CORRELATION) VALUES (?,?,?)";

		prestatement = con.prepareStatement(sql);
		prestatement.setInt(1, a);
		prestatement.setInt(2, u);
		prestatement.setDouble(3, correlation);
		prestatement.executeUpdate();
		System.out.println(a + ":" + u + ":" + correlation);
	}

	public void storeItemBaseCorrelationIntoDB(int i, int j, double correlation)
			throws SQLException {
		String sql = "INSERT INTO " + table
				+ "(MOVIEID_I,MOVIEID_J,CORRELATION) VALUES (?,?,?)";

		prestatement = con.prepareStatement(sql);
		prestatement.setInt(1, i);
		prestatement.setInt(2, j);
		prestatement.setDouble(3, correlation);
		prestatement.executeUpdate();
	}

	/**
	 * 用在将文本文件的数据存放到数据库的操作
	 * */
	public void storeDatasIntoDB() {
		String[] datas;
		File file = new File(fileName);
		String sql = "INSERT INTO " + table
				+ "(USERID,MOVIEID,RATING,TAG) VALUES (?,?,?,?)";
		BufferedReader reader = null;
		try {
			prestatement = con.prepareStatement(sql);
			reader = new BufferedReader(new FileReader(file));
			String lineString = null;
			while ((lineString = reader.readLine()) != null) {
				datas = lineString.split("::");
				prestatement.setInt(1, Integer.valueOf(datas[0]));
				prestatement.setInt(2, Integer.valueOf(datas[1]));
				prestatement.setDouble(3, Double.valueOf(datas[4]));
				prestatement.setString(4, datas[2]);
				prestatement.executeUpdate();
			}
		} catch (SQLException x) {
			x.printStackTrace();
		} catch (IOException x) {
			x.printStackTrace();
		}

	}

	/**
	 * 存储最后的预测数据
	 * 
	 * @param userid
	 *            被预测的用户id
	 * @param movieid
	 *            预测该用户对某一部评分的movieID
	 * @param prediction
	 *            预测的评分数
	 * @param actuality
	 *            实际的评分数
	 * @exception 抛出sql操作异常
	 * */
	public void storePredictionsIntoDB(int userid, int movieid,
			double prediction, double actuality) {
		String sql = "INSERT INTO " + table
				+ "(USERID,MOVIEID,PREDICTION,ACTUALITY) VALUES (?,?,?,?)";
		try {
			prestatement = con.prepareStatement(sql);
			prestatement.setInt(1, userid);
			prestatement.setInt(2, movieid);
			prestatement.setDouble(3, prediction);
			prestatement.setDouble(4, actuality);
			prestatement.executeUpdate();
		} catch (SQLException x) {
			x.printStackTrace();
		}

	}

	/**
	 * 通过用户的id和电影id找到对应的评分
	 * 
	 * @param userid
	 *            用户id
	 * @param movieid
	 *            某一部电影的movieID
	 * @exception 抛出sql操作异常
	 * */
	public double findRatingByUserIdAndMovieId(int userid, int movieid)
			throws SQLException {
		double rating = 0.0;
		String sql = "SELECT DISTINCT rating FROM " + table
				+ " where userid=? AND movieid=?";
		prestatement = con.prepareStatement(sql);
		prestatement.setInt(1, userid);
		prestatement.setInt(2, movieid);
		ResultSet rs = prestatement.executeQuery();
		while (rs.next()) {
			rating = rs.getDouble(1);
		}
		return rating;
	}

	/**
	 * 统计训练集或测试集用户数量
	 * */
	public int countUsers() throws SQLException {
		String sql = "SELECT COUNT(DISTINCT userid) from " + table;
		prestatement = con.prepareStatement(sql);
		int count = 0;
		ResultSet rs = prestatement.executeQuery();
		while (rs.next()) {
			count = rs.getInt(1);
		}
		return count;
	}

	/**
	 * 统计训练集或测试集不同记录数量
	 * */
	public int countDistinctRecord() throws SQLException {
		String sql = "SELECT COUNT(*) from "
				+ "(SELECT DISTINCT userid,movieid FROM " + table + ") A";
		prestatement = con.prepareStatement(sql);
		int count = 0;
		ResultSet rs = prestatement.executeQuery();
		while (rs.next()) {
			count = rs.getInt(1);
		}
		return count;
	}

	/**
	 * 统计训练集或测试集电影数量
	 * */
	public int countMovies() throws SQLException {
		String sql = "SELECT COUNT(DISTINCT movieid) from " + table;
		prestatement = con.prepareStatement(sql);
		int count = 0;
		ResultSet rs = prestatement.executeQuery();
		while (rs.next()) {
			count = rs.getInt(1);
		}
		return count;
	}

	public ResultSet findAllUsers() throws SQLException {
		String sql = "SELECT DISTINCT userid from " + table
				+ " ORDER BY userid ASC";
		prestatement = con.prepareStatement(sql);
		return prestatement.executeQuery();
	}

	/**
	 * 查找用户对某部电影的评分
	 * */
	public ResultSet findUsersANDMoviesANDRatings() throws SQLException {
		String sql = "SELECT DISTINCT userid,movieid,rating from " + table
				+ " ORDER BY userid ASC";
		prestatement = con.prepareStatement(sql);
		return prestatement.executeQuery();
	}

	/**
	 * 查找所有的电影
	 */
	public ResultSet findAllMovies() throws SQLException {
		String sql = "SELECT DISTINCT movieid from " + table
				+ " ORDER BY movieid ASC";
		prestatement = con.prepareStatement(sql);
		return prestatement.executeQuery();
	}

	/**
	 * 通过用户d查找到该用户看过的电影以及相应的评分
	 */
	public ResultSet findMoviesANDRatingsByUserId(int userid)
			throws SQLException {
		String sql = "SELECT DISTINCT movieid,rating from " + table
				+ " WHERE userid=?" + " ORDER BY movieid ASC";
		prestatement = con.prepareStatement(sql);
		prestatement.setInt(1, userid);
		return prestatement.executeQuery();
	}

	/**
	 * 通过电影id查找到所有看过该部电影的用户的id
	 * 
	 * @param movieid
	 *            电影的id
	 * @exception 抛出sql操作异常
	 * */
	public ResultSet findUsersByMovieid(int movieid) throws SQLException {
		String sql = "SELECT DISTINCT userid from " + table
				+ " WHERE movieid=?" + " ORDER BY userid ASC";
		prestatement = con.prepareStatement(sql);
		prestatement.setInt(1, movieid);
		return prestatement.executeQuery();
	}

	public ResultSet findMoviesWithUserInTrainingset() throws SQLException {
		String sql = "SELECT DISTINCT movieid from " + table
				+ " WHERE prediction=0";
		prestatement = con.prepareStatement(sql);
		return prestatement.executeQuery();
	}

	/**
	 * 查找用户的平均评分
	 * 
	 * @param userid
	 *            用户的ID
	 * @exception 抛出sql操作异常
	 * */
	public double getUserAvgRating(int userid) throws SQLException {
		String sql = "SELECT AVG(rating) FROM ("
				+ "SELECT DISTINCT userid, movieid,rating FROM " + table
				+ " WHERE userid=?" + ") temp;";
		prestatement = con.prepareStatement(sql);
		prestatement.setInt(1, userid);
		double rating = 0.0;
		ResultSet rs = prestatement.executeQuery();
		while (rs.next()) {
			rating = rs.getDouble(1);
		}
		return rating;
	}

	/**
	 * 计算皮尔森相关系数
	 * 
	 * @param a
	 *            用户a的userID
	 * @param u
	 *            用户u的userID
	 * @exception 抛出sql操作异常
	 * */
	public double getPearsonCorrelationbetweenUsers(int a, int u)
			throws SQLException {
		ResultSet movieSet = findMoviesANDRatingsByUserId(a);
		double a_avg = getUserAvgRating(a);
		double u_avg = getUserAvgRating(u);
		double ra_i = 0.0, ru_i = 0.0;
		double denominator = 1.0, numerator = 0.0;
		double denominator_1 = 0.0, denominator_2 = 0.0;
		while (movieSet.next()) {
			ra_i = movieSet.getDouble(2);
			ru_i = findRatingByUserIdAndMovieId(u, movieSet.getInt(1));
			// 排除没有在训练集出现过的用户
			if (ru_i == 0)
				continue;
			numerator += (ra_i - a_avg) * (ru_i - u_avg);
			denominator_1 += Math.pow((ra_i - a_avg), 2);
			denominator_2 += Math.pow((ru_i - u_avg), 2);
		}
		denominator = Math.sqrt(denominator_2 * denominator_1);
		if (denominator == 0)
			return 0.0;
		BigDecimal result = new BigDecimal(numerator / denominator);
		result.setScale(5, 2);
		return result.doubleValue();
	}

}
