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
 * ��������������ı������� ���ı����ݴ�ŵ����ݿ� ����user����Ŀ ����movie����Ŀ ѵ���� total user:1870 total
 * movie:4797 ���Լ� total user:1043 total movie:2955 ��ȡ���ݿ�����ݹ���user-item����
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
	 *            pramas[0]:fileName �ı��ļ��� pramas[1]:url ���ݿ�url��ַ
	 *            pramas[2]:userName ���ݿ��û��� pramas[3]��password ���ݿ�����
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
	 * �ͷ���Դ
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
	 * ��������Ԥ����ʱ�����Ƥ��ɭ���ϵ����ʱ���ж��Ƿ��Ѿ�������ĺ���
	 * �����ݿ��н��в�ѯ����ѯ���Ϊtrue������Ѿ�����������ȴ�ŵ����ݿ�Ͳ��Ƚ������ϵ���������
	 * 
	 * @param a
	 *            �û�a��userID
	 * @param u
	 *            �û�u��userID
	 * @exception �׳�sql�����쳣
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
	 * ���ڲ����Ѿ���������ݿ����Ƥ��ɭ���ϵ�����Ȳ����û�֮������ƶ�
	 * 
	 * @param a
	 *            �û�a��userID
	 * @param u
	 *            �û�u��userID
	 * @exception �׳�sql�����쳣
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
	 * ִ�д��Ƥ��ɭ���ϵ���Ĳ�����������õ����û�֮������ϵ��ͬ�����û���userID
	 * ��correlationһͬ��ŵ����ݿ����棬��������������Ԥ��
	 * 
	 * @param a
	 *            �û�a��userID
	 * @param u
	 *            �û�u��userID
	 * @param correlation
	 *            ����õ���Ƥ��ɭ���ϵ��
	 * @exception �׳�sql�����쳣
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
	 * ���ڽ��ı��ļ������ݴ�ŵ����ݿ�Ĳ���
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
	 * �洢����Ԥ������
	 * 
	 * @param userid
	 *            ��Ԥ����û�id
	 * @param movieid
	 *            Ԥ����û���ĳһ�����ֵ�movieID
	 * @param prediction
	 *            Ԥ���������
	 * @param actuality
	 *            ʵ�ʵ�������
	 * @exception �׳�sql�����쳣
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
	 * ͨ���û���id�͵�Ӱid�ҵ���Ӧ������
	 * 
	 * @param userid
	 *            �û�id
	 * @param movieid
	 *            ĳһ����Ӱ��movieID
	 * @exception �׳�sql�����쳣
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
	 * ͳ��ѵ��������Լ��û�����
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
	 * ͳ��ѵ��������Լ���ͬ��¼����
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
	 * ͳ��ѵ��������Լ���Ӱ����
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
	 * �����û���ĳ����Ӱ������
	 * */
	public ResultSet findUsersANDMoviesANDRatings() throws SQLException {
		String sql = "SELECT DISTINCT userid,movieid,rating from " + table
				+ " ORDER BY userid ASC";
		prestatement = con.prepareStatement(sql);
		return prestatement.executeQuery();
	}

	/**
	 * �������еĵ�Ӱ
	 */
	public ResultSet findAllMovies() throws SQLException {
		String sql = "SELECT DISTINCT movieid from " + table
				+ " ORDER BY movieid ASC";
		prestatement = con.prepareStatement(sql);
		return prestatement.executeQuery();
	}

	/**
	 * ͨ���û�d���ҵ����û������ĵ�Ӱ�Լ���Ӧ������
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
	 * ͨ����Ӱid���ҵ����п����ò���Ӱ���û���id
	 * 
	 * @param movieid
	 *            ��Ӱ��id
	 * @exception �׳�sql�����쳣
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
	 * �����û���ƽ������
	 * 
	 * @param userid
	 *            �û���ID
	 * @exception �׳�sql�����쳣
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
	 * ����Ƥ��ɭ���ϵ��
	 * 
	 * @param a
	 *            �û�a��userID
	 * @param u
	 *            �û�u��userID
	 * @exception �׳�sql�����쳣
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
			// �ų�û����ѵ�������ֹ����û�
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
