package com.kafka.dao.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;

public class MysqlUtil {

	private final String jdbcUrl = "jdbc:mysql://120.26.230.18:3306/kafka";
	private final String user = "kafka";
	private final String password = "kafka!";

	private Connection conn = null;
	private DruidDataSource druidDataSource;
	private static MysqlUtil instance = null;

	public static synchronized MysqlUtil getInstance() {
		if (instance == null) {
			instance = new MysqlUtil();
		}
		return instance;
	}

	/**
	 * get druid connection
	 * 
	 * @return
	 */
	public Connection getConnection() {
		druidDataSource = new DruidDataSource();
		druidDataSource.setUrl(jdbcUrl);
		druidDataSource.setUsername(user);
		druidDataSource.setPassword(password);

		druidDataSource.setMaxActive(4);
		druidDataSource.setMinIdle(3);
		druidDataSource.setTestOnBorrow(true);

		try {
			this.conn = druidDataSource.getConnection();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return this.conn;
	}

	public Map<String, String> getTableMeta(String _schema, String _tablename) {
		Map<String, String> tableMap = new HashMap<String, String>();
		String tableKey = _schema + "." + _tablename;

		ResultSet rs = null;
		try {
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = getConnection();
			}
			PreparedStatement prep = this.conn.prepareStatement("SELECT * FROM " + tableKey);
			rs = prep.executeQuery();

			ResultSetMetaData resultMetaData = prep.getMetaData();
			int columnCounts = resultMetaData.getColumnCount();
			String resultRow = "";
			for (int i = 0; i < columnCounts; i++) {
				tableMap.put(resultMetaData.getColumnName(i + 1), resultMetaData.getColumnTypeName(i + 1));
				// resultRow = "colname=" + resultMetaData.getColumnName(i + 1)
				// + "|type="
				// + resultMetaData.getColumnType(i + 1);
				// resultRow += "|typeName=" +
				// resultMetaData.getColumnTypeName(i + 1);
				// resultRow += "|typeClassName=" +
				// resultMetaData.getColumnClassName(i + 1);
				// resultRow += "|typeDisplaySize=" +
				// resultMetaData.getColumnDisplaySize(i + 1);
				// resultRow += "|typeTableName=" +
				// resultMetaData.getTableName(i + 1);
				// resultRow += "|typeCatalogName=" +
				// resultMetaData.getCatalogName(i + 1);
				// System.out.println(resultRow);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(tableMap);
		return tableMap;
	}

	public void excuteKafkaUpdate(List<ConsumerRecord<String, String>> buffer) {
		String sql = "INSERT INTO test1 (b) VALUES(?)";
		PreparedStatement prep = null;

		try {
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = getConnection();
			}
			this.conn.setAutoCommit(false);
			prep = this.conn.prepareStatement(sql);
			for (ConsumerRecord<String, String> record : buffer) {
				prep.setString(1, record.value());
				prep.addBatch();

			}
			prep.executeBatch();
			this.conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				this.conn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} finally {
			try {
				this.conn.setAutoCommit(true);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * @param conn
	 * @return
	 */
	public ResultSet excutePrepareUpdate(String _sql, int _timeout_second) {
		ResultSet rs = null;
		try {
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = getConnection();
			}
			PreparedStatement prep = this.conn.prepareStatement(_sql);
			prep.setQueryTimeout(_timeout_second);
			rs = prep.executeQuery();
			ResultSetMetaData resultMetaData = prep.getMetaData();
			int columnCounts = resultMetaData.getColumnCount();
			String resultRow = "";
			for (int i = 0; i < columnCounts; i++) {
				resultRow = "colname=" + resultMetaData.getColumnName(i + 1) + "|type="
						+ resultMetaData.getColumnType(i + 1);
				resultRow += "|typeName=" + resultMetaData.getColumnTypeName(i + 1);
				resultRow += "|typeClassName=" + resultMetaData.getColumnClassName(i + 1);
				resultRow += "|typeDisplaySize=" + resultMetaData.getColumnDisplaySize(i + 1);
				resultRow += "|typeTableName=" + resultMetaData.getTableName(i + 1);
				resultRow += "|typeCatalogName=" + resultMetaData.getCatalogName(i + 1);
				resultRow += "|xxxxxxxxxx=" + resultMetaData.getColumnTypeName(i + 1);
				System.out.println(resultRow);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;

	}

	/**
	 * 
	 * @param sql
	 * @param arr
	 * @return
	 */
	public int excuteUpdate(String sql, String[] arr) {
		int row = 0;

		try {
			if (this.conn == null || this.conn.isClosed()) {
				this.conn = getConnection();
			}
			PreparedStatement prep = conn.prepareStatement(sql);
			if (arr != null && arr.length > 0) {
				for (int i = 0; i < arr.length; i++) {
					prep.setString(i + 1, arr[i]);
				}
			}
			row = prep.executeUpdate();
		} catch (SQLException e) {

			e.printStackTrace();
		}
		return row;
	}

	/**
	 * 
	 */
	private void closeAll() {

		try {
			if (this.conn != null) {
				this.conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String sql = "SELECT * FROM test1 WHERE 1=1";

		MysqlUtil.getInstance().getTableMeta("kafka", "test1");
		// ResultSet rs = mysqlUtil.getInstance().excuteQuery(sql, 1);
		// try {
		// JdbcUtils.printResultSet(rs);
		// } catch (SQLException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		String arr[] = { "2", "aaa" };
		// ConnnectionUtil.getInstance().excuteUpdate("insert into test1(id,b)
		// values(?,?)", arr);
		// ConnnectionUtil.getInstance().closeAll();
	}

}