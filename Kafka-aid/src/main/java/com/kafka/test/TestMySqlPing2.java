package com.kafka.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;

import junit.framework.TestCase;

public class TestMySqlPing2 extends TestCase {

	private String jdbcUrl;
	private String user;
	private String password;
	private String driverClass;

	private DruidDataSource dataSource;

	protected void setUp() throws Exception {
		// jdbcUrl = "jdbc:oracle:thin:@a.b.c.d:1521:ocnauto";
		// user = "alibaba";
		// password = "ccbuauto";
		// SQL = "SELECT * FROM WP_ORDERS WHERE ID = ?";

		jdbcUrl = "jdbc:mysql://120.26.230.18:3306/kafka";
		user = "kafka";
		password = "kafka!";

		driverClass = "com.mysql.jdbc.Driver";

		dataSource = new DruidDataSource();
		dataSource.setUrl(jdbcUrl);
		dataSource.setUsername(user);
		dataSource.setPassword(password);

		dataSource.setMaxActive(4);
		dataSource.setMinIdle(1);
		dataSource.setTestOnBorrow(true);
	}

	public void test_o() throws Exception {

		Connection conn = dataSource.getConnection();

		String sql = "SELECT * FROM test1 WHERE 1=1";
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setQueryTimeout(1);
		ResultSet rs = stmt.executeQuery();
		JdbcUtils.printResultSet(rs);
		rs.close();
		stmt.close();
		conn.close();
	}

	public void pring(com.mysql.jdbc.Connection oracleConn) throws SQLException {
		oracleConn.ping();
	}

	public void select(com.mysql.jdbc.Connection oracleConn) throws SQLException {
		Statement stmt = oracleConn.createStatement();
		stmt.execute("SELECT 'x'");
		stmt.close();
	}
}