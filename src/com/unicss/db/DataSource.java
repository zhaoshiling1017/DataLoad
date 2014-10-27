package com.unicss.db;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.lang.StringUtils;


/**
 * @author lenzhao 
 * @email zhaosl1017@gmail.com
 * @date  2014-10-22 上午10:54:17
 */
public class DataSource {
	private String mysqlHost;
	private String mysqlUname;
	private String mysqlPwd;
	private String pgHost;
	private String pgUname;
	private String pgPwd;
	public String getMysqlHost() {
		return mysqlHost;
	}
	public void setMysqlHost(String mysqlHost) {
		this.mysqlHost = mysqlHost;
	}
	public String getMysqlUname() {
		return mysqlUname;
	}
	public void setMysqlUname(String mysqlUname) {
		this.mysqlUname = mysqlUname;
	}
	public String getMysqlPwd() {
		return mysqlPwd;
	}
	public void setMysqlPwd(String mysqlPwd) {
		this.mysqlPwd = mysqlPwd;
	}
	public String getPgHost() {
		return pgHost;
	}
	public void setPgHost(String pgHost) {
		this.pgHost = pgHost;
	}
	public String getPgUname() {
		return pgUname;
	}
	public void setPgUname(String pgUname) {
		this.pgUname = pgUname;
	}
	public String getPgPwd() {
		return pgPwd;
	}
	public void setPgPwd(String pgPwd) {
		this.pgPwd = pgPwd;
	}
	public DataSource(String mysqlHost,String mysqlUname,String mysqlPwd,String pgHost,String pgUname,String pgPwd) {
		this.mysqlHost = mysqlHost;
		this.mysqlUname = mysqlUname;
		this.mysqlPwd = mysqlPwd;
		this.pgHost = pgHost;
		this.pgUname = pgUname;
		this.pgPwd = pgPwd;
	}
	public DataSource(String mysqlUname,String mysqlPwd,String pgUname,String pgPwd) {
		this.mysqlHost = "localhost";
		this.pgHost = "localhost";
		this.mysqlUname = mysqlUname;
		this.mysqlPwd = mysqlPwd;
		this.pgUname = pgUname;
		this.pgPwd = pgPwd;
	}
	//获取mysql连接
	public Connection getMysqlConn() throws Exception {
		Connection conn = null;
		Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://"+this.mysqlHost+":3306/honeycomb",this.mysqlUname,this.mysqlPwd);
		return conn;
	}
	//获取postgres连接
	public Connection getPostgresConn() throws Exception {
		Connection conn = null;
		Class.forName("org.postgresql.Driver");
	    conn = DriverManager.getConnection("jdbc:postgresql://"+this.pgHost+":5432/cc_base",this.pgUname,this.pgPwd);
		return conn;
	}
}
