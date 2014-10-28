package com.unicss.db;

import com.unicss.ssh2.SSH2Conn;

/**
 * @author lenzhao 
 * @email zhaosl1017@gmail.com
 * @date  2014-10-22 上午11:22:53
 */
public class Main {
	public static void main(String[] args) {
		/*DataSource ds = null;
		String schema = "";
		String path = "";
		SSH2Conn ssh2 = null;
		boolean flag = true;
		if(args.length != 0 && args.length == 10){
			String mysqlHost = args[0];
			String mysqlUname = args[1];
			String mysqlPwd = args[2];
			String pgHost = args[3];
			String pgUname = args[4];
			String pgPwd = args[5];
			schema = args[6];
			path = args[7];
			String username = args[8];
			String pwd = args[9];
			ds = new DataSource(mysqlHost,mysqlUname,mysqlPwd,pgHost,pgUname,pgPwd);
			ssh2 = new SSH2Conn(pgHost, username, pwd);
		}else if(args.length != 0 && args.length == 8){
			String mysqlUname = args[0];
			String mysqlPwd = args[1];
			String pgUname = args[2];
			String pgPwd = args[3];
			schema = args[4];
			path = args[5];
			String username = args[6];
			String pwd = args[7];
			ds = new DataSource(mysqlUname,mysqlPwd,pgUname,pgPwd);
			ssh2 = new SSH2Conn(username, pwd);
		}else{
			flag = false;
		}*/
		//DBUtil.dropSequences(ds,"cc_call_record","em_101101101");
		
		/*if(flag){
			DBUtil.execute(ds, schema, path,ssh2);
		}else{
			System.out.println("params set error. example: [mysqlHost] mysqlUname mysqlPwd [pgHost] pgUname pgPwd schema filePath username password");
		}*/
		SSH2Conn ssh2 = new SSH2Conn("192.168.1.231", "root", "bjiamcall");
		String cmd = "scp root@192.168.1.229:/tmp/test.csv /tmp/";
		boolean isSuc = ssh2.executeCmd(cmd);
		System.out.println(isSuc);
	}	
}
