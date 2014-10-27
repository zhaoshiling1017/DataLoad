package com.unicss.db;
/**
 * @author lenzhao 
 * @email zhaosl1017@gmail.com
 * @date  2014-10-22 上午11:22:53
 */
public class Main {
	public static void main(String[] args) {
		DataSource ds = null;
		String schema = "";
		String path = "";
		boolean flag = true;
		if(args.length != 0 && args.length == 8){
			String mysqlHost = args[0];
			String mysqlUname = args[1];
			String mysqlPwd = args[2];
			String pgHost = args[3];
			String pgUname = args[4];
			String pgPwd = args[5];
			schema = args[6];
			path = args[7];
			ds = new DataSource(mysqlHost,mysqlUname,mysqlPwd,pgHost,pgUname,pgPwd);
		}else if(args.length != 0 && args.length == 6){
			String mysqlUname = args[0];
			String mysqlPwd = args[1];
			String pgUname = args[2];
			String pgPwd = args[3];
			schema = args[4];
			path = args[5];
			ds = new DataSource(mysqlUname,mysqlPwd,pgUname,pgPwd);
		}else{
			flag = false;
		}
		//DBUtil.dropSequences(ds,"cc_call_record","em_101101101");
		
		if(flag){
			DBUtil.execute(ds, schema, path);
		}else{
			System.out.println("params set error. example: [mysqlHost] mysqlUname mysqlPwd [pgHost] pgUname pgPwd schema filePath");
		}
	}	
}
