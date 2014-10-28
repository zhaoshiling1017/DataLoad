package com.unicss.db;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.lang.StringUtils;

import com.unicss.ssh2.SSH2Conn;


/**
 * @author lenzhao 
 * @email zhaosl1017@gmail.com
 * @date  2014-10-24 上午10:00:06
 */
public class DBUtil {
	public static void execute(DataSource ds,String schema,String path,SSH2Conn ssh2){
		//判断文件夹路径是否已斜杠结尾
		if (null != path && !path.endsWith("/")) {
			path = path + "/";
		}
		//清除指定文件夹下csv文件
		checkFileExist(path);
		//操作用户组表
		opGroupTable(ds,schema,path,ssh2);
		//操作用户表
		opUserTable(ds,schema,path,ssh2);
		//关联用户组表和用户表
		linkUserAndGroup(ds,schema);
		//操作项目表
		opProjectTable(ds,schema);
		//操作项目参数表
		opProjectParamsTable(ds,schema);
		List<String> tableNameList = getTableList(ds,"callsheets");
		for(String tableName : tableNameList){
			String pgTableName = "cc_call_record";
			if(tableName.indexOf("_") != -1){
				String str = tableName.substring(tableName.indexOf("_")+1);
				pgTableName = "cc_call_record_"+str.replace("_", "");
			}
			//判断是否存在指定的表,如果不存在创建一张表
			checkTableExist(ds,schema,pgTableName,"cc_call_record");
			//操作话单表
			opCallRecordTable(ds,schema,tableName,pgTableName, path,ssh2);
		}
	}
	//获取表名
	public static List<String> getTableList(DataSource ds,String tableName) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		List<String> list = new ArrayList<String>();
		try {
			conn = ds.getMysqlConn();
			stmt = conn.createStatement();
			String sql = "show tables like '"+tableName+"%';";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String name = rs.getString(1);
				list.add(name);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}
	//判断是否存在文件，存在删掉
	public static void checkFileExist(String path) {
		File dir = new File(path);
		File[] files = dir.listFiles();
		for (File file : files) {
			String fileName = file.getName();
			if(fileName.endsWith(".csv")){
				file.delete();
			}
		}
		
	}
	//判断表是否存在，不存在新建一张表
	public static void checkTableExist(DataSource ds,String schema,String newTable,String oldTable) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = ds.getPostgresConn();
			String sql1 = "select count(*) from information_schema.tables where table_schema='"+schema+"' and table_type='BASE TABLE' and table_name='"+newTable+"';";
			System.out.println(sql1);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql1);
			while (rs.next()) {
				int count = rs.getInt(1);
				if (count == 0) {
					//String sql2 = "CREATE SEQUENCE "+schema+"."+newTable+"_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;";
					String sql3 = "select * into "+schema+"."+newTable+" from "+schema+"."+oldTable+" where 1<>1;";
					String sql4 = "ALTER TABLE "+schema+"."+newTable+" add primary KEY(id)";
					//String sql5 = "alter table "+schema+"."+newTable+" alter column id set default nextval('"+schema+".hibernate_sequence');";
					String sql6 = " ALTER TABLE "+schema+"."+newTable+" ALTER COLUMN dnis TYPE varchar(255);";
					String sql7 = "ALTER TABLE "+schema+"."+newTable+" OWNER TO "+schema+";";
					//String sql8 = "alter sequence "+schema+"."+newTable+"_id_seq owner to "+schema+";";
					//stmt.execute(sql2);
					stmt.execute(sql3);
					stmt.execute(sql4);
					//stmt.execute(sql5);
					stmt.execute(sql6);
					stmt.execute(sql7);
					//stmt.execute(sql8);
				}
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//操作话单表
	public static void opCallRecordTable(DataSource ds,String schema,String mySqlTableName,String pgTableName,String path,SSH2Conn ssh2) {
		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		ResultSet rs3 = null;
		try {
			conn1 = ds.getMysqlConn();
			conn2 = ds.getPostgresConn();
			String sql0 = "select count(*) from "+mySqlTableName +";";
			int count = 0;
			stmt1 = conn1.createStatement();
			rs1 = stmt1.executeQuery(sql0);
			while (rs1.next()) {
				count = rs1.getInt(1);
			}
			String sql1 = "select id, ani,dnis, begin_at, connect_at,end_at,IF(agent_dn,agent_dn,null)," +
					"IF(agent_jobcode,agent_jobcode,null), agent_connect_at,record_file_name,direction,transfer_agent_at,call_id," +
					"call_type,IF(tag,tag,null),created_at from "+mySqlTableName +" order by id into outfile '"+path+mySqlTableName+".csv' fields terminated by ',' lines terminated by '\r\n';" ;
			stmt1.execute(sql1);
			String sql2 = "copy "+schema+"."+pgTableName+"(id,ani,dnis,begin_at,connect_at,end_at,device_no,agent_job_code,agent_connect_at,file_path,call_direction,transfer_agent_at,call_id,call_type," +
					"tag,created_at) from '"+path+mySqlTableName+".csv' delimiter as ',';";
			//String cmd = "scp root@"+ds.getMysqlHost()+":"+path+mySqlTableName+".csv "+path;
			stmt2 = conn2.createStatement();
			//boolean isSuc = ssh2.executeCmd(cmd);
			//Thread.sleep(10*1000);
			/*if (isSuc) {
				
			}else{
				System.err.println("操作话单表,csv文件拷贝失败");
			}*/
			stmt2.execute(sql2);
			String sql3 = "select id from "+schema+"."+pgTableName+" order by id desc limit 1";
			int id = 0;
			rs2 = stmt2.executeQuery(sql3);
			while (rs2.next()) {
				id = rs2.getInt(1);
			}
			String sql4 = "select (select p.name from projects p where p.id=project_id),(select ug.name from user_groups ug where ug.id=(select u.user_group_id from  users u where u.jobcode=agent_jobcode )),agent_connect_at  from "+mySqlTableName +" order by id";
			rs3 = stmt1.executeQuery(sql4);
			int i = id-count;
			while (rs3.next()) {
				i++;
				String projectName = rs3.getString(1);
				String groupName = rs3.getString(2);
				Date agentConnectAt = rs3.getDate(3);
				String sql5 = "";
				if (null != agentConnectAt) {
					sql5 = "update "+schema+"."+pgTableName+" set project_id=(select p.id from "+schema+".cc_project p where p.name='"+projectName+"' limit 1),group_id=(select g.id from "+schema+".cc_group g where g.name='"+groupName+"' limit 1),if_connected=1 where id="+i;
				} else {
					sql5 = "update "+schema+"."+pgTableName+" set project_id=(select p.id from "+schema+".cc_project p where p.name='"+projectName+"' limit 1),group_id=(select g.id from "+schema+".cc_group g where g.name='"+groupName+"' limit 1),if_connected=0 where id="+i;
				}
				 
				stmt2.execute(sql5);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
			if (null != stmt1) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != stmt2) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn1) {
				try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn2) {
				try {
					conn2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//项目参数表
	public static void opProjectParamsTable(DataSource ds,String schema) {
		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs1 = null;
		try {
			conn1 = ds.getMysqlConn();
			conn2 = ds.getPostgresConn();
			String sql1 = "select p.name,p.mode,p.status,(select g.name from user_groups g where g.id=p.user_group_id) as group_name,date_format(created_at,'%Y-%m-%d %H:%i:%s') created_at,date_format(updated_at,'%Y-%m-%d %H:%i:%s') updated_at,p.display_numbers, (select u.nickname from users u where u.id=p.owner_id) nickname,p.cps from projects p";
			stmt1 = conn1.createStatement();
			rs1 = stmt1.executeQuery(sql1);
			while (rs1.next()) {
				String name = rs1.getString(1);
				//String groupName = rs1.getString(4);
				String createdAt = rs1.getString(5);
				String updatedAt = rs1.getString(6);
				String displayNums = rs1.getString(7);
				String nickname = rs1.getString(8);
				int cps = rs1.getInt(9);
				if (StringUtils.isNotBlank(name)) {
					String sql3 = "insert into "+schema+".CC_PROJECT_PARAMS(id,project_id,phones,ratio,if_play_voice,if_transfer,created_at,operator_id,updated_at,answer_type,call_type,call_speed,if_continue,ring_time_out,concurrent_num) values(nextval('"+schema+".hibernate_sequence'),(select p.id from "+schema+".CC_PROJECT p where p.name='"+name+"' limit 1),'"+displayNums+"',1,0,1,'"+createdAt+"'::timestamp,(select u.id from "+schema+".cc_user u where u.user_name='"+nickname+"' limit 1)," +
							"'"+updatedAt+"'::timestamp,'connect','capacity',"+cps+",1,20,1)";
					stmt2 = conn2.createStatement();
					stmt2.execute(sql3);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt1) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != stmt2) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn1) {
				try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn2) {
				try {
					conn2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//项目表
	public static void opProjectTable(DataSource ds,String schema) {
		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs1 = null;
		try {
			conn1 = ds.getMysqlConn();
			conn2 = ds.getPostgresConn();
			String sql1 = "select p.name,p.mode,p.status,(select g.name from user_groups g where g.id=p.user_group_id) as group_name,date_format(created_at,'%Y-%m-%d %H:%i:%s') created_at,date_format(updated_at,'%Y-%m-%d %H:%i:%s') updated_at,p.display_numbers, (select u.nickname from users u where u.id=p.owner_id) nickname,p.cps from projects p";
			stmt1 = conn1.createStatement();
			rs1 = stmt1.executeQuery(sql1);
			while (rs1.next()) {
				String name = rs1.getString(1);
				String mode = rs1.getString(2);
				String status = rs1.getString(3);
				String createdAt = rs1.getString(5);
				String updatedAt = rs1.getString(6);
				String nickname = rs1.getString(8);
				//System.out.println("name:"+name+",createdAt:"+createdAt);
				if (StringUtils.isNotBlank(name)) {
					String sql2 = "insert into "+schema+".CC_PROJECT(id,name,type,administrator,if_reply,created_at,creator_id,updated_at,operator_id,status,first_start_time,last_start_time) values(nextval('"+schema+".hibernate_sequence'),'"+name+"','"+mode+"'," +
							"(select u.id from "+schema+".cc_user u where u.user_name='"+nickname+"' limit 1),"+0+",'"+createdAt+"'::timestamp,(select u.id from "+schema+".cc_user u where u.user_name='"+nickname+"' limit 1),'"+updatedAt+"'::timestamp,(select u.id from "+schema+".cc_user u where u.user_name = '"+nickname+"' limit 1),'"+status+"','"+createdAt+"'::timestamp,now()::timestamp)";
					stmt2 = conn2.createStatement();
					stmt2.execute(sql2);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt1) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != stmt2) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn1) {
				try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn2) {
				try {
					conn2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//用户表和用户组表建立链接
	public static void linkUserAndGroup(DataSource ds,String schema) {
		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs1 = null;
		try {
			conn1 = ds.getMysqlConn();
			conn2 = ds.getPostgresConn();
			String sql1 = "select a.nickname,b.name from users a ,user_groups b  where a.user_group_id = b.id";
			stmt1 = conn1.createStatement();
			rs1 = stmt1.executeQuery(sql1);
			while (rs1.next()) {
				String nickname = rs1.getString(1);
				String name = rs1.getString(2);
				if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(nickname)) {
					String sql2 = "insert into "+schema+".cc_user_group(users_id,groups_id) values((select u.id from "+schema+".cc_user u where u.user_name = '"+nickname+"' limit 1),(select g.id from "+schema+".cc_group g where g.name='"+name+"' limit 1))";
					stmt2 = conn2.createStatement();
					stmt2.execute(sql2);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt1) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != stmt2) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn1) {
				try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn2) {
				try {
					conn2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//操作用户表
	public static void opUserTable(DataSource ds,String schema,String path,SSH2Conn ssh2) {
		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs = null;
		try {
			conn1 = ds.getMysqlConn();
			conn2 = ds.getPostgresConn();
			String sql1 = "select nickname,name,jobcode,did,date_format(created_at,'%Y-%m-%d %H:%i:%s') from users";
			stmt1 = conn1.createStatement();
			rs = stmt1.executeQuery(sql1);
			while(rs.next()){
				String nickname = rs.getString(1);
				String name = rs.getString(2);
				String jobcode = rs.getString(3);
				String did = rs.getString(4);
				String created_at = rs.getString(5);
				String sql2 = "insert into "+schema+".cc_user(id,user_name,name,staff_no,phone,created_at,if_verify,if_use,if_system) values(nextval('"+schema+".hibernate_sequence'),'"+nickname+"','"+name+"','"+jobcode+"','"+did+"','"+created_at+"'::timestamp,0,1,0)";
				stmt2 = conn2.createStatement();
				stmt2.execute(sql2);
			}
			/*String sql1 = "select nickname,name,jobcode,did,created_at,0,1,0 from users into outfile '"+path+"users.csv' fields terminated by ',' lines terminated by '\r\n';";
			String sql2 = "copy "+schema+".cc_user(user_name,name,staff_no,phone,created_at,if_verify,if_use,if_system) from '"+path+"users.csv' delimiter as ',' ;";
			executeCsv(conn1,sql1);
			String cmd = "scp root@"+ds.getMysqlHost()+":"+path+"users.csv "+path;
			boolean isSuc = ssh2.executeCmd(cmd);
			Thread.sleep(10*1000);
			if (isSuc) {
				executeCsv(conn2,sql2);
			}else{
				System.err.println("操作用户表,csv文件拷贝失败");
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt1) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != stmt2) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn1) {
				try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn2) {
				try {
					conn2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	//操作用户组表
	public static void opGroupTable(DataSource ds,String schema,String path,SSH2Conn ssh2) {
		Connection conn1 = null;
		Connection conn2 = null;
		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs = null;
		try {
			conn1 = ds.getMysqlConn();
			conn2 = ds.getPostgresConn();
			String sql1 = "select name,date_format(created_at,'%Y-%m-%d %H:%i:%s'),date_format(updated_at,'%Y-%m-%d %H:%i:%s') from user_groups";
			stmt1 = conn1.createStatement();
			rs = stmt1.executeQuery(sql1);
			while(rs.next()){
				String name = rs.getString(1);
				String created_at = rs.getString(2);
				String updated_at = rs.getString(3);
				String sql2 = "insert into "+schema+".cc_group(id,name,created_at,updated_at,status) values(nextval('"+schema+".hibernate_sequence'),'"+name+"','"+created_at+"'::timestamp,'"+updated_at+"'::timestamp,0)";
				stmt2 = conn2.createStatement();
				stmt2.execute(sql2);
			}
			//String sql1 = "select name,created_at,updated_at,0 from user_groups into outfile '"+path+"user_groups.csv' fields terminated by ',' lines terminated by '\r\n';";
			//String sql2 = "copy "+schema+".cc_group (name,created_at,updated_at,status) from '"+path+"user_groups.csv' delimiter as ',';";
			//executeCsv(conn1,sql1);
			//String cmd = "scp root@"+ds.getMysqlHost()+":"+path+"user_groups.csv "+path;
			/*boolean isSuc = ssh2.executeCmd(cmd);
			Thread.sleep(10*1000);
			if (isSuc) {
				executeCsv(conn2,sql2);
			}else{
				System.err.println("操作用户组表,csv文件拷贝失败");
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt1) {
				try {
					stmt1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != stmt2) {
				try {
					stmt2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn1) {
				try {
					conn1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != conn2) {
				try {
					conn2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	//执行CSV文件
	public static void executeCsv(Connection conn,String sql) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	public static void dropTables(DataSource ds,String tableName,String schema) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = ds.getPostgresConn();
			String sql = "select table_name  from information_schema.tables where table_schema='"+schema+"' and table_type='BASE TABLE' and table_name like '"+tableName+"_%';";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String tname = rs.getString(1);
				String sql2 = "drop table "+schema+"."+tname;
				stmt.execute(sql2);
				rs = stmt.executeQuery(sql);
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			
			
		}
	}
	public static void dropSequences(DataSource ds,String seqName,String schema) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = ds.getPostgresConn();
			String sql = "select sequence_name from information_schema.SEQUENCEs  where sequence_schema = '"+schema+"' and sequence_name like '"+seqName+"_%' and sequence_name <> 'cc_call_record_id_seq'";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String tname = rs.getString(1);
				String sql2 = "drop SEQUENCE "+schema+"."+tname;
				stmt.execute(sql2);
				rs = stmt.executeQuery(sql);
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			
			
		}
	}
}
