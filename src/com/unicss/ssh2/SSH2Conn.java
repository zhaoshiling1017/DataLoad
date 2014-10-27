package com.unicss.ssh2;

import java.io.IOException;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

public class SSH2Conn {
	private String hostname;
	private String username;
	private String password;
	
	public SSH2Conn(String hostname, String username, String password) {
		super();
		this.hostname = hostname;
		this.username = username;
		this.password = password;
	}
	
	public SSH2Conn(String username, String password) {
		super();
		this.hostname = "localhost";
		this.username = username;
		this.password = password;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean executeCmd(String cmd) {
		boolean isSuc = false;
		try{
			Connection conn = new Connection(this.hostname);
			conn.connect();
			boolean isAuthenticated = conn.authenticateWithPassword(this.username, this.password);
			if (isAuthenticated == false)
				throw new IOException("Authentication failed.");
			Session sess = conn.openSession();
			sess.execCommand(cmd);
			/*InputStream stdout = new StreamGobbler(sess.getStdout());
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
			while (true) {
				String line = br.readLine();
				if (line == null)
					break;
				System.out.println(line);
			}*/
			//得到脚本运行成功与否的标志 ：0－成功 非0－失败
			System.out.println("ExitCode: " + sess.getExitStatus());
			if (sess.getExitStatus() == 0) {
				isSuc = true;
			}
			sess.close();
			conn.close();
		}
		catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(2);
		}
		return isSuc;
	}
}
