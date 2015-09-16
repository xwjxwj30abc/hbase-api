package zx.soft.hbase.api.core;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HConn {

	private static HConnection conn;
	static {
		try {
			conn = HConnectionManager.createConnection(HBaseConfig.getZookeeperConf());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HConnection getHConnection() {
		return conn;
	}

	public static void closeConn() {
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
