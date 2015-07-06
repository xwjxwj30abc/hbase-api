package zx.soft.hbase.api.core;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import zx.soft.utils.config.ConfigUtil;

import com.google.protobuf.ServiceException;

public class HBaseClient {

	private static HBaseAdmin hbaseAdmin;
	private static Configuration conf;

	static {
		Properties prop = ConfigUtil.getProps("zookeeper.properties");
		//在classpath下查找hbase-site.xml文件，如果不存在，则使用默认的hbase-core.xml文件
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"));
	}

	public HBaseClient() throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		hbaseAdmin = new HBaseAdmin(conf);
		HBaseAdmin.checkHBaseAvailable(conf);
	}

	/**
	 * 通过表名和列族创建表
	 * @param tableName
	 * @param columnFamilys
	 * @throws IOException
	 */
	public boolean createTable(String tableName, String[] columnFamilys) throws IOException {
		boolean success = false;
		if (!(hbaseAdmin.tableExists(tableName))) {
			TableName name = TableName.valueOf(tableName);
			HTableDescriptor tableDescriptor = new HTableDescriptor(name);
			for (String columnFamily : columnFamilys) {
				tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			try {
				hbaseAdmin.createTable(tableDescriptor);
				success = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return success;
	}

	/**
	 * 判断指定表名的表是否存在
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean isTableExists(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName);
	}

	/**
	 * 删除指定表名的表,若待删除的表不存在，返回false
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean deleteTable(String tableName) throws IOException {
		boolean deleted = false;
		if (hbaseAdmin.tableExists(tableName)) {
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
			deleted = true;
		}
		return deleted;
	}

	/**
	 * 关闭hbaseAdmin
	 * @throws IOException
	 */
	public void close() throws IOException {
		hbaseAdmin.close();
	}

}
