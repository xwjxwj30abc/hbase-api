package zx.soft.hbase.api.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import zx.soft.hbase.api.utils.ObjectTrans;
import zx.soft.utils.config.ConfigUtil;

public class HBaseTable {

	private HTableInterface table;
	private HConnection conn;
	private String tableName;
	private static Configuration conf;

	static {
		Properties prop = ConfigUtil.getProps("zookeeper.properties");
		//在classpath下查找hbase-site.xml文件，如果不存在，则使用默认的hbase-core.xml文件
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"));
	}

	public HBaseTable(String tableName) throws IOException {
		this.tableName = tableName;
		conn = HConnectionManager.createConnection(conf);
		table = conn.getTable(this.tableName);
	}

	/**
	 * 添加或修改一行的值
	 * @param rowKey
	 * @param family
	 * @param qualifer
	 * @param value
	 * @throws IOException
	 */
	public void put(String rowKey, String family, String qualifer, String value) throws IOException {
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), Bytes.toBytes(value));
		table.put(put);
		table.flushCommits();
		System.out.println("put " + "success");
	}

	/**
	 * 将指定的列和对应的值及时间戳添加到Put实例中
	 * @param rowKey
	 * @param family
	 * @param qualifer
	 * @param ts
	 * @param value
	 * @throws IOException
	 */
	public void put(String rowKey, String family, String qualifer, long ts, String value) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), ts, Bytes.toBytes(value));
		table.put(put);
		table.flushCommits();
	}

	/**
	 * 获得指定rowKey的对象
	 * @param rowKey
	 * @param cls
	 * @return
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public <T> T getObject(String rowKey, Class<T> cls) throws IOException, InstantiationException,
	IllegalAccessException, NoSuchFieldException, SecurityException {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		T t = ObjectTrans.Result2Object(result, cls);
		return t;
	}

	/**
	 * 获取指定rowKey，列族，标识符的值value，一般用于查询对象的某个字段的值
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws IOException
	 */
	public String get(String rowKey, String family, String qualifier) throws IOException {
		Get get = new Get(rowKey.getBytes());
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Result result = table.get(get);
		byte[] res = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return new String(res);
	}

	/**
	 * 删除指定rowKey的行
	 * @param rowKey
	 * @throws IOException
	 */
	public void delete(String rowKey) throws IOException {
		Delete delete = new Delete(rowKey.getBytes());
		table.delete(delete);
	}

	//扫描表
	public <T> List<T> scan(Class<T> cls) throws IOException, InstantiationException, NoSuchFieldException,
	SecurityException, IllegalAccessException {
		Scan scan = new Scan();
		List<T> list = new ArrayList<>();
		ResultScanner results = table.getScanner(scan);
		for (Result result : results) {
			list.add(ObjectTrans.Result2Object(result, cls));
		}
		results.close();
		return list;
	}

	/**
	 * 关闭表和连接
	 */
	public void close() {
		try {
			table.close();
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
