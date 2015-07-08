package zx.soft.hbase.api.core;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import zx.soft.hbase.api.utils.ObjectTrans;

public class HBaseTable {

	private HTableInterface table;
	private HConnection conn;

	public HBaseTable(String tableName) throws IOException {
		conn = HConnectionManager.createConnection(HBaseConfig.getZookeeperConf());
		table = conn.getTable(tableName);
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
	 * 插入行到数据表中
	 * @param rowKey
	 * @param family
	 * @param t　　插入的对象
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public <T> void putObject(String rowKey, String family, T t) throws InstantiationException, IllegalAccessException,
	IOException {
		Field[] fields = t.getClass().getDeclaredFields();
		Put put = new Put(Bytes.toBytes(rowKey));
		for (Field field : fields) {
			field.setAccessible(true);
			put.add(Bytes.toBytes(family), Bytes.toBytes(field.getName()), field.get(t).toString().getBytes());
		}
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

	/**
	 * 扫描表
	 * @param cls
	 * @return
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 * @throws IllegalAccessException
	 */
	public <T> List<T> scan(Class<T> cls) throws IOException, InstantiationException, NoSuchFieldException,
	SecurityException, IllegalAccessException {
		List<T> list = new ArrayList<>();
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		for (Result result : results) {
			list.add(ObjectTrans.Result2Object(result, cls));
		}
		results.close();
		return list;
	}

	/**
	 * 返回行键比较的结果
	 * @param rowKey
	 * @param compareOp -2代表小于，-1代表小于等于，０代表等于，１代表大于等于，２代表大于
	 * @param cls
	 * @return
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 * @throws IllegalAccessException
	 */
	public <T> List<T> scan(String rowKey, int compareOp, Class<T> cls) throws IOException, InstantiationException,
	NoSuchFieldException, SecurityException, IllegalAccessException {
		List<T> list = new ArrayList<>();
		Filter filter = null;
		if (compareOp == -2) {
			filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(rowKey)));
		} else if (compareOp == -1) {
			filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
		} else if (compareOp == 0) {
			filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
		} else if (compareOp == 1) {
			filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(rowKey)));
		} else if (compareOp == 2) {
			filter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(rowKey)));
		}
		Scan scan = new Scan();
		scan.setFilter(filter);
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
