package zx.soft.hbase.api.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import zx.soft.hbase.api.examples.UserInfo;

import com.google.protobuf.ServiceException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseTableTest {

	HBaseTable hbaseTable;
	String tableName = "SinaInfo";
	String[] columnFamilys = { "UserInfo" };

	@Test
	public void test1Put() throws IOException, ServiceException {
		HBaseClient hbaseClient = new HBaseClient();
		hbaseClient.createTable(tableName, columnFamilys);
		hbaseTable = new HBaseTable(tableName);
		hbaseTable.put("user1", "UserInfo", "id", 143616867, "111");
		hbaseTable.put("user1", "UserInfo", "nickname", 143616867, "nickname");
		hbaseTable.put("user1", "UserInfo", "age", 143616867, "33");
		hbaseTable.put("user1", "UserInfo", "address", 143616867, "安徽合肥");
		hbaseTable.put("user2", "UserInfo", "id", 143616860, "222");
		hbaseTable.close();
	}

	@Test
	public void test2Get() throws IOException, ServiceException {

		hbaseTable = new HBaseTable(tableName);
		String id = hbaseTable.get("user1", "UserInfo", "id");
		String nickname = hbaseTable.get("user1", "UserInfo", "nickname");
		String age = hbaseTable.get("user1", "UserInfo", "age");
		String address = hbaseTable.get("user1", "UserInfo", "address");
		assertEquals("111", id);
		assertEquals("nickname", nickname);
		assertEquals("33", age);
		assertEquals("安徽合肥", address);
		hbaseTable.close();
	}

	@Test
	public void test2getObject() throws IOException, InstantiationException, IllegalAccessException,
	NoSuchFieldException, SecurityException {
		hbaseTable = new HBaseTable(tableName);
		UserInfo expected = new UserInfo();
		expected.setId("111");
		expected.setNickname("nickname");
		expected.setAge(33);
		expected.setAddress("安徽合肥");
		UserInfo actual = hbaseTable.getObject("user1", UserInfo.class);
		assertEquals(expected, actual);
	}

	@Test
	public void test3DeleteRowKey() throws IOException, InstantiationException, IllegalAccessException,
	NoSuchFieldException, SecurityException {
		hbaseTable = new HBaseTable(tableName);
		hbaseTable.delete("user2");
		assertEquals(new UserInfo(), hbaseTable.getObject("user2", UserInfo.class));
		hbaseTable.close();
	}

	@Test
	public void test4Scan() throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
			ServiceException, InstantiationException, NoSuchFieldException, SecurityException, IllegalAccessException {
		HBaseClient hbaseClient = new HBaseClient();
		hbaseTable = new HBaseTable(tableName);
		assertEquals(1, hbaseTable.scan(UserInfo.class).size());
		hbaseTable.close();
		hbaseClient.deleteTable(tableName);
		hbaseClient.close();
	}
}
