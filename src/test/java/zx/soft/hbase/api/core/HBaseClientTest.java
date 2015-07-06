package zx.soft.hbase.api.core;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.protobuf.ServiceException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HBaseClientTest {

	HBaseClient hbaseClient;
	String tableName = "SinaInfo";

	@Before
	public void getConf() throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		hbaseClient = new HBaseClient();
	}

	@Test
	public void test1CreateTable() throws IOException {
		String[] columnFamily = { "UserInfo" };
		assertTrue(hbaseClient.createTable(tableName, columnFamily));
	}

	@Test
	public void test2IsTableExists() throws IOException {
		assertTrue(hbaseClient.isTableExists(tableName));
	}

	@Test
	public void test3DeleteTable() throws IOException {
		assertTrue(hbaseClient.deleteTable(tableName));
	}

	@After
	public void close() throws IOException {
		hbaseClient.close();
	}
}
