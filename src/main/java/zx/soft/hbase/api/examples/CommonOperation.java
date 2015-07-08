package zx.soft.hbase.api.examples;

import java.io.IOException;

import zx.soft.hbase.api.core.HBaseClient;
import zx.soft.hbase.api.core.HBaseTable;

import com.google.protobuf.ServiceException;

public class CommonOperation {

	//前提是hbase中存在表SinaInfo,并且存有相应字段的记录
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException,
	NoSuchFieldException, SecurityException, ServiceException {
		String tableName = "SinaInfo";
		String rowKey = "user1";
		String[] columnFamilys = { "UserInfo" };
		//创建对表操作的对象client
		HBaseClient client = new HBaseClient();
		//创建表
		client.createTable(tableName, columnFamilys);
		HBaseTable hbaseTable = new HBaseTable(tableName);
		//插入数据
		hbaseTable.put("user1", "UserInfo", "id", "444");
		hbaseTable.put("user1", "UserInfo", "nickname", "user1_nickname");
		hbaseTable.put("user1", "UserInfo", "age", "40");
		hbaseTable.put("user1", "UserInfo", "address", "北京");
		//获取指定rowKey的数据
		UserInfo user1 = hbaseTable.getObject(rowKey, UserInfo.class);
		System.out.println(user1.toString());
		//不进行表操作时关闭表
		hbaseTable.close();
		//删除表
		client.deleteTable(tableName);
		client.close();
	}
}
