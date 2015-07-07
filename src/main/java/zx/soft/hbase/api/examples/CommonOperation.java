package zx.soft.hbase.api.examples;

import java.io.IOException;

import zx.soft.hbase.api.core.HBaseTable;

import com.google.protobuf.ServiceException;

public class CommonOperation {

	//前提是hbase中存在表SinaInfo,并且存有相应字段的记录
	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException,
			NoSuchFieldException, SecurityException, ServiceException {
		String tableName = "SinaInfo";
		String rowKey = "user1";
		HBaseTable hbaseTable = new HBaseTable(tableName);
		UserInfo user1 = hbaseTable.getObject(rowKey, UserInfo.class);
		System.out.println(user1.toString());
	}
}
