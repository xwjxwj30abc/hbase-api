package zx.soft.hbase.api.endpoint;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import zx.soft.hbase.api.core.HConn;
import zx.soft.hbase.api.endpoint.DailyCountProtos.DailyCountService;

import com.google.protobuf.ServiceException;

public class DailyCountClient {

	public static void main(String[] args) throws ServiceException, Throwable {

		String tableName = "test_proto";
		//		String cf = "count";
		//		String qu = "data";
		//		Map<String, String> kvs = new HashMap();
		//		kvs.put("key", "value");
		//		HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseConfig.getZookeeperConf());
		//		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
		//		tableDesc.addFamily(new HColumnDescriptor(cf));
		//		Path path = new Path("hdfs://bigdata4:8020/user/hdfs/hbase-api-1.0.0.jar");
		//		tableDesc.addCoprocessor("zx.soft.hbase.api.endpoint.DailyCountEndpoint", path, Integer.MAX_VALUE / 2, kvs);
		//		hbaseAdmin.createTable(tableDesc);

		HConnection conn = HConn.getHConnection();
		HTableInterface table = conn.getTable(tableName);
		table.setAutoFlushTo(true);
		//
		//		for (int i = 0; i < 5; i++) {
		//			byte[] iBytes = String.valueOf(i).getBytes();
		//			Put p = new Put(iBytes);
		//			p.add(cf.getBytes(), iBytes, iBytes);
		//			table.put(p);
		//		}

		final DailyCountProtos.CountRequest request = DailyCountProtos.CountRequest.getDefaultInstance();
		Map<byte[], Long> result = table.coprocessorService(DailyCountProtos.DailyCountService.class, null, null,
				new Batch.Call<DailyCountProtos.DailyCountService, Long>() {

					@Override
			public Long call(DailyCountService counter) throws IOException {
				ServerRpcController controller = new ServerRpcController();
				BlockingRpcCallback<DailyCountProtos.CountResponse> rpcCallback = new BlockingRpcCallback<DailyCountProtos.CountResponse>();
				counter.getDailyCount(controller, request, rpcCallback);
				DailyCountProtos.CountResponse response = rpcCallback.get();
				if (controller.failedOnException()) {
					throw controller.getFailedOn();
				}
				return (response != null && response.hasCount()) ? response.getCount() : 0;

					}
		});

		Map<byte[], Long> results = table.coprocessorService(DailyCountProtos.DailyCountService.class, null, null,
				new Batch.Call<DailyCountProtos.DailyCountService, Long>() {

					@Override
			public Long call(DailyCountService counter) throws IOException {
				ServerRpcController controller = new ServerRpcController();
				BlockingRpcCallback<DailyCountProtos.CountResponse> rpcCallback = new BlockingRpcCallback<DailyCountProtos.CountResponse>();
				counter.getKeyValueCount(controller, request, rpcCallback);
				DailyCountProtos.CountResponse response = rpcCallback.get();
				if (controller.failedOnException()) {
					throw controller.getFailedOn();
				}
				return (response != null && response.hasCount()) ? response.getCount() : 0;

					}
		});
		System.out.println(result.values().iterator().next().longValue());
		System.out.println(results.values().iterator().next().longValue());
		table.close();
	}
}
