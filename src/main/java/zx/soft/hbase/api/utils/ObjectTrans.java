package zx.soft.hbase.api.utils;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

public class ObjectTrans {

	//将hbase　Get结果Result转换为T类的对象
	public static <T> T Result2Object(Result result, Class<T> cls) throws InstantiationException, NoSuchFieldException,
			SecurityException, IllegalAccessException {

		List<Cell> cells = result.listCells();
		T object = cls.newInstance();
		String qualifier = null;
		String value = null;

		if (cells != null) {
			for (Cell cell : cells) {
				qualifier = new String(CellUtil.cloneQualifier(cell));
				value = new String(CellUtil.cloneValue(cell));
				Field field = object.getClass().getDeclaredField(qualifier);
				field.setAccessible(true);
				if (field.getType() == Integer.class || field.getType() == int.class) {
					field.set(object, Integer.valueOf(value));
				} else if (field.getType() == Long.class || field.getType() == long.class) {
					field.set(object, Long.valueOf(value));
				} else {
					field.set(object, value);
				}
			}
		}
		return object;
	}
}
