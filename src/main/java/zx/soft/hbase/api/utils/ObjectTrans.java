package zx.soft.hbase.api.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

public class ObjectTrans {

	public static List<String> Result2StringList(Result result) {
		List<Cell> cells = result.listCells();
		List<String> list = new ArrayList<>();
		String row = null;
		String family = null;
		String qualifier = null;
		String value = null;
		if (cells != null) {
			for (Cell cell : cells) {
				row = new String(CellUtil.cloneRow(cell));
				family = new String(CellUtil.cloneFamily(cell));
				qualifier = new String(CellUtil.cloneQualifier(cell));
				value = new String(CellUtil.cloneValue(cell));
				list.add(row + "    " + family + ":" + qualifier + "    " + cell.getTimestamp() + "    " + value);
			}
		}
		return list;
	}
}
