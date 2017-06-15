package com.asiainfo.queryhbase.resource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.queryhbase.Constant;
import com.asiainfo.queryhbase.util.HeadMessage;

public abstract class Record {
	protected static final Logger logger = LoggerFactory.getLogger(Record.class);
	protected long m_transTime = 0;
	protected long m_hbaseTime = 0;
	protected int m_hbaseRow = 0;
	protected final static boolean CDR_EFFECTIVE = true;
	protected final static boolean CDR_NONE_EFFECTIVE = false;
	protected Vector<CDRINFO> m_vecCDR;
	protected final static ExecutorService exec = Executors.newCachedThreadPool();
	protected Filter filter = null;
	
	// 外部调用接口
	public abstract StringBuffer GetResult(HeadMessage head, HashMap<String, String> map);

	// 内部Hbase查询接口，最后的参数billPeriod当是清单查询时表示计费月，当是账单查询时，是账单ID
	protected abstract int GetHbase(String mob, String begintime, String endtime, String billPeriod) throws IOException;

	protected abstract int GetTranslate(StringBuffer _resbuf, String _subno) throws IOException;

	// 统计信息包括：IP|PORT|号码|计费月|开始时间|结束时间|请求时间|清单查询类型|账单ID|清单量|查询时间|翻译时间|发送时间|总时间|错误码
	public long getRowCnt() {
		return m_hbaseRow;
	}

	public long getTransTime() {
		return m_transTime;
	}

	public long getHbaseTime() {
		return m_hbaseTime;
	}

	// 拼rowkey
	protected static String getRowKey(String subno, String time) {
		return subno + "|" + time;
	}

	// 读文件代替Hbase
	protected static int GetDebugFile(StringBuffer buf) throws IOException {
		int rowcnt = 0;
		File file = null;
		if ((Constant.DEBUG_CDR_FILE == null) || (file = new File(Constant.DEBUG_CDR_FILE)) == null) {
			logger.error("调试模式，但模拟清单数据文件不存在");
			return -1;
		}

		BufferedReader reader = new BufferedReader(new FileReader(file));
		String tempString = null;
		while ((tempString = reader.readLine()) != null) {
			buf.append(tempString);
			buf.append("\r\n");
			rowcnt++;
		}

		if (rowcnt > Constant.MAX_CDR_COUNT) {
			logger.error("查询清单数超过限制：" + Constant.MAX_CDR_COUNT);
			buf.setLength(0);
			rowcnt = 0;
		}

		reader.close();
		return rowcnt;
	}

	protected static class CDRINFO{
		String cdr;			// 一条记录
		boolean flag=CDR_NONE_EFFECTIVE;	// 是否展现
		int modid=-1;		// 模板类型
		
		public CDRINFO(String _cdr, boolean _flag, int _modid){
			cdr = _cdr;
			flag = _flag;
			modid = _modid;
		}
	}
	
	protected class Task implements Callable<Integer>{
		private Table table;
		private Scan scan;
		Vector<CDRINFO> m_vecCDR;
		String col1;
		String col2;

		public Task(Table _table, Scan _scan, Vector<CDRINFO> _vecCDR, String _col1, String _col2) {
			table = _table;
			scan = _scan;
			m_vecCDR = _vecCDR;
			col1 = _col1;
			col2 = _col2;
		}

		@Override
		public Integer call() throws Exception {
    		ResultScanner rs = null;
    		int rowcnt = 0;
    		StringBuffer tmp = new StringBuffer();
    		
	        try {
	    		rs = table.getScanner(scan);
				for (Result r : rs) {
					tmp.setLength(0);
					byte[] b_col1 = r.getValue(Bytes.toBytes("Info"), Bytes.toBytes(col1));
					if (b_col1 != null) {
						tmp.append(new String(b_col1, "GBK"));
					}
					
					if (col2 != null && !col2.equals("")) {
						byte[] b_col2 = r.getValue(Bytes.toBytes("Info"), Bytes.toBytes(col2));
						if (b_col2 != null) {
							tmp.append(new String(b_col2, "GBK"));
						}
					}
					
					this.m_vecCDR.add(new CDRINFO(new String(tmp)+"\r\n", CDR_EFFECTIVE, -1));
					rowcnt++;
				}
	        } catch (Exception e) {
	        	logger.warn("scan encount an error.", e);
	        } finally {
	    		rs.close();
	        }
	        return rowcnt;
		}
	}
	
	protected void SubGetCDR(String mob, String startkey, String endkey, String tabname, 
			Vector<CDRINFO> m_vecCDR, String col1, String col2) throws IOException {
		
		Table table = Constant.connection.getTable(TableName.valueOf(tabname));
		Scan scan = new Scan();
		logger.info("* start: [" + startkey + "],end: [" + endkey + "],table: [" + tabname +"]");

		scan.setStartRow(Bytes.toBytes(startkey));
		scan.setStopRow(Bytes.toBytes(endkey));
		if (filter != null) {
			scan.setFilter(filter);
		}
		
		// 超时控制
		Task task = new Task(table, scan, m_vecCDR, col1, col2);
		Future<Integer> future = null;

		try {
			future = exec.submit(task);
			Integer res = future.get(1000 * Constant.HBASE_TIMEOUT, TimeUnit.MILLISECONDS);
			this.m_vecCDR = task.m_vecCDR;
			logger.info("* count=[" + res + "]");

		} catch (TimeoutException ex) {
			logger.error("处理超时", ex);
		} catch (Exception e) {
			logger.warn("处理失败", e);
		} finally {
			if (!future.isDone()) {
				future.cancel(true);				
			}
		}
		
	}
	
}
