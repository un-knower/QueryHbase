package com.asiainfo.resource;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;

import com.asiainfo.Constant;
import com.asiainfo.util.HeadMessage;
import com.asiainfo.util.StringUtil;

/**
 * 集团总代付帐单
 */
public class GROUPDFBill extends Record {

	private String area = null;
	
	@Override
	public StringBuffer GetResult(HeadMessage head, HashMap<String, String> map) {
		StringBuffer resbuf = new StringBuffer();
		String mobileNo = map.get("MobileNo");	//集团编码
		String acctID = map.get("AcctID");	//产品编号
		String startCycle = map.get("StartCycle");
		String endCycle = map.get("EndCycle");
		area = map.get("Area");
		int rowcnt = 0;
		
		logger.info("号码：" + mobileNo + "，编号：" + acctID + "，开始时间：" + startCycle + "，结束时间：" + endCycle);
		
		try {
			long startTime = System.currentTimeMillis();
			
			/*1.获取查询时间 */
			SimpleDateFormat sf = new SimpleDateFormat("yyyyMM");
			
			try {
				startCycle = startCycle.substring(0, 6);
				endCycle = endCycle.substring(0, 6);
				sf.parse(startCycle);
				sf.parse(endCycle);
			} catch (ParseException e) {
				logger.error("集团账单明细查询，时间格式有误，startCycle=" + startCycle + "，endCycle=" + endCycle);
				resbuf.setLength(0);
				head.setErrcode("3");
				resbuf.append("&MSG=error");
				return resbuf;
			}
			
			logger.info("实际的开始时间：" + startCycle + "，结束时间：" + endCycle + "，号码：" + acctID);
			
			/*2.获取数据*/
			m_hbaseRow = GetHbase(mobileNo, startCycle, endCycle, acctID);
			long queryTime = System.currentTimeMillis();
	        m_hbaseTime = queryTime - startTime;
	        logger.info("# hadoop查询完毕，号码：" + mobileNo + "，行数：" + m_hbaseRow + "，耗时：" + m_hbaseTime + "ms");
	        
	        /*3.翻译数据*/
			rowcnt = GetTranslate(resbuf, mobileNo);
			m_transTime = System.currentTimeMillis() - queryTime;
            logger.info("# 翻译账单完毕，号码：" + mobileNo + "，行数：" + rowcnt + "，大小：" + resbuf.length() + "，耗时：" + m_transTime + "ms");
			
            resbuf.insert(0, "COUNT=" + rowcnt);
            resbuf.append("&MSG^success");
		} catch (IOException e) {
			logger.error("", e);
            resbuf.setLength(0);
			head.setErrcode("8");
            resbuf.append("&MSG=error");
		} catch (IllegalStateException ie) { // 数据库表信息没配正确
			logger.error("严重错误，数据库表信息没配正确",ie);
			resbuf.setLength(0);
			head.setErrcode("9");
			resbuf.append("&MSG=error");
        } catch (RuntimeException re) { // 普通的运行时异常
            logger.error("", re);
            resbuf.setLength(0);
			head.setErrcode("8");
            resbuf.append("&MSG=error");
        }
		
		return resbuf;
	}

	@Override
	protected int GetHbase(String mob, String startCycle, String endCycle, String acctid) throws IOException {
		String tabname = null;
		m_vecCDR = new Vector<CDRINFO>(500);
		
		if (Constant.DEBUGMODE) {
			StringBuffer buf = new StringBuffer(500*1024); // 预计500条单大小
			GetDebugFile(buf);

			int start=0, end=0;
			while ((end=buf.indexOf("\r\n", start)) != -1){
				m_vecCDR.add(new CDRINFO(buf.substring(start, end+2), CDR_EFFECTIVE, -1));
				start=end+2;
			}
			return m_vecCDR.size();
		}
		
		int begmonth = StringUtil.ParseInt(startCycle);
		int endmonth = StringUtil.ParseInt(endCycle);
		int begyear = StringUtil.ParseInt(startCycle.substring(0, 4));

		//集团编码|产品编码|账单类型
		String startkey = getRowKey(mob, acctid+"|3|");
		String endkey = getRowKey(mob, acctid+"|3|999999");
		
		while (begmonth <= endmonth) {
			tabname = "GROUPBILL_" + begmonth;

			try {
				SubGetCDR(mob, startkey, endkey, tabname, m_vecCDR, "Header", "Body");
			} catch (TableNotEnabledException te){
				logger.warn("table may be disabled" + te);
				continue;
			} catch (TableNotFoundException te){
				logger.warn("table may be delete" + te);
				continue;
			} catch (Exception te){
				logger.error("fatal error", te);
				continue;
			} finally {
				if (begmonth % 100 == 12) {
					begmonth = (++begyear) * 100;
				}
				begmonth++;			
			}
		}
		
		return m_vecCDR.size();
	}
	
	@Override
	protected int GetTranslate(StringBuffer resbuf, String mobileNo) throws IOException {
		int start = 0;
		int end = 0;
		String bill = null;
		int rowcnt = 0;

		StringBuffer buf = new StringBuffer();
		for (CDRINFO oneCDRINFO : m_vecCDR){
			buf.append(oneCDRINFO.cdr);
		}
		
		if(Constant.DEBUGMODE){
			//进行格式转换
			if(buf.substring(0, 5).equals("START")){
				StringBuffer buf_transf = new StringBuffer();
				while ((end = buf.indexOf("\r\n", start)) != -1) {
					bill = buf.substring(start, end);
					start = end + 2;
					if(bill.startsWith("START")){
						buf_transf.append(bill.substring(6));
					} else if(bill.equals("END")) {
						buf_transf.append("\r\n");
					} else {
						buf_transf.append(bill.replaceAll("\\|", "^") + "|");
					}
				}
				buf = buf_transf;
			} else {
				logger.info("#不需要翻译，号码：" + mobileNo);
	            resbuf.append(buf.substring(0, buf.length() - 2));
	            return m_hbaseRow;
			}
		}
		
		start = 0;
		end = 0;
		while ((end = buf.indexOf("\r\n", start)) != -1) {
			bill = buf.substring(start, end);
			start = end + 2;
			String[] value_bill = bill.split("\\|");
			int bodystart = 0;
			
			if(value_bill.length >= 8){
				resbuf.append("&BILLDATA=");
				if("SZ".equals(area)){
					resbuf.append("GROUPNO^"+value_bill[0]+"|GROUPNAME^"+value_bill[1]+"|POSTCODE^"+value_bill[2]
							+"|CONTACTADDR^"+value_bill[3]+"|CONTACT^"+value_bill[4]+"|PRODUCTNO^"+value_bill[5]
							+"|BILLCYC^"+value_bill[6]+"|PRINTDATE^"+value_bill[7]+"|");
					bodystart = 8;
				} else {
					resbuf.append("GROUPNO^"+value_bill[2]+"|GROUPNAME^"+value_bill[3]+"|POSTCODE^"+value_bill[4]
							+"|CONTACTADDR^"+value_bill[5]+"|CONTACT^"+value_bill[6]+"|PRODUCTNO^"+value_bill[0]
							+"|BILLCYC^"+value_bill[8]+"|PRINTDATE^"+value_bill[9]+"|");
					bodystart = 10;
				}
				
				for (int i = bodystart; i < value_bill.length; i++) {
					resbuf.append(value_bill[i]+"|");
					
					/*String[] acctDetail = value_bill[i].split("\\^");
					for (int j = 0; j < acctDetail.length; j++) {
						resbuf.append(acctDetail[j]);
					}
					
					
					if (acctDetail.length == 7) {
						resbuf.append(acctDetail[0]+"^"+acctDetail[1]+"^"+acctDetail[2]+"^"+acctDetail[3]+"^"+acctDetail[4]+"^"+acctDetail[5]+"^"+acctDetail[6]+"|");
					} else if(acctDetail.length == 6) {
						resbuf.append(acctDetail[0]+"^"+acctDetail[1]+"^"+acctDetail[2]+"^"+acctDetail[3]+"^"+acctDetail[4]+"^"+acctDetail[5]+"|");
					}*/
				}
			}
			rowcnt++;
		}
		
		return rowcnt;
	}

}
