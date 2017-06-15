package com.asiainfo.queryhbase.resource;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;

import com.asiainfo.queryhbase.Constant;
import com.asiainfo.queryhbase.util.HeadMessage;
import com.asiainfo.queryhbase.util.StringUtil;

public class LLBill extends Record {

	@Override
	public StringBuffer GetResult(HeadMessage head, HashMap<String, String> map) {
		StringBuffer resbuf = new StringBuffer();
		String mobileNo = map.get("MobileNo");
		String startCycle = map.get("StartCycle");
		String endCycle = map.get("EndCycle");
		String acctID = map.get("AcctID");
		int rowcnt = 0;
		
		logger.info("���룺"+mobileNo+"����ʼʱ�䣺"+startCycle+"������ʱ�䣺"+endCycle+"���˻�ID��"+acctID); 
        
		try {
	        long startTime=System.currentTimeMillis();

	        if (startCycle.length()<6 || endCycle.length()<6) {
    			resbuf.setLength(0);
    			head.setErrcode("3");
    			resbuf.append("&MSG=error");
    			return resbuf;
	        }

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
			try {
		        startCycle = startCycle.substring(0, 6);
		        endCycle = endCycle.substring(0, 6);
				sdf.parse(startCycle);
				sdf.parse(endCycle);
			} catch (ParseException e) {
				logger.error("�˵���ѯ��ʱ���ʽ����startCycle="+startCycle+"��endCycle="+endCycle);
    			resbuf.setLength(0);
    			head.setErrcode("3");
    			resbuf.append("&MSG=error");
    			return resbuf;
			}

            logger.info("ʵ�ʵĿ�ʼʱ�䣺" + startCycle + "������ʱ�䣺" + endCycle + "���˻���" + acctID); 
	        m_hbaseRow = GetHbase(mobileNo, startCycle, endCycle, acctID);
	        long queryTime=System.currentTimeMillis();
	        m_hbaseTime = queryTime-startTime;
	        logger.info("# hadoop��ѯ��ϣ����룺"+mobileNo+"��������"+m_hbaseRow+"����ʱ��"+m_hbaseTime+"ms"); 

			rowcnt = GetTranslate(resbuf, mobileNo);
			m_transTime = System.currentTimeMillis()-queryTime;
            logger.info("# �����˵���ϣ����룺"+mobileNo+"��������"+rowcnt+"����С��"+resbuf.length()+"����ʱ��"+m_transTime+"ms");         	

            resbuf.insert(0, "COUNT=" + rowcnt);
            resbuf.append("&MSG^success");
	        
		} catch (IOException e) {
            logger.error("", e);
            resbuf.setLength(0);
			head.setErrcode("8");
            resbuf.append("&MSG=error");
        } catch (IllegalStateException ie) { // ���ݿ����Ϣû����ȷ
			logger.error("���ش������ݿ����Ϣû����ȷ",ie);
			resbuf.setLength(0);
			head.setErrcode("9");
			resbuf.append("&MSG=error");
        } catch (RuntimeException re) { // ��ͨ������ʱ�쳣
            logger.error("", re);
            resbuf.setLength(0);
			head.setErrcode("8");
            resbuf.append("&MSG=error");
        }
		
		return resbuf;
	}
	
	@Override
	protected int GetHbase(String mob, String startCycle, String endCycle, String acctid) throws IOException {
		String tabname;
		m_vecCDR = new Vector<CDRINFO>(500);
		
		if (Constant.DEBUGMODE) {
			StringBuffer buf = new StringBuffer(500*1024); // Ԥ��500������С
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

		String startkey = getRowKey(mob, acctid);
		String endkey = getRowKey(mob, acctid+"|999999");

		while (begmonth <= endmonth) {
			tabname = "LLBILL_" + begmonth;

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
		
		if (Constant.DEBUGMODE) {
			if (buf.substring(0, 5).equals("START")) { // �������ļ����س����зָ�����Ҫ���и�ʽת��
				StringBuffer buf_transf = new StringBuffer();
				while ((end = buf.indexOf("\r\n", start)) != -1) {
					bill = buf.substring(start, end);
					start=end+2;
					if (bill.startsWith("END")) {
						buf_transf.append("\r\n");
					} else if (bill.startsWith("START")) {
						buf_transf.append(bill.substring(6));
					} else {
						buf_transf.append(bill.replaceAll("\\|", "^")+"|");
					}
				}
				buf = buf_transf;
			} else {
	            logger.info("# ����Ҫ���룬���룺"+mobileNo);  
	            resbuf.append(buf.substring(0, buf.length()-2));
	            return m_hbaseRow;
			}
		}

		start = 0;
		end = 0;
		while ((end = buf.indexOf("\r\n", start)) != -1) {
			bill = buf.substring(start, end);
			start=end+2;
			String[] value_bill = bill.split("\\|");
			
			if (value_bill.length >= 24) { // ����ͷ����Ϊ24��
				resbuf.append("&BILLDATA=");
				resbuf.append("ACCTID^"+value_bill[0]+"|"+"MOBILENO^"+value_bill[2]+"|BILLCYC^"+value_bill[4]+"|BILLSTART^"
						+value_bill[5]+"|BILLEND^"+value_bill[6]+"|BILLTYPE^"+value_bill[19]+"|VERSION^"+value_bill[20]+"|");
				for (int i = 24; i < value_bill.length; i++) {
					String[] acctDetail = value_bill[i].split("\\^");
					if (acctDetail.length == 3) {
						resbuf.append(acctDetail[2]+"^"+acctDetail[1]+"^"+acctDetail[0]+"|");
					}
				}
				rowcnt++;
			}
		}
		
		return rowcnt;
	}

}
