package com.asiainfo.queryhbase.resource;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;

import com.asiainfo.queryhbase.Constant;
import com.asiainfo.queryhbase.util.HeadMessage;
import com.asiainfo.queryhbase.util.MysqlJdbcProcess;
import com.asiainfo.queryhbase.util.StringUtil;

public class FETIONBill extends Record {

	@Override
	public StringBuffer GetResult(HeadMessage head, HashMap<String, String> map) {
		StringBuffer resbuf = new StringBuffer();
		String subno = map.get("SubNo");
		String beginDate = map.get("BeginDate");
		String endDate = map.get("EndDate");
		int m_type = StringUtil.ParseInt(map.get("CDRType"));
		int rowcnt=0;
        logger.info("号码："+subno+"，开始时间："+beginDate+"，结束时间："+endDate); 
        
        try {
        	// 查询
	        long startTime=System.currentTimeMillis();
	        if((beginDate==null || StringUtil.ParseLong(beginDate)==0) || (endDate==null || StringUtil.ParseLong(endDate)==0) ) {
    			resbuf.setLength(0);
    			head.setErrcode("3");
    			resbuf.append("&MSG=error");
    			return resbuf;
	        } else {
	        	// 起末时间不为空，则为实时查询
				Calendar calendar = Calendar.getInstance();
    			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss"); 
    			Date date = null;
    			
    			if (beginDate.length()>14) {
    				beginDate = beginDate.substring(0, 14);
    			}
    			
    			try {
					date = sdf.parse(beginDate); // 仅校验
				} catch (ParseException e) {
					logger.error("按实时查询，格式有误，BeginDate="+beginDate);
					StringBuffer tmpBeginDate = new StringBuffer("20160101000000");
					tmpBeginDate.replace(0, beginDate.length(), beginDate);
					beginDate = tmpBeginDate.toString();
					logger.error("修正后，BeginDate="+beginDate);
				}

    			if (endDate.length()>14) {
    				endDate = endDate.substring(0, 14);
    			}
    			try {
					date = sdf.parse(endDate);
	    			calendar.setTime(date);
	    			calendar.add(Calendar.SECOND, 1);  // 前闭后开
	    			endDate = sdf.format(calendar.getTime());
				} catch (ParseException e) {
					logger.error("按实时查询，格式有误，EndDate="+endDate);
					StringBuffer tmpEndDate = new StringBuffer("20160101240000");
					tmpEndDate.replace(0, endDate.length(), endDate);
					endDate = tmpEndDate.toString();
					logger.error("修正后，EndDate="+endDate);
				}
	        }
	        
	        m_hbaseRow = GetHbase(subno, beginDate, endDate, null);
	        long queryTime=System.currentTimeMillis();
	        m_hbaseTime = queryTime-startTime;
	        logger.info("# hadoop查询，号码："+subno+"，行数："+m_hbaseRow+"，耗时："+m_hbaseTime+"ms"); 

	        // 翻译
	        if (m_type == 8) {

	    		for (CDRINFO oneCDRINFO : m_vecCDR){
	    			resbuf.append(oneCDRINFO.cdr);
	    		}

	        	resbuf.append("&MSG=Successful");
	        	logger.info("# 不翻译清单，号码："+subno);
	        } else {
	    		rowcnt = GetTranslate(resbuf, subno);
	    		m_transTime = System.currentTimeMillis()-queryTime;
	            logger.info("# 翻译清单，号码："+subno+"，行数："+rowcnt+"，大小："+resbuf.length()+"，耗时："+m_transTime+"ms");         	
	        }
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
	protected int GetHbase(String mob, String begintime, String endtime, String acctid) throws IOException {
		String tabname;
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
		
		int begmonth = StringUtil.ParseInt(begintime.substring(0, 6));
		int endmonth = StringUtil.ParseInt(endtime.substring(0, 6));
		int begyear = StringUtil.ParseInt(begintime.substring(0, 4));
		String startkey = getRowKey(mob, begintime);
		String endkey = getRowKey(mob, endtime);
		
		while (begmonth <= endmonth) {
			tabname = "NMS_" + begmonth;
			
			try {
				SubGetCDR(mob, startkey, endkey, tabname, m_vecCDR, "cdr", "");
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
	protected int GetTranslate(StringBuffer _resbuf, String _subno) throws IOException {
		StringBuffer outString = new StringBuffer();
		String inString = null;
		String cdrValue = null;
		String cdrtype = "61"; 
		List<String> tmpValue = null;
		String[] subvalue_INF_MOD_COLUMN = null;
		String[] value_INF_COLUMN = null;
		String[] value_INF_MOD = null;
		String value_INF_HENMS_TRAN = null;
		Integer mod_id = null;
		Integer seqid = null;
		Integer columnid = null;
		int querytype = 54; 
		int errcdr = 0;
		int j = 0;
		
		if ((Constant.INF_QUERY_MAP.get(querytype) != null) && (Constant.INF_QUERY_MAP.get(querytype).get(querytype+MysqlJdbcProcess.SPLITCHAR+cdrtype) != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(querytype).get(querytype+MysqlJdbcProcess.SPLITCHAR+cdrtype);
		} else {
			logger.error("cbep0_inf_query 没有配置请求信息，int_querytype="+querytype+",int_cbe_type="+cdrtype);
			throw new IllegalStateException();
		}

		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger.error("cbep0_inf_mod 没有配置列头信息，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}
		
		_resbuf.setLength(0);
		_resbuf.append("Count=");
		_resbuf.append(value_INF_MOD[1]);
		
		for (CDRINFO oneCDRINFO : m_vecCDR){
			outString.setLength(0);
			outString.append("&CDR=");
			
			// 长度只有一种，直接按照长度截取
			inString = oneCDRINFO.cdr;
			if (inString.length() != Constant.CDR_LEN_FETION){
				logger.error("清单长度"+(inString.length())+"和格式长度不相等"+Constant.CDR_LEN_FETION);
				errcdr++;
				continue;
			}
			
			if (Constant.INF_MOD_COLUMN_MAP.get(mod_id) != null){
				tmpValue = Constant.INF_MOD_COLUMN_MAP.get(mod_id);
			} else {
				logger.error("cbep0_inf_mod_column 没有配置模板列信息，int_mod_id="+mod_id);
				throw new IllegalStateException();
			}
			
			j = 0;
			for (String s : tmpValue){
				subvalue_INF_MOD_COLUMN = s.split(MysqlJdbcProcess.SPLITCHAR);
				
				if (j != StringUtil.ParseInt(subvalue_INF_MOD_COLUMN[0])) {
					logger.error("cbep0_inf_mod_column 序列号配置不连续，int_mod_id="+mod_id+",int_seq_id="+j);
					throw new IllegalStateException();
				}
				
				seqid = StringUtil.ParseInt(subvalue_INF_MOD_COLUMN[0]);
				columnid = StringUtil.ParseInt(subvalue_INF_MOD_COLUMN[1]);
				
				// 取列字段对应的清单值
				if ((Constant.INF_COLUMN_MAP.get(columnid) != null) && (Constant.INF_COLUMN_MAP.get(columnid).get(columnid+MysqlJdbcProcess.SPLITCHAR+cdrtype) != null)) {
					value_INF_COLUMN = Constant.INF_COLUMN_MAP.get(columnid).get(columnid+MysqlJdbcProcess.SPLITCHAR+cdrtype).split(MysqlJdbcProcess.SPLITCHAR);
				} else {
					logger.error("cbep0_inf_column 没有配置此列信息，int_column_id="+columnid+",int_cbe_type="+cdrtype);
					throw new IllegalMonitorStateException();
				}
				cdrValue = inString.substring(StringUtil.ParseInt(value_INF_COLUMN[1]), StringUtil.ParseInt(value_INF_COLUMN[1])+StringUtil.ParseInt(value_INF_COLUMN[2]));
				
				// 清单字符串型左对齐，右补空格；数值型右对齐，左补零
				// 取清单值对应的翻译值，默认去掉前面的0，否则协商填清单用空格来填充
				value_INF_HENMS_TRAN = null;
				if (Constant.INF_HENMS_TRAN_MAP.get(seqid) != null){
					try {
						value_INF_HENMS_TRAN = Constant.INF_HENMS_TRAN_MAP.get(seqid).get(seqid+MysqlJdbcProcess.SPLITCHAR+StringUtil.ParseInt(cdrValue.trim()));
					} catch (NumberFormatException e){
						logger.error("清单开始为["+inString.substring(0, 20)+"]的["+seqid+"]字段["+cdrValue+"]在HENMS_Tran没有Col_val匹配值,将不翻译");
						value_INF_HENMS_TRAN = null;
					}
				}

				if (value_INF_HENMS_TRAN == null) {
					value_INF_HENMS_TRAN = cdrValue.trim();
					if (seqid == 11) { // 此字段为会话时长，数值型，左补零，需要去掉
						value_INF_HENMS_TRAN = cdrValue.trim().replaceAll("^(0+)", "");
						if ("".equals(value_INF_HENMS_TRAN)){
							value_INF_HENMS_TRAN=0+"";
						}
					}
				}

				outString.append(value_INF_HENMS_TRAN+value_INF_MOD[0]);
				
				j++;
			}
			
			outString.deleteCharAt(outString.length()-1);
			_resbuf.append(outString);
		}
		_resbuf.insert("Count=".length(), m_hbaseRow-errcdr+"");
		_resbuf.append("&MSG=Successful");
		logger.info("返回总行数为："+m_hbaseRow+",过滤错误条数为："+errcdr);
		return m_hbaseRow-errcdr;
	}

}
