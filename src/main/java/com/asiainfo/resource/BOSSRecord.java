package com.asiainfo.resource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.Constant;
import com.asiainfo.Constant.Sysp1InfMsi;
import com.asiainfo.Constant.Sysp1InfPbs;
import com.asiainfo.method.ColumnMethod;
import com.asiainfo.resource.BOSSRecord.Pair;
import com.asiainfo.util.HeadMessage;
import com.asiainfo.util.MysqlJdbcProcess;
import com.asiainfo.util.StringUtil;

public class BOSSRecord extends Record {
	public static final Logger logger_trans = LoggerFactory.getLogger("translate");
	
	@Override
	public StringBuffer GetResult(HeadMessage head, HashMap<String, String> map) {
		StringBuffer resbuf = new StringBuffer(50*1024);
		String subno = map.get("SubNo");
		String beginDate = map.get("BeginDate");
		String endDate = map.get("EndDate");
		String billPeriod = map.get("BillPeriod");
		int rowcnt = 0;
		m_type = StringUtil.ParseInt(map.get("CDRType"));
		
        logger.info("号码："+subno+"，开始时间："+beginDate+"，结束时间："+endDate+"，帐期："+billPeriod+"，查询类型："+m_type);
        
		try {
        	// 查询
	        long startTime=System.currentTimeMillis();
			Calendar calendar = Calendar.getInstance();
			
			//历史月累计流量
			if(m_type == 16) {
				if((beginDate != null && StringUtil.ParseLong(beginDate) > 0) && (endDate != null && StringUtil.ParseLong(endDate) > 0)) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
					
					if(beginDate.length() > 6) {
						beginDate = beginDate.substring(0, 6);
					}
					if(endDate.length() > 6) {
						endDate = endDate.substring(0, 6);
					}
					
					try {
						sdf.parse(beginDate);//校验格式
						sdf.parse(endDate);//校验格式
					} catch (ParseException e) {
						logger.error("按历史月累计流量查询，起末时间格式不符合协议");
						resbuf.setLength(0);
						head.setErrcode("3");
						resbuf.append("&MSG=error");
						return resbuf;
					}
					
					rowcnt = LLHistoryMonthQuery(resbuf, subno, beginDate, endDate);
					logger.info("# 翻译清单完毕，号码："+subno+"，行数："+rowcnt+"，大小："+resbuf.length()+"，耗时："+m_transTime+"ms");
					return resbuf;
				} else {//始末时间不能为空，为空则退出查询
					logger.error("按历史月累计流量查询，起末时间不能为空");
					resbuf.setLength(0);
					head.setErrcode("3");
					resbuf.append("&MSG=error");
					return resbuf;
				}
				
			} else if (m_type == 15) {// 历史月流量查询
	    		billPeriod = "0";
	    		
	    		// 缺省是查过去6个月的数据
	    		if ((beginDate==null || StringUtil.ParseLong(beginDate)==0) 
		        		&& (endDate==null || StringUtil.ParseLong(endDate)==0) ) {

	    			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd"); 
	    			
	    			calendar.add(Calendar.MONTH, -6);
	    			calendar.set(Calendar.DATE, 1);
	    			beginDate = sdf.format(calendar.getTime())+"000000";
	    			
	    			calendar.add(Calendar.MONTH, 6);
	    			calendar.set(Calendar.DATE, 1);
	    			calendar.add(Calendar.DATE, -1);
	    			endDate = sdf.format(calendar.getTime())+"240000"; // 前闭后开
	    			
	    		}
	    	} else if((beginDate==null || StringUtil.ParseLong(beginDate)==0) && (endDate==null || StringUtil.ParseLong(endDate)==0) ) {
		    	/*
	    		 * 起末时间为空，则为计费月查询，用月份填充此两值，假如月份也为空，则输入格式错误
	    		 * 计费月查询的月表可能存在跨月的清单，需要查询出来，所以时间格式控制要放大，例如：
	    		 * 输入为201604，则需要查询BOSS_201604表的[subno|201, subno|202)
	    		 */
	    		
	        	
	        	if (billPeriod!=null && StringUtil.ParseLong(billPeriod)!=0) {
	    			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
	    			
	    			try {
						sdf.parse(billPeriod);
					} catch (ParseException e) {
						logger.error("按计费月查询，账期格式有误，BillPeriod="+billPeriod);
		    			resbuf.setLength(0);
		    			head.setErrcode("3");
		    			resbuf.append("&MSG=error");
		    			return resbuf;
					}
	    			
	    			beginDate = "201";
	    			endDate = "202"; // 前闭后开
	        	} else {
					logger.error("按计费月查询，账期格式有误，BillPeriod="+billPeriod);
	    			resbuf.setLength(0);
	    			head.setErrcode("3");
	    			resbuf.append("&MSG=error");
	    			return resbuf;
	        	}
	        } else {
	        	// 起末时间不为空，则为实时查询
    			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss"); 
    			Date date = null;
    			billPeriod = "0"; // 必须为0，查询的时候回判断是否为实时查询
    			
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

			if (m_type == 55) {
				realMsisdn = map.get("RealMsisdn"); // 真实号码real_msisdn [849,24],左对齐,右补空格
				if (realMsisdn == null || realMsisdn.length() == 0) {
					logger.error("集团语音行业骚扰查询必须指定主叫号码RealMsisdn");
	    			resbuf.setLength(0);
	    			head.setErrcode("3");
	    			resbuf.append("&MSG=error");
	    			return resbuf;
				} else if (realMsisdn.startsWith("+86")) {
					realMsisdn = realMsisdn.substring(3);
				}
				
				String B_subno = map.get("MsisdnB"); // 规整后对方号码B_subno[165,24],左对齐,右补空格
				if (B_subno == null || B_subno.length() == 0) {
					// realMsisdn需要确认前缀问题,B_subno无前缀,注意字段无换行符
					filter = new ValueFilter(
							CompareFilter.CompareOp.EQUAL, 
							new RegexStringComparator("11.{846}.*"+realMsisdn+" *.{"+(Constant.CDR_LEN_GROVOICE-2-872)+"}")); 
				} else {
					filter = new ValueFilter(
							CompareFilter.CompareOp.EQUAL, 
							new RegexStringComparator("11.{162}"+B_subno+" {"+(24-B_subno.length())+"}"+
									".{660}.*"+realMsisdn+" *.{"+(Constant.CDR_LEN_GROVOICE-2-872)+"}")); 
				}
			}

            logger.info("实际的开始时间：" + beginDate + "，结束时间：" + endDate + "，计费月：" + billPeriod);
            m_hbaseRow = GetHbase(subno, beginDate, endDate, billPeriod);
	        long queryTime=System.currentTimeMillis();
	        m_hbaseTime = queryTime-startTime;
	        logger.info("# hadoop查询完毕，号码："+subno+"，行数："+m_hbaseRow+"，耗时："+m_hbaseTime+"ms");

	        // 翻译
	        if (m_type == 8) {
	        	
	    		for (CDRINFO oneCDRINFO : m_vecCDR) {
	    			resbuf.append(oneCDRINFO.cdr);
	    		}

	        	resbuf.append("&MSG=Successful");
	        	logger.info("# 不翻译清单，号码："+subno);
	        } else {
				rowcnt = GetTranslate(resbuf, subno);
				m_transTime = System.currentTimeMillis()-queryTime;
	            logger.info("# 翻译清单完毕，号码："+subno+"，行数："+rowcnt+"，大小："+resbuf.length()+"，耗时："+m_transTime+"ms");         	
	        }
	        
		} catch (IOException e) {
            logger.error("", e);
			resbuf.setLength(0);
			head.setErrcode("8");
			resbuf.append("&MSG=error");
        } catch (IllegalStateException ie) { // 数据库表信息没配正确，需要退出
			logger.error("严重错误，数据库表信息没配正确",ie);
			resbuf.setLength(0);
			head.setErrcode("9");
			resbuf.append("&MSG=error");
        } catch (ConcurrentModificationException e) {
            logger.error("清单集合可能有改动，m_vecCDR.size="+m_vecCDR.size(), e);
			resbuf.setLength(0);
			head.setErrcode("8");
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
	protected int GetHbase(String mob, String begintime, String endtime, String billPeriod) throws IOException  {
		String tabname;
		int begmonth = 0;
		int endmonth = 0;
		int begyear = 0;
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

		if (!billPeriod.equals("0")) { // 计费月查询
			begmonth = StringUtil.ParseInt(billPeriod);
			endmonth = StringUtil.ParseInt(billPeriod);
			begyear = StringUtil.ParseInt(billPeriod.substring(0, 4));
		} else {
			begmonth = StringUtil.ParseInt(begintime.substring(0, 6));
			endmonth = StringUtil.ParseInt(endtime.substring(0, 6));
			begyear = StringUtil.ParseInt(begintime.substring(0, 4));
		}
		
		String startkey = getRowKey(mob.trim(), begintime);
		String endkey = getRowKey(mob.trim(), endtime);
		
		while (begmonth <= endmonth) {
			tabname = "BOSS_" + begmonth;
			
			try {
				// 预防集团清单超过200w条，会导致溢出，所以不把所有清单拼串后返回，而是直接入vector
				SubGetCDR(mob, startkey, endkey, tabname, m_vecCDR, "Cdr", "");
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
		m_resbuf = _resbuf;
		m_subno = _subno;
		
		// 清单拆分补全
		SplitCDR();
		
		// 清单过滤
		FilterCDR();

		//  补换卡接口
		if (m_type == 21 || m_type == 18) {
			return RepairQuery();
		}
		
		// 历史日累计流量接口
		if (m_type == 17) {
			return LLHistoryDayQuery();
		}

		// 历史月语音累计流量接口
		if (m_type == 19) {
			return LLHistoryMonthSoundQuery();
		}
		
		// 获取模板
		GetModCDR();
		
		// 清单排序
		SortCDR();
		
		// 合并冲销单
		CombileReversalCDR();

		//  万能副卡接口
		if (m_type == 13) {
			return ViceNumberQuery();
		}
		
		// 合并流量单
		CombileGprsCDR();

		// 流量统计接口
		if (m_type == 15) {
			return LLSumQuery();
		}
		
		return TranslateCDR(); // 翻译后的条数
	}

	private void SplitCDR() {
		String oneCDR = null;
		int errcnt=0;
		int addcnt=0;
		
		logger_trans.info("*********** translate begin ***********");

		// 此m_vecCDR在遍历过程中，可能会新增或减少元素
		// 新增单判断的条件导致不会造成循环新增单的情况程序
		for (int i = 0; i < m_vecCDR.size(); i++){
			
			oneCDR = m_vecCDR.elementAt(i).cdr;
			if (!CheckLen(oneCDR)){
				errcnt++;
				m_vecCDR.remove(i--);
				continue;
			}
			
			try {
			if (oneCDR.substring(164, 166).equals("GC") && StringUtil.ParseLong(oneCDR.substring(216, 224)) != 0) {
				// 03单且gprs日租费不为0，新增06固定费单
				StringBuffer addCDR = new StringBuffer();
				addCDR.setLength(Constant.CDR_LEN_FIX); // 空格填充
				
				// 头
				addCDR.replace(0, 19, oneCDR.substring(0, 19));
				// 新增的清单改为06固定费清单
				addCDR.setCharAt(1, '6');
				// 号码
				addCDR.replace(19, 43, oneCDR.substring(21, 45));
				// 记录生成时间
				addCDR.replace(43, 57, oneCDR.substring(75, 89));
				// 计费方归属局
				addCDR.replace(57, 59, oneCDR.substring(125, 127));
				// 计费方品牌	
				addCDR.setCharAt(59, oneCDR.charAt(129));
				// Bill_type
				addCDR.replace(60, 62, oneCDR.substring(164, 166));
				// 费用
				addCDR.replace(76+4, 88, oneCDR.substring(216, 224));
				// 详单分类
				addCDR.setCharAt(130, '3');
				// 详单显示时间
				addCDR.replace(131, 145, oneCDR.substring(75, 89));
				// 详单显示开始日期
				addCDR.replace(145, 153, oneCDR.substring(75, 83));
				// 详单显示结束日期
				addCDR.replace(153, 161, oneCDR.substring(75, 83));
				// 产品编码
				addCDR.replace(161, 174, "gprsdailylist");
				// end
				addCDR.replace(298, 300, "\r\n");
				
				// 清空原清单的长途费
				StringBuffer newOneCDR = new StringBuffer(oneCDR).replace(216, 224, "       0");
				m_vecCDR.elementAt(i).cdr = newOneCDR.toString();
				
				if (addCDR.length() != Constant.CDR_LEN_FIX) {
					logger_trans.error("新增清单长度变化，请检查代码");
				}
				m_vecCDR.add(new CDRINFO(addCDR.toString(), CDR_EFFECTIVE, -1));
				addcnt++;
				
			} else if (oneCDR.startsWith("01") && oneCDR.substring(772, 774).equals("03")
				&& (oneCDR.substring(702, 703).equals("5") || oneCDR.substring(702, 703).equals("6"))) {
				// 呼转单可能修改了oneCDR ？？？
				
				// 01单且呼转且主叫，新增呼转单
				StringBuffer addCDR = new StringBuffer(oneCDR);
				
				char direct_type = oneCDR.charAt(702);
				if (direct_type=='5') addCDR.setCharAt(702, '7');
				if (direct_type=='6') addCDR.setCharAt(702, '8');

				// 话单对方号码取自第三方号码
				addCDR.replace(776, 800, oneCDR.substring(800, 824));
				if (oneCDR.charAt(705)>'3' && oneCDR.charAt(706)>'9') {  // 国内呼转单
					addCDR.replace(208, 216, "       0");
					addCDR.replace(216, 224, "       0");
					addCDR.replace(224, 232, "       0");
				} else { // 国际呼转单
					// 主叫单只保留长途费  
					addCDR.replace(208, 216, "       0");
					addCDR.replace(224, 232, "       0");
					// 被叫单只保留移动费
					addCDR.replace(216, 224, "       0");
					addCDR.replace(224, 232, "       0");
					
					GetGlobalCallForwardCDR(addCDR);
				}
				
				if (addCDR.length() != Constant.CDR_LEN_VOICE) {
					logger_trans.error("新增清单长度变化，请检查代码");
				}
				m_vecCDR.add(new CDRINFO(addCDR.toString(), CDR_EFFECTIVE, -1));
				addcnt++;
			}
			}catch (Exception e) {
				logger_trans.error(oneCDR.substring(0, 20)+"：新增单出错，忽略此单" ,e);
			}
			
		}
		logger_trans.info("* SplitCDR: 号码："+ m_subno + "，错误过滤条数：" + errcnt + "，新增条数：" + addcnt + "，总条数：" + m_vecCDR.size());
		logger.info("* SplitCDR: 号码："+ m_subno + "，错误过滤条数：" + errcnt + "，新增条数：" + addcnt + "，总条数：" + m_vecCDR.size());
		
	}

	private boolean CheckLen(String _str){
		if (_str.length()!=Constant.CDR_LEN_VOICE && _str.length()!=Constant.CDR_LEN_SMS
				&& _str.length()!=Constant.CDR_LEN_GPRS && _str.length()!=Constant.CDR_LEN_MOBILESELF
				&& _str.length()!=Constant.CDR_LEN_SP && _str.length()!=Constant.CDR_LEN_FIX
				&& _str.length()!=Constant.CDR_LEN_OTHER && _str.length()!=Constant.CDR_LEN_GROVOICE
				&& _str.length()!=Constant.CDR_LEN_GROSMS && _str.length()!=Constant.CDR_LEN_GROGPRS
				&& _str.length()!=Constant.CDR_LEN_GROTHER ) {
			logger_trans.error("清单长度"+_str.length()+"不符合格式");
			return false;
		}
		
		return true;
	} 
	
	// 国际呼转单
	private void GetGlobalCallForwardCDR(StringBuffer _addCDR) {
		TOLLINFO tollInfo = new TOLLINFO();
		
		// 计费月取开话单的处理时间proc_time，开始时间取start_time
		tollInfo.msisdnC = _addCDR.substring(800, 824).trim();
		tollInfo.start_time = _addCDR.substring(75, 89);
		tollInfo.bill_period = _addCDR.substring(91, 97);
		tollInfo.roam_type = _addCDR.substring(705, 706);
		tollInfo.visit_switch_flag = _addCDR.substring(703, 705);
		tollInfo.b_switch_flag = _addCDR.substring(711, 713);

		if (tollInfo.msisdnC.startsWith("00") && !tollInfo.msisdnC.startsWith("0086")){
			
			tollInfo.b_switch_flag = "ZZ"; 	// 设置默认的国际归属局
			tollInfo.b_operator = "13"; 	// 国际运营商
			tollInfo.b_user_type = "14"; 	// 国际用户
			tollInfo.b_brand = "0"; 		// 品牌
			
		} else if((tollInfo.msisdnC.startsWith("86") && (tollInfo.msisdnC.length()<=12 || tollInfo.msisdnC.charAt(12) != ' ')) 
				|| (!tollInfo.msisdnC.startsWith("86") && (tollInfo.msisdnC.length()<=10 || tollInfo.msisdnC.charAt(10) != ' '))){ 
			// 当为86+手机号码或者手机号码 时，查询sysp1_inf_msi/sysp1_inf_pbs，而当前使用的数据库，已废弃这两个表，故不迁移

			String msisdnC = tollInfo.msisdnC;
			if(tollInfo.msisdnC.startsWith("86")) {
				msisdnC = tollInfo.msisdnC.substring(2);
			}
			
			Sysp1InfMsi msi = null;
			Sysp1InfPbs pbs = null;
			if((msi=GetMsi(msisdnC, tollInfo.bill_period, tollInfo.start_time)) != null){
				tollInfo.b_switch_flag = msi.vc_switch_flag;
				tollInfo.b_operator = msi.vc_operator;
				tollInfo.b_brand = msi.vc_brand;
				tollInfo.b_user_type = msi.vc_user_type;
				tollInfo.b_area = msi.vc_area;
			}else if((pbs=GetPbs(msisdnC, tollInfo.bill_period, tollInfo.start_time)) != null){
				tollInfo.b_switch_flag = pbs.vc_switch_flag;
				tollInfo.b_operator = pbs.vc_operator;
				tollInfo.b_brand = pbs.vc_brand;
				tollInfo.b_user_type = pbs.vc_user_type;
				tollInfo.b_area = pbs.vc_area;
			}else{
				tollInfo.b_switch_flag = "42";
				tollInfo.b_operator = "03";
				tollInfo.b_user_type = "00";
				tollInfo.b_brand = "0";
			}
			
		} else if(tollInfo.msisdnC.startsWith("0")){
			// 当为0开头时，查询 ratp0_Inf_nat
			// 先设置为未明号明归属地的电信号码，后面查到具体的信息再更新
			tollInfo.b_switch_flag = "42";
			tollInfo.b_operator = "03";
			tollInfo.b_user_type = "00";
			tollInfo.b_brand = "0";

			String area = tollInfo.msisdnC.substring(0, 4); // 可能越界
			if (Constant.INF_NAT_MAP.get(area) != null) {
				tollInfo.b_area = area;
				if (tollInfo.b_area.startsWith("0765"))  tollInfo.b_area = "0757";
			}else{
				logger_trans.warn("ratp0_Inf_nat 国内长途区号表无此信息，area="+area);
			}
		} else {
			tollInfo.b_switch_flag = "42";
			tollInfo.b_operator = "03";
			tollInfo.b_user_type = "00";
			tollInfo.b_brand = "0";
		}
		GetToll(tollInfo);
	 	
		if (tollInfo.b_switch_flag != null) _addCDR.replace(711, 713, tollInfo.b_switch_flag);
		if (tollInfo.b_operator != null) 	_addCDR.replace(706, 708, tollInfo.b_operator);
		if (tollInfo.b_user_type != null) 	_addCDR.replace(714, 716, tollInfo.b_user_type);
		if (tollInfo.b_brand != null) 		_addCDR.replace(713, 714, tollInfo.b_brand);
		if (tollInfo.toll_type != null) 	_addCDR.replace(706, 707, tollInfo.toll_type);
		if (tollInfo.roam_type != null) 	_addCDR.replace(705, 706, tollInfo.roam_type);
		
	}

	private Sysp1InfMsi GetMsi(String msisdnC, String bill_period, String start_time) {
		int size = Constant.INF_MSI_VEC.size();
		if (size == 0) {
			return null;
		}
		
		Sysp1InfMsi msi = new Sysp1InfMsi();
		msi.vc_msisdn_low=msisdnC;
		msi.vc_start_period=bill_period;
		msi.dt_start_time=start_time;
		
		int index = Collections.binarySearch(Constant.INF_MSI_VEC, msi);
		
		if(index == Constant.INF_MSI_VEC.size()) {
			--index;
		}
		
		while(index != size){
			Sysp1InfMsi tmp = Constant.INF_MSI_VEC.elementAt(index);
			if(msisdnC.equals(tmp.vc_msisdn_low.substring(0, Constant.MIN_SAME_MSI_LEN))){
				if (msisdnC.compareTo(tmp.vc_msisdn_low) >= 0 && msisdnC.compareTo(tmp.vc_msisdn_high) <= 0 
					&& bill_period.compareTo(tmp.vc_start_period) >= 0 && bill_period.compareTo(tmp.vc_stop_period) <= 0 
					&& start_time.compareTo(tmp.dt_start_time) >= 0 && start_time.compareTo(tmp.dt_stop_time) <= 0) {
					
					return msi;
				} else {
					++index;
				}
			} else {
				 break;
			}
		}
		return null;
	}
	
	private Sysp1InfPbs GetPbs(String msisdnC, String bill_period, String start_time) {
		int size = Constant.INF_PBS_VEC.size();
		if (size == 0) {
			return null;
		}
		
		Sysp1InfPbs pbs = new Sysp1InfPbs();
		pbs.vc_subno=msisdnC;
		pbs.vc_start_period=bill_period;
		pbs.dt_start_time=start_time;
		
		int index = Collections.binarySearch(Constant.INF_PBS_VEC, pbs);
		
		if(index == Constant.INF_PBS_VEC.size()) {
			--index;
		}
		
		while(index != size){
			Sysp1InfPbs tmp = Constant.INF_PBS_VEC.elementAt(index);
			if(msisdnC.equals(tmp.vc_subno)){
				if (bill_period.compareTo(tmp.vc_start_period) >= 0 && bill_period.compareTo(tmp.vc_stop_period) <= 0 
					&& start_time.compareTo(tmp.dt_start_time) >= 0 && start_time.compareTo(tmp.dt_stop_time) <= 0) {
					
					return pbs;
				} else {
					++index;
				}
			} else {
				 break;
			}
		}
		return null;
	}

	private void GetToll(TOLLINFO _tollInfo) {
		
		//以下设置对方号码归属标志cb_switch_flag
		if (_tollInfo.b_switch_flag.compareTo("99") > 0) {
			//对方是国际用户
			if (_tollInfo.b_switch_flag.equals("HA")) {
				//香港号码
				_tollInfo.toll_type = "A";
			} else if (_tollInfo.b_switch_flag.equals("IA")) {
				//澳门号码
				_tollInfo.toll_type = "B";
			} else if (_tollInfo.b_switch_flag.equals("JA")) {
				//台湾号码
				_tollInfo.toll_type = "C";
			} else {
				//国际号码
				_tollInfo.toll_type = "D";
			}			
		} else {
			if(_tollInfo.roam_type.equals("1")){
				if (_tollInfo.b_operator.equals("01")) {
					//网内
					//2本地同区网内电话
					_tollInfo.toll_type = "2";
				} else {
					//本地网外
					//3本地同区网外电话
					_tollInfo.toll_type = "3";
				}
			}else if(_tollInfo.roam_type.equals("2")){
				if (_tollInfo.b_operator.equals("01")) {
					//网内
					//2本地同区网内电话
					if(_tollInfo.b_area.equals(_tollInfo.visit_switch_flag)){ 
						// strncmp(list.visit_switch_flag, list.b_area, 8) == 0 长度且内容相等才满足，使用equals可行，不需要substring
						_tollInfo.toll_type = "2";
					}else{
						//6省内网内长途
						_tollInfo.toll_type = "6";
					}
				} else {
					//本地网外
					if(_tollInfo.b_area.equals(_tollInfo.visit_switch_flag)){
						//3本地同区网外电话
						_tollInfo.toll_type = "3";
					}else{
						//7省内网外长途
						_tollInfo.toll_type = "7";
					}
				}
			}else if(_tollInfo.roam_type.equals("3")){
				 if (_tollInfo.b_operator.equals("01")) {
					 //网内
					 if(_tollInfo.b_area.equals(_tollInfo.visit_switch_flag)){
						 //2本地同区网内电话
							_tollInfo.toll_type = "2";
					 }else{
						  //3国内网内长途
							_tollInfo.toll_type = "8";
					 }
				 } else {
					 //本地网外
					 if(_tollInfo.b_area.equals(_tollInfo.visit_switch_flag)){
						 //8本地同区网外电话
							_tollInfo.toll_type = "3";
					 }else{
						 //9国内网外长途
							_tollInfo.toll_type = "9";
					 }
				 }
			}
		}
	}


	private void FilterCDR() throws IOException {
		int filtercnt = 0;
		for (CDRINFO oneCDRINFO : m_vecCDR) {
			
			// BUG_信安优化_BR201501200007 过滤上网清单临时解决方案, 保留输出模版.仅过滤03,05类型清单数据;
			if(m_type == 11){
				if(oneCDRINFO.cdr.startsWith("03") || oneCDRINFO.cdr.startsWith("05")){
					oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
					filtercnt++;
				}
			}
			
			if (oneCDRINFO.cdr.substring(130,132).equals("04") || "KC/LC/MC/NC/ZC".contains(oneCDRINFO.cdr.substring(164,166))) {
				oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
				filtercnt++;
			}

			// 上下行流量为0
			if (oneCDRINFO.cdr.startsWith("03") && oneCDRINFO.cdr.substring(164,166).equals("HC")) {
				if (StringUtil.ParseLong(oneCDRINFO.cdr.substring(961,972)) + StringUtil.ParseLong(oneCDRINFO.cdr.substring(972,983)) == 0) {
					oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
					filtercnt++;
				}
			}


			//集团语音查询过滤
			if (m_type == 51 ) {
				if (oneCDRINFO.cdr.startsWith("12") || oneCDRINFO.cdr.startsWith("13") || oneCDRINFO.cdr.startsWith("15")) {
					oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
					filtercnt++;
				}
			}

			//集团短彩查询过滤
			if (m_type == 52) {
				if (oneCDRINFO.cdr.startsWith("11") || oneCDRINFO.cdr.startsWith("13") || oneCDRINFO.cdr.startsWith("15")){
					oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
	                filtercnt++;
	            }	
			}
			
			// BR201610190011，只保留03单
			if (m_type == 16 || m_type == 17) {
				if (!oneCDRINFO.cdr.startsWith("03")){
					oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
	                filtercnt++;
	            }	
			}
			
			// 过滤
			if (m_type == 55) {
				// realMsisdn传入无空格，经过正则匹配，prefix应该不会出现负长度
				String realMs = oneCDRINFO.cdr.substring(848, 872).trim();
				int prefixLen = realMs.length() - realMsisdn.length();
				if (prefixLen < 0) {
					oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
	                filtercnt++;
	            } else {
					String prefix = realMs.substring(0, prefixLen);
					// Constant.PrefixSet如为空则要精确匹配
					if (prefix.length()>0 && !Constant.PrefixSet.contains(prefix)){
						oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
		                filtercnt++;
		            }
	            }
			}
		}
		logger_trans.info("* FilterCDR: 号码："+ m_subno + "，错误过滤条数：" + filtercnt);
		logger.info("* FilterCDR: 号码："+ m_subno + "，错误过滤条数：" + filtercnt);
	}

	private int LLSumQuery() {
		int cnt = 0;
		String key;
		
		String yearmon;		// 话单开始年月
		String nettype; 	// 上网类型，2G/3G/4G/WLAN
		int usetype = 0;	// 流量0、时长1计费
		long total = 0;		// 总流量或时长
		long inside = 0;	// 套餐内流量或时长，最终展现的[套餐外=总流量-套餐内],[套餐内=套餐内-结转]
		long lastturn = 0; 	// 结转流量
		String up_volume;
		String down_volume;
		String lastmon;
		String duration;
		String dis_dura;

		ll_map = new TreeMap<String, LLSum>();
		for (CDRINFO cdrInfo : m_vecCDR) {
			if (cdrInfo.flag == CDR_EFFECTIVE) {
				usetype = 0;
				total = 0L;	
				inside = 0L;
				lastturn = 0L; 
				// 分清单类型取值
				if (cdrInfo.cdr.startsWith("03") && cdrInfo.cdr.charAt(649)=='3') {
					yearmon = cdrInfo.cdr.substring(75, 81);
					
					if (cdrInfo.cdr.charAt(991) == '3') {
						nettype = "3G";
					} else if (cdrInfo.cdr.charAt(991) == '4') {
						nettype = "4G";
					} else {
						nettype = "2G";
					}
					
					if (cdrInfo.cdr.charAt(769) == '0') { // 时长计费，取值和05的不同
						duration = cdrInfo.cdr.substring(934, 941);	
						total = StringUtil.ParseLong(duration);
						usetype = 1;
						lastturn = 0;
						
						dis_dura = cdrInfo.cdr.substring(707, 719); // 03是12位，05是7位
						inside = StringUtil.ParseLong(dis_dura)*60; // 按照时长计费，此字段单位特殊，为分钟
					
					} else { // 流量计费 yx是1 华为可能是2
						up_volume = cdrInfo.cdr.substring(961, 972); // 03是11位，05是13位
						down_volume = cdrInfo.cdr.substring(972, 983);
						total = StringUtil.ParseLong(up_volume)/1024 + StringUtil.ParseLong(down_volume)/1024;
						usetype = 0;
						
						lastmon = cdrInfo.cdr.substring(1024, 1031);
						lastturn = StringUtil.ParseLong(lastmon); // 只有按照03单的流量计费,此字段单位特殊，为kb
						
						dis_dura = cdrInfo.cdr.substring(707, 719);
						inside = StringUtil.ParseLong(dis_dura)/1024;
					}
					
				} else if (cdrInfo.cdr.startsWith("05") && cdrInfo.cdr.charAt(649)=='3') {
					yearmon = cdrInfo.cdr.substring(75, 81);
					nettype = "WLAN";
					
					if (cdrInfo.cdr.charAt(761) == '1') {
						duration = cdrInfo.cdr.substring(789, 796);	
						total = StringUtil.ParseLong(duration);
						usetype = 1;
						
						dis_dura = cdrInfo.cdr.substring(707, 714);
						inside = StringUtil.ParseLong(dis_dura)*60;
					} else { // 流量计费 yx是2
						up_volume = cdrInfo.cdr.substring(796, 809);
						down_volume = cdrInfo.cdr.substring(809, 822);
						total = StringUtil.ParseLong(up_volume)/1024 + StringUtil.ParseLong(down_volume)/1024;
						usetype = 0;
						
						dis_dura = cdrInfo.cdr.substring(707, 714);
						inside = StringUtil.ParseLong(dis_dura)/1024;
					}
					
					lastturn = 0;
					
				} else {
					continue;
				}
				
				// yearmon#nettype#usetype 如 "2016013G1"
				key = yearmon+nettype+usetype;
				LLSum value = ll_map.get(key);
				if (value != null) {
					value.total += total;
					value.inside += inside;
					value.lastturn += lastturn;
				} else {
					value = new LLSum();
					value.yearmon = yearmon;
					value.nettype = nettype;
					value.lastturn = lastturn;
					value.usetype = usetype;
					value.total = total;
					value.inside = inside;
					ll_map.put(key, value);
				}
			}
		}
		
		// &Row=6&TYPE=月份,上网方式,本月已用总流量,套餐外使用量,套餐内使用,是否结转
		// 随便给一个cdr_type即可，例如语音1
		int mod_id = 0;
		String[] value_INF_MOD = null;
		if ((Constant.INF_QUERY_MAP.get(m_type) != null) && (Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1") != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1");
		} else {
			logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type=1");
			throw new IllegalStateException();
		}

		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger_trans.error("cbep0_inf_mod 没有配置列头信息1，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}

		m_resbuf.setLength(0);
		m_resbuf.append("Count=");
		m_resbuf.append(value_INF_MOD[1]);
		
		Iterator<Entry<String, LLSum>> iter = ll_map.entrySet().iterator();
		while (iter.hasNext()) {
			LLSum trav_LL = iter.next().getValue();
			if (trav_LL.total != 0) { // kb值不为0
				m_resbuf.append("&CDR=");
				m_resbuf.append(trav_LL.yearmon + ",");
				m_resbuf.append(trav_LL.nettype + ",");
				
				if (trav_LL.usetype == 0) { // 按流量计费
					
					if (trav_LL.lastturn != 0){ // 有结转流量，分成两条展示
						m_resbuf.append(String.format("%.2f", (trav_LL.total-trav_LL.lastturn)*1.0/1024) + "MB,");
						m_resbuf.append(String.format("%.2f", (trav_LL.total-trav_LL.inside)*1.0/1024) + "MB,");
						m_resbuf.append(String.format("%.2f", (trav_LL.inside-trav_LL.lastturn)*1.0/1024) + "MB,0");

						m_resbuf.append(String.format("&CDR=%s,%s,%.2fMB,0.00MB,%.2fMB,1", 
								trav_LL.yearmon, trav_LL.nettype, trav_LL.lastturn*1.0/1024, trav_LL.lastturn*1.0/1024));
						
					} else {
						m_resbuf.append(String.format("%.2f", trav_LL.total*1.0/1024) + "MB,");
						m_resbuf.append(String.format("%.2f", (trav_LL.total-trav_LL.inside)*1.0/1024) + "MB,");
						m_resbuf.append(String.format("%.2f", trav_LL.inside*1.0/1024) + "MB,0");
					}
					
				} else { // 按时长计费
					m_resbuf.append(String.format("%.2f", trav_LL.total*1.0/3600) + "小时,");
					m_resbuf.append(String.format("%.2f", (trav_LL.total-trav_LL.inside)*1.0/3600) + "小时,");
					m_resbuf.append(String.format("%.2f", trav_LL.inside*1.0/3600) + "小时,0");
				}
				
				cnt++;
			}
		}
		m_resbuf.append("&MSG=Successful");
		m_resbuf.insert(6, cnt);
		
		logger_trans.info("************ translate end ************");
		return m_resbuf.length();
	}

	private int RepairQuery() {
		String B_subno;
		String msisdnB;
		
		rep_set = new HashSet<String>();
		for (CDRINFO cdrInfo : m_vecCDR) {
			if (cdrInfo.flag == CDR_EFFECTIVE) {
				if (cdrInfo.cdr.startsWith("01")) {			
					// 过滤非主叫的清单
					if (cdrInfo.cdr.charAt(702) != '3'){
						continue;
					}
					
					// 过滤30s以下的号码
					if (StringUtil.ParseInt(cdrInfo.cdr.substring(763,770)) < 30){
						continue;
					}

					B_subno = cdrInfo.cdr.substring(140, 164);
					rep_set.add(formatNo(B_subno));
					
					msisdnB = cdrInfo.cdr.substring(776, 800);
					rep_set.add(formatNo(msisdnB));
				} 
			}
			
		}

		// 随便给一个cdr_type即可，例如语音1
		int mod_id = 0;
		String[] value_INF_MOD = null;
		if ((Constant.INF_QUERY_MAP.get(m_type) != null) && (Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1") != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1");
		} else {
			logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type=1");
			throw new IllegalStateException();
		}

		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger_trans.error("cbep0_inf_mod 没有配置列头信息2，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}

		m_resbuf.setLength(0);
		
		if (m_type == 18) {
			m_resbuf.append("Count=1");
			m_resbuf.append(value_INF_MOD[1]);
			m_resbuf.append("&CDR="+rep_set.size());
		} else {
			m_resbuf.append("Count=1");
			m_resbuf.append(value_INF_MOD[1]);
			m_resbuf.append("&CDR=");
			for (String ss : rep_set) {
				m_resbuf.append(ss + "|");
			}
			if(rep_set.size() != 0) {
				m_resbuf.deleteCharAt(m_resbuf.length() - 1);
			}
		}
		
		m_resbuf.append("&MSG=Successful");
		
		logger_trans.info("************ translate end ************");		
		return m_resbuf.length();
	}
	
	private String formatNo(String mob) {
		String dest;
	
		dest = mob;
	
		if (dest.startsWith("1795100") || dest.startsWith("1259300")) {
			dest = dest.substring(5);
		}
	
		if (dest.charAt(0) == '+') {
			dest = dest.substring(1);
		}
	
		if (dest.startsWith("0086")) {
			dest = dest.substring(4);
			/*
			if (isMobileNo(dest) && dest[0] != '0'
					&& strncmp(dest, "1349", 4) != 0) {
				snprintf(temp, sizeof(temp), "0%s", dest);
				snprintf(dest, sizeof(dest), "%s", temp);
			}
			*/
		}
		
		if (dest.startsWith("86")) {
			dest = dest.substring(2);
		}

		return dest.trim();
	}

	private void GetModCDR() throws IOException {
		int filtercnt = 0;
		int cdrtype=-1;
		Integer modid = null;

		for (CDRINFO oneCDRINFO : m_vecCDR) {

			if (oneCDRINFO.flag == CDR_NONE_EFFECTIVE) {
				continue;
			}
			
			try {
				cdrtype = GetMod1(oneCDRINFO.cdr);
				cdrtype = GetMod2(oneCDRINFO.cdr, cdrtype);
			} catch (Exception e) {
				logger_trans.error(oneCDRINFO.cdr.substring(0, 20)+":获取模板错误", e);
				oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
				filtercnt++;
				continue;
			}

			if (Constant.INF_QUERY_MAP.get(m_type) != null) {
				modid = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+cdrtype);
			} else {
				logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type="+cdrtype);
				throw new IllegalStateException();
			}

			if (modid == null) {
            	oneCDRINFO.flag = CDR_NONE_EFFECTIVE;
            	filtercnt++;
			} else {
            	oneCDRINFO.modid = modid.intValue();
			}
		}
		
		logger_trans.info("* GetModCDR: 号码："+ m_subno + "，错误过滤条数：" + filtercnt);
		logger.info("* GetModCDR: 号码："+ m_subno + "，错误过滤条数：" + filtercnt);
	}

	private int GetMod1(String cdr) {
		KeyInfo info = new KeyInfo();
		GetKeyInfo(cdr, info);
		
		int cdrtype = GetSpecilMod(cdr, info);
		if (cdrtype != 0) 
			return cdrtype;

		switch (info.billtype.charAt(1)) {
			case 'B': 
				return GetBbillType(info, cdr); 
			case 'C': 
				return GetCbillType(info, cdr); 
			case 'A': 
				return GetAbillType(info, cdr); 
			case 'D': 
				return GetDbillType(info, cdr); 
			default:  
				return 0; 
		}
	}
	
	private void GetKeyInfo(String cdr, KeyInfo info) {
		info.negw_file_type = cdr.substring(0, 2);
		info.subno = cdr.substring(21, 45);
		
		if (info.negw_file_type.equals("06") || info.negw_file_type.equals("07") 
				|| info.negw_file_type.equals("11") || info.negw_file_type.equals("12")){
			return;
		}

		info.billtype = cdr.substring(164, 166);
		info.bustype = cdr.substring(665, 667);
		info.subbustype = cdr.substring(667, 669);
		info.buscode = cdr.substring(650, 655);
		info.icpcode = cdr.substring(655, 663);
		info.filetype = cdr.substring(19, 21);
		info.switchflag = cdr.substring(125, 127);
		info.ratetype = cdr.substring(669, 670);
		
		// cbe 只有01(voice) 04(special) 有长途类型，其他默认为0 
		if (info.negw_file_type.equals("01") || info.negw_file_type.equals("04")) {
			info.tolltype = cdr.substring(706, 707);			
		} else {
			info.tolltype = "0";
		}

		info.mobfee = StringUtil.ParseLong(cdr.substring(208, 216));
		info.tollfee = StringUtil.ParseLong(cdr.substring(216, 224));
	}

	private int GetSpecilMod(String cdr, KeyInfo info) {
		if (info.negw_file_type.equals("06"))
			return FindBillAcct(cdr);

		if (info.negw_file_type.equals("07"))
			return OTHER;

		if (info.negw_file_type.equals("11"))
			return GROUP_VOICE;

		if (info.negw_file_type.equals("12")){
			if (cdr.substring(19, 21).equals("5D") && info.subno.equals(cdr.substring(810, 834))){
				return GROUP_DATA_DF;
			} else {
				return GROUP_DATA;
			}
		}

		return 0;
	}

	private int FindBillAcct(String cdr) {
	    String acctid = cdr.substring(62, 76).trim();
	    
	    String item_flag = Constant.CBP_BILLACCTNEW_MAP.get(acctid);
	    if (item_flag == null) {
			logger_trans.warn("Not found in cbep0_cbp_billacctnew, acctid="+acctid);
	    	return FIX;
	    } else if (item_flag.equals("1")){
            return MOBILESELF; // billtype为空，不会进入到ABCD逻辑，也不会有字符串比较时的空异常
	    }
	    
		return FIX;
	}

	private int GetAbillType(KeyInfo info, String cdr) {
		/* 
		 * AA：梦网语音新业务账单，
		 * 包括 音信互动、QQ蜜语、WAD新业务、
		 * 自动售货、语音天气台、移动大赢家、
		 * 语音门户等业务
		 */
		if (info.billtype.charAt(0) == 'A')
			return NOT_SURE;

		/* 
		 * BA：12593业务账单，包括12593优惠新干线业务、
		 * 12593语音甄别业务（只有享受12593优惠的，
		 * 才归入此帐单，SCP＋OCS的不归入此帐单）
		 */
		if (info.billtype.charAt(0) == 'B' && info.tollfee != 0) {
			if (((info.filetype.charAt(0) == '1') || info.filetype.equals("40")) && "2/3/6/7/A/B".contains(info.ratetype)) {
				return NOT_SURE;
			}
			if (info.filetype.equals("0T") && "0/1".contains(info.ratetype)) {
				return NOT_SURE;
			}
			if (info.ratetype.equals("5")) {
				return NOT_SURE;
			}
		}
		return 0;
	}

	private int GetBbillType(KeyInfo info, String cdr) {
		/* B类帐单除了特殊的：EB/AB/XB外
		 * 全部归类到通话
		 */
		
		// EB：本地网外账单
		if (info.billtype.charAt(0) == 'E') {
			// IP专线呼叫
			if (info.bustype.equals("0B") && "A5/A6".contains(info.subbustype)) {
				return SP;
			}
			// IP专线UMG模式
			if (info.bustype.equals("0M") && "AB/AC".contains(info.subbustype)) {
				return SP;
			}
		}
		
		// AB单
		if (info.billtype.charAt(0) == 'A') {
			// 移动费归属
			if (info.mobfee != 0) {
				if ("00015/00022".contains(info.buscode)) {
					return OTHER;
				}
				return FIX;
			}
			// 长途费归属
			if (info.tollfee != 0) {
				if (info.tolltype.compareTo("A") >= 0 && info.tolltype.compareTo("D") <= 0) {
					return FIX;
				}
				if (info.buscode.equals("00029")) {
					return NOT_SURE;
				}
				if (info.bustype.equals("50") && "A5/A6/A9/AH".contains(info.subbustype)) {
					return NOT_SURE;
				}
			}
		}

		// EB单，移动800业务账单
		if (info.billtype.charAt(0) == 'X' && info.icpcode.equals("00000001")) {
			return OTHER;
		}

		return 0;
	}

	private int GetCbillType(KeyInfo info, String cdr) {
		/*C类帐单，按照清单量依次判断 */

		/* GPRS 通信费账单:AC/GC */
		if ((info.billtype.charAt(0) == 'G') || (info.billtype.charAt(0) == 'A')) {
			if (info.buscode.trim().equals("SZ")) {
				return VOICE;
			}
			return GPRS_WLAN;
		}
		
		/*
		 * 普通短彩:UC/SC
		 * SC：普通短信账单，含网内、网外点对点短信，多媒体点对点短信和非结算ICP通信费
		 * UC：梦网ICP通信费账单，含短信、多媒体短信的通信费
		 */
		if ((info.billtype.charAt(0) == 'U') || (info.billtype.charAt(0) == 'S')) {
			return SMS;
		}
		
		/*
		 * WC: WLAN
		 * HC: 国际漫游GPRS
		 */
		if ((info.billtype.charAt(0) == 'W') || (info.billtype.charAt(0) == 'H')) {
			return GPRS_WLAN;
		}
		
		/* BC:自有业务梦网信息费 ??? */
		if (info.billtype.charAt(0) == 'B') {
			if (info.icpcode.equals("99901508")) {
				return VOICE;
			}
			return SP;
		}
		
		/* 
		 * DC : 含WAP172和非WAP172业务、百宝箱业务、
		 * 		PDA业务、PIME话簿管家业务、超级邮箱业务账单
		 */
		if (info.billtype.charAt(0) == 'D') {
			if (info.buscode.equals("69119"))
				return MOBILESELF;
			if (info.icpcode.equals("00900145"))
				return MOBILESELF;
			return SP;
		}
		
		/*
		 * EC：原DC中分出的移动自有业务账单
		 * TC：梦网退费账单
		 */
		if ((info.billtype.charAt(0) == 'E') || (info.billtype.charAt(0) == 'T'))
			return NOT_SURE;

		/* XC: 亲亲家园 */
		if (info.billtype.charAt(0) == 'X')
			return MOBILESELF;

		/* VC: ICP端口账单，含短信批发、GPS业务的端口通信费*/
		if (info.billtype.charAt(0) == 'V') {
			if (info.buscode.equals("00011") && info.icpcode.equals("25000012")) {
				return FIX;
			}
			if ("ADCSM/ADCMM".contains(info.buscode)) {
				return NOT_SURE;
			}
			return SMS;
		}

		/* JC: ICP赠送话费帐单*/
		if (info.billtype.charAt(0) == 'J') {
			return SP;
		}
		
		/* CC:
		 *
		 * 梦网信息费账单，含IOD、ISMG、MISC平台过来的月租信息费、
		 * 感地带非套餐月租等。仅作为计费系统中间转换使用，不下发出帐。
		 * 对于神州行用户以及动感地带用户则分别生成scc*、hcc*账单，
		 * 此账单需要下发。彩铃月租费、信息费也放在cc账单里面
		 */
		if (info.billtype.charAt(0) == 'C') {
			if (info.buscode.equals("00001") && info.icpcode.equals("00001156")) {
				return SP;
			}
			
			if (info.switchflag.equals("01") && "50000261/00000649".contains(info.icpcode)) {
				return SP;
			}
			
			if (info.buscode.equals("00001") && 
				("00000584/00001831".contains(info.icpcode) 
					|| (info.icpcode.compareTo("01") >= 0 && info.icpcode.substring(0, 2).compareTo("23") < 0 && info.subno.startsWith("00")))) {
				return FIX;
			}
			
			if (info.switchflag.equals("01") && info.icpcode.equals("cnc88888")) {
				return FIX;
			}
			
			if ("99901808/99801174".contains(info.icpcode)) {
				return MOBILESELF;
			}
			
			return NOT_SURE;
		}

		/* OCE固定费账单*/
		if (info.billtype.charAt(0) == 'O') {
			return FIX;
		}

		/* ??? 默认值，文档中未涉及*/
		return 0;
	}

	private int GetDbillType(KeyInfo info, String cdr) {
		/* 
		 * FD 固网拨打17951账单（flb） 
		 */
		if (info.billtype.charAt(0) == 'F')
			return VOICE;
		return 0;
	}
	
	private int GetMod2(String cdr, int cdrtype) {
		int ret=0;
		String negw_file_type = cdr.substring(0, 2);
		if (cdrtype == NOT_SURE) {
			if (negw_file_type.equals("02") || 
				(cdr.startsWith("04") && cdr.substring(164,166).equals("AA") && cdr.substring(665, 667).equals("06"))) {

				ret = FindBusTranslate(cdr);
				if (ret == 0) {
					return GetMod3(cdr);
				} else {
					return ret;
				}
			} else {
				return GetMod3(cdr);
			}
		} else if (cdrtype == SMS || cdrtype == MOBILESELF || cdrtype == SP) {
			if (negw_file_type.equals("02")) {
				ret = FindBusTranslate(cdr);
				if (ret < 0) {
					return ret;
				}
			}
			return cdrtype;
		} else if (cdrtype == 0) {
			return GetMod3(cdr);
		} else {
			return cdrtype;
		}
	}

	private int FindBusTranslate(String cdr) {
		String sp_code = ""; 	// 20
		String bus_code = "";	// 30
		String cbe_type = "";	// 2
		String bus_type = "";	// 2
		String imsi = ""; 		// 8
		String imsi_code = "";	// 15
		int ret_val = 0;
		int tmp_ret_val = 0;
		String spcodeFromTable = null;

		cbe_type = cdr.substring(0, 2);
		
		if ("-      ".equals(cdr.substring(816, 823)))
		{
			sp_code = cdr.substring(823, 836).trim();
		} else {
			sp_code = cdr.substring(816, 836).trim();
		}
		
		if (cdr.startsWith("04") && cdr.substring(164,166).equals("AA") && cdr.substring(665, 667).equals("06")) { // 特殊语音
			bus_code = cdr.substring(650, 665).trim();
			sp_code = "-100";
		} else {
			bus_code = cdr.substring(836, 866).trim();
		}

		m_strSpInfos = new TmpSpInfos();
		
		ArrayList<String> value_Bus_Translate = Constant.BUS_TRANSLATE_MAP.get(bus_code);
		String[] subvalue_Bus_Translate = null;
		
		if (value_Bus_Translate == null) { // 没有数据
			if (cdr.startsWith("04") && cdr.substring(164,166).equals("AA") && cdr.substring(665, 667).equals("06")) {
				m_strSpInfos.bus_name = "语音杂志";
				ret_val = SP;
			} else {
				bus_type = cdr.substring(665, 667);
				imsi_code = cdr.substring(650, 665);
				if (imsi_code.charAt(0) != ' ') {
					imsi = cdr.substring(655, 663).trim();
				} else {
					imsi = cdr.substring(51, 59).trim();
				}

				if (cbe_type.equals("02") && "52/60".contains(bus_type)) {
					tmp_ret_val = FindSpTranslate(imsi);
				}
				
				if (tmp_ret_val == 1 && !m_strSpInfos.bus_name.equals("")) {
					AddRefund(cdr);
					FindGameInfo(cdr);
				} else {
					if (m_strSpInfos.bus_name.equals("")) {
						m_strSpInfos.bus_name = bus_code;
						logger_trans.warn("Not found in cbep0_bus_translate, bus_code = " + bus_code);
					}
				}
			}			
		} else { // 有数据
			for (String s : value_Bus_Translate) {
				subvalue_Bus_Translate = s.split(MysqlJdbcProcess.SPLITCHAR);
				spcodeFromTable = subvalue_Bus_Translate[0].trim();

				if (spcodeFromTable.equals(sp_code)) {
					m_strSpInfos.bus_name = subvalue_Bus_Translate[1].trim();
					m_strSpInfos.sp_name = subvalue_Bus_Translate[2];
					m_strSpInfos.use_type = subvalue_Bus_Translate[3].trim();
					m_strSpInfos.fee_type = subvalue_Bus_Translate[4].trim();
					m_strSpInfos.flag = subvalue_Bus_Translate[5];

					if (!m_strSpInfos.bus_name.equals("")) {
						AddRefund(cdr);
						FindGameInfo(cdr);
					}
					
					int flag = StringUtil.ParseInt(m_strSpInfos.flag);
					if (flag == 0) {
						ret_val = MOBILESELF;
						break;
					} else {
						ret_val = SP;
						break;
					}
				} else {
					continue;
				}
			}
			
			if (ret_val == 0) { // 没有spcode匹配的记录
				if (cdr.startsWith("04") && cdr.substring(164,166).equals("AA") && cdr.substring(665, 667).equals("06")) {
					m_strSpInfos.bus_name = "语音杂志";
					ret_val = SP;
				} else if (m_strSpInfos.bus_name.equals("")) {
					bus_type = cdr.substring(665, 667);
					imsi_code = cdr.substring(650, 665);
					if (imsi_code.charAt(0) != ' ') {
						imsi = cdr.substring(655, 663).trim();
					} else {
						imsi = cdr.substring(51, 59).trim();
					}
					
					if (cbe_type.equals("02") && "52/60".contains(bus_type)) {
						tmp_ret_val = FindSpTranslate(imsi);
					}

					if (tmp_ret_val == 1 && !m_strSpInfos.bus_name.equals("")) {
						AddRefund(cdr);
						FindGameInfo(cdr);
					} else {
						if (m_strSpInfos.bus_name.equals("")) {
							m_strSpInfos.bus_name = bus_code;
							logger_trans.warn("Not found in cbep0_bus_translate, bus_code = " + bus_code);
						}
					}
				}
			}
		} // end if
		
		String billtype;	// 2
		String filetype;	// 2
		String content_code;// 30
		String content_name = "";// 64
		String provider_name = "";// 64
		String start_time;	// 8
		String[] subvalue_IB_MM_CONTENT_INFO = null;
		String[] subvalue_IB_12582_CONTENTINFO = null;
		
		cbe_type = cdr.substring(0, 2);
		billtype = cdr.substring(164, 166);
		filetype = cdr.substring(19, 21);
		content_code = cdr.substring(903, 933).trim();
		start_time = cdr.substring(75, 83);
		
		if (cbe_type.equals("02") && filetype.equals("4I") && billtype.equals("DC") && content_code.length() <= 12) {

			String value_IB_MM_CONTENT_INFO = Constant.IB_MM_CONTENT_INFO_MAP.get(content_code);
			if (value_IB_MM_CONTENT_INFO == null) {
				logger_trans.warn("Not find in datp0_ib_mm_content_info, vc_content_id = " + content_code);
			} else {
				subvalue_IB_MM_CONTENT_INFO = value_IB_MM_CONTENT_INFO.split(MysqlJdbcProcess.SPLITCHAR);
				if (start_time.compareTo(subvalue_IB_MM_CONTENT_INFO[3]) < 0 
						|| (!subvalue_IB_MM_CONTENT_INFO[4].equals("") && start_time.compareTo(subvalue_IB_MM_CONTENT_INFO[4]) > 0)) {
					logger_trans.warn("Find vc_content_id in datp0_ib_mm_content_info cdr_time " + start_time + "not between valid and expire!\n");
				} else {
					content_name = subvalue_IB_MM_CONTENT_INFO[0];
					provider_name = subvalue_IB_MM_CONTENT_INFO[2];
				}
			}
			
		} else if (cbe_type.equals("02") && filetype.equals("4Z") && billtype.equals("DC") && content_code.length() <= 20) {

			String value_IB_12582_CONTENTINFO = Constant.IB_12582_CONTENTINFO_MAP.get(content_code);
			if (value_IB_12582_CONTENTINFO == null) {
				logger_trans.warn("Not find in datp0_ib_12582_contentinfo, vc_content_id = " + content_code);
			} else {
				subvalue_IB_12582_CONTENTINFO = value_IB_12582_CONTENTINFO.split(MysqlJdbcProcess.SPLITCHAR);
				if (start_time.compareTo(subvalue_IB_12582_CONTENTINFO[3]) < 0 
						|| (!subvalue_IB_12582_CONTENTINFO[4].equals("") && start_time.compareTo(subvalue_IB_12582_CONTENTINFO[4]) > 0)) {
					logger_trans.warn("Find vc_content_id in datp0_ib_12582_contentinfo cdr_time " + start_time + "not between valid and expire!\n");
				} else {
					content_name = subvalue_IB_12582_CONTENTINFO[0];
					provider_name = subvalue_IB_12582_CONTENTINFO[2];
				}
			}
			
		}
		
		// 只有在datp0_ib_mm_content_info 或 datp0_ib_12582_contentinfo找到才使用该规则翻译，否则都走原逻辑
		if (!content_name.equals("") || !provider_name.equals("")) {
			m_strSpInfos.bus_name = content_name;
			m_strSpInfos.sp_name = provider_name;
			ret_val = 0;
		}
		
		return ret_val;
	}

	private int FindSpTranslate(String spcode) {
		String value_INF_BUS = Constant.INF_BUS_MAP.get(spcode);
		
		String[] subvalue_INF_BUS = null;

		if (value_INF_BUS == null) {
			m_strSpInfos.bus_name = spcode;
	        logger_trans.warn("Not found in rptp0_inf_bus, spcode = " + spcode);
			return 0;
		} else {
			subvalue_INF_BUS = value_INF_BUS.split(MysqlJdbcProcess.SPLITCHAR);
			String bus_name = subvalue_INF_BUS[0];
			if (bus_name != null && !bus_name.equals("")){
				m_strSpInfos.bus_name = bus_name;
				return 1;
			} else {
				m_strSpInfos.bus_name = spcode;
				logger_trans.warn("Found in rptp0_inf_bus, but sp_name is ''. spcode = " + spcode);
				return 0;
			}
		}
	}
	
	private void FindGameInfo(String cdr) {
		String cbe_type;	// 2
		String file_type;	// 2
		String bill_type;	// 2
		String content_code;// 30
		String start_time;	// 8

	    cbe_type = cdr.substring(0, 2);
	    file_type = cdr.substring(19, 21);
	    bill_type = cdr.substring(164, 166);
	    content_code = cdr.substring(903, 933).trim();
	    start_time = cdr.substring(75, 83);

	    if (cbe_type.equals("02") && file_type.equals("3Z") && bill_type.equals("DC") && content_code.length() <= 11) {

			ArrayList<String> value_IB_GAME_CONTENT_INFO = Constant.IB_GAME_CONTENT_INFO_MAP.get(content_code);
			if (value_IB_GAME_CONTENT_INFO == null) {
				logger_trans.warn("Not find in datp0_ib_game_content_info, content_code = " + content_code);
			} else {
			    String[] subvalue_IB_GAME_CONTENT_INFO = null;
			    for (String s : value_IB_GAME_CONTENT_INFO) {
			    	subvalue_IB_GAME_CONTENT_INFO = s.split(MysqlJdbcProcess.SPLITCHAR);
			        if (start_time.compareTo(subvalue_IB_GAME_CONTENT_INFO[1]) >= 0 
			        		&& (!subvalue_IB_GAME_CONTENT_INFO[2].equals("") && start_time.compareTo(subvalue_IB_GAME_CONTENT_INFO[2]) <= 0)){
						if (!subvalue_IB_GAME_CONTENT_INFO.equals("")){
							m_strSpInfos.bus_name += "|" + subvalue_IB_GAME_CONTENT_INFO[0];
			            	return ;
						}else {
							logger_trans.warn("Found in datp0_ib_game_content_info with correct start_time, but content_name is '', content_code=" + content_code);
							return ;
						}
			        }
			    }
			    logger_trans.warn("Not Found in datp0_ib_game_content_info with incorrect start_time, content_code=" + content_code);
			}
		}
	}

	private void AddRefund(String cdr) {
		String fee = null;
		String bill_type = null;
		bill_type = cdr.substring(164, 166);
		if (cdr.startsWith("02") && cdr.charAt(702) == '2' && cdr.charAt(669) == '7' && cdr.substring(19, 21).equals("5D")){
			if (bill_type.equals("CC")) {
				fee = cdr.substring(216, 224);
			} else if(bill_type.equals("DC")) {
				fee = cdr.substring(224, 232);
			} else {
				return ;
			}
			
			if (StringUtil.ParseLong(fee) < 0 && !m_strSpInfos.bus_name.equals("")){
				m_strSpInfos.bus_name += "退费";
			}
		}
	}

	private int GetMod3(String cdr) {
		String bus_type = cdr.substring(665, 667);
		
		if (bus_type.charAt(0) == '0') {
			return VOICE;
		} else if (bus_type.charAt(0) == '3') {
			return GPRS_WLAN;
		} else if (bus_type.charAt(0) == '6' && !bus_type.equals("60")) {
			return GPRS_WLAN;
		} else if (bus_type.charAt(0) == '5' && !bus_type.equals("50")) {
			return SMS;
		} else if (bus_type.equals("50") || bus_type.equals("70")) {
			return FIX;
		} else {
			return OTHER;
		}
	}

	private void SortCDR() throws IOException {
        
        Collections.sort(m_vecCDR, new Comparator<CDRINFO>(){
            public int compare(CDRINFO cdrInfoL, CDRINFO cdrInfoR) {
                if (cdrInfoL.modid-cdrInfoR.modid != 0) {
                	return cdrInfoL.modid-cdrInfoR.modid;
                } else {
                	int posL = GetStartPos(cdrInfoL.cdr);
                	int posR = GetStartPos(cdrInfoR.cdr);
                	return cdrInfoL.cdr.substring(posL, posL+14).compareTo(cdrInfoR.cdr.substring(posR, posR+14));
                }
            }
        }); 
        
        logger_trans.info("* SortCDR: 号码："+ m_subno); 
        logger.info("* SortCDR: 号码："+ m_subno); 
	}

	private int GetStartPos(String cdr) {
		int pos = 75;
		
		if (cdr.startsWith("06")) {
			pos = 131;
		} else if (cdr.startsWith("07")) {
			pos = 146;
		}
		
		return pos;
	}
	
	private void CombileReversalCDR() throws IOException {
		int mergecnt = 0;

		for (int i = 0; i != m_vecCDR.size(); i++) {
			if (m_vecCDR.elementAt(i).flag == CDR_EFFECTIVE) {
				boolean isFoundReversal = false;
				int pos = GetStartPos(m_vecCDR.elementAt(i).cdr);
				String time = m_vecCDR.elementAt(i).cdr.substring(pos, pos+14);

				if (m_vecCDR.elementAt(i).cdr.charAt(2) == '1') { // 冲销标志
					m_vecCDR.elementAt(i).flag = CDR_NONE_EFFECTIVE;
				}

				int tmp_pos = 0;
				for (int j = i + 1; j != m_vecCDR.size(); j++) {
					tmp_pos = GetStartPos(m_vecCDR.elementAt(j).cdr);
					if (!time.equals(m_vecCDR.elementAt(j).cdr.substring(tmp_pos, tmp_pos+14))) {
						break;
					}

					if (m_vecCDR.elementAt(j).flag == CDR_NONE_EFFECTIVE) {
						continue;
					}

					if (IsReversal(m_vecCDR.elementAt(i).cdr, m_vecCDR.elementAt(j).cdr)) {
						m_vecCDR.elementAt(i).flag = CDR_NONE_EFFECTIVE;
						m_vecCDR.elementAt(j).flag = CDR_NONE_EFFECTIVE;
						isFoundReversal = true;
					}
				}
				
				if (isFoundReversal) {
					mergecnt += 2;
				} else if (m_vecCDR.elementAt(i).flag == CDR_NONE_EFFECTIVE) {
					mergecnt += 1;
				}
			}
		}
        logger_trans.info("* CombileReversalCDR: 号码："+ m_subno + "，合并过滤条数："+mergecnt); 
        logger.info("* CombileReversalCDR: 号码："+ m_subno + "，合并过滤条数："+mergecnt); 
	}
	
	private boolean IsReversal(String cdr1, String cdr2) {
		
		if (cdr1.charAt(2) == cdr2.charAt(2)) {
			return false;
		}

		if (!cdr1.startsWith(cdr2.substring(0,2))) {
			return false;
		}

		if (cdr1.length() !=cdr2.length()) {
			return false;
		}

		int cbetype = StringUtil.ParseInt(cdr1.substring(0,2));
		if (cbetype >= 1 && cbetype <= 5) {
			if (!cdr1.substring(15,396).equals(cdr2.substring(15,396))) {
				return false;
			}
			
			if (!cdr1.substring(408).equals(cdr2.substring(408))) {
				return false;
			}

		} else if (cbetype >= 6 && cbetype <= 7) {
			if (!cdr1.substring(15,88).equals(cdr2.substring(15,88))) {
				return false;
			}
			
			if (!cdr1.substring(100).equals(cdr2.substring(100))) {
				return false;
			}
		} else if (cbetype >= 11 && cbetype <= 15) {
			if (!cdr1.substring(15,420).equals(cdr2.substring(15,420))) {
				return false;
			}
			
			if (!cdr1.substring(432).equals(cdr2.substring(432))) {
				return false;
			}
		}

		return true;
	}

	public void CombileGprsCDR() throws IOException {
	    String mergeKey; // 115
		int toll_fee = 0;
		int mergecnt = 0;

		MergeGprsMap = new HashMap<String, MergeValue>();
		
	    for (int i = 0; i != m_vecCDR.size(); i++) {
	         if (m_vecCDR.elementAt(i).flag == CDR_EFFECTIVE && m_vecCDR.elementAt(i).cdr.startsWith("03")) {
				 toll_fee = StringUtil.ParseInt(m_vecCDR.elementAt(i).cdr.substring(216, 224));
				 
				 if ("0/1/2/3".contains(m_vecCDR.elementAt(i).cdr.substring(1006, 1007)) 
						 && "AC/GC".contains(m_vecCDR.elementAt(i).cdr.substring(164, 166)) && toll_fee == 0) {
					 
					 mergeKey = GetMergeKey(m_vecCDR.elementAt(i).cdr);
					 mergecnt += CombileMergeValue(m_vecCDR.elementAt(i), mergeKey, i);
	             }
	         }
	    }

	    Iterator<Entry<String, MergeValue>> iter = MergeGprsMap.entrySet().iterator();
	    while (iter.hasNext()) {
	    	@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next();
	        UpdateGprsCdr((MergeValue) entry.getValue());
	    }
        logger_trans.info("* CombileGprsCDR: 号码："+ m_subno + "，合并过滤条数："+mergecnt); 
        logger.info("* CombileGprsCDR: 号码："+ m_subno + "，合并过滤条数："+mergecnt); 
	}
	
	private String GetMergeKey(String cdr) {
		String charging_id = cdr.substring(772, 782).trim();
		String start_time = cdr.substring(75, 83).trim();
		String asubno = cdr.substring(350, 374).trim();
		String special_flag = cdr.substring(89, 91).trim();
		String dis_id = cdr.substring(233, 297).trim();
		char net_type = cdr.charAt(991);
		int flag = 0; 
		
		// 取第一个产品id做合并
		String[] values = dis_id.split(",");
		dis_id = values[0];

		if (net_type != '4' && net_type != '3') {
			net_type = '2';
		}
		
		// BR201509060013 和飞信业务支撑改造
		if ("3017".equals(cdr.substring(665, 669))){
			flag=1;
		}
		
		return charging_id+start_time+net_type+asubno+special_flag+dis_id+flag;
	}
	
	private int CombileMergeValue(CDRINFO cdrInfo, String mergeKey, int i) {
		String up_usage = cdrInfo.cdr.substring(961, 972);
		String down_usage = cdrInfo.cdr.substring(972, 983);
		String timelong = cdrInfo.cdr.substring(934, 941);
		String dis_dura = cdrInfo.cdr.substring(707, 719);
		String mob_fee = cdrInfo.cdr.substring(208, 216);
		String toll_fee = cdrInfo.cdr.substring(216, 224);
		String inf_fee = cdrInfo.cdr.substring(224, 232);
		String bill_type = cdrInfo.cdr.substring(164, 166);
		String discount_lastmon = cdrInfo.cdr.substring(1024, 1030);

	    if (StringUtil.ParseInt(up_usage) == 0 && StringUtil.ParseInt(down_usage) == 0 && StringUtil.ParseInt(timelong) == 0) {
	    	cdrInfo.flag = CDR_NONE_EFFECTIVE;
    	    return 1;
	    }

		if (bill_type.startsWith("GC") && (StringUtil.ParseInt(up_usage)/1024 + StringUtil.ParseInt(down_usage)/1024 == 0) 
				&& (StringUtil.ParseInt(mob_fee) + StringUtil.ParseInt(toll_fee) + StringUtil.ParseInt(inf_fee)) == 0) {
			cdrInfo.flag = CDR_NONE_EFFECTIVE;
    	    return 1;
		}
		
	    MergeValue mv = new MergeValue();
	    mv.up_usage = StringUtil.ParseLong(up_usage)/1024*1024;
	    mv.down_usage = StringUtil.ParseLong(down_usage)/1024*1024;
	    mv.timelong = StringUtil.ParseInt(timelong);
	    mv.index = i;
	    mv.mob_fee =  StringUtil.ParseInt(mob_fee);
	    mv.dis_dura = StringUtil.ParseLong(dis_dura);
	    mv.discount_lastmon = StringUtil.ParseLong(discount_lastmon);

	    MergeValue curr_mv = MergeGprsMap.get(mergeKey);
	    if (curr_mv == null) {
    	    MergeGprsMap.put(mergeKey, mv);
	    } else {
	        if ((curr_mv.up_usage + mv.up_usage > StringUtil.ParseLong("99999999999"))
	                || (curr_mv.down_usage + mv.down_usage > StringUtil.ParseLong("99999999999"))
	                || (curr_mv.timelong + mv.timelong > 9999999)
	    			|| (curr_mv.discount_lastmon + mv.discount_lastmon > 999999)) {
	        	
	        	UpdateGprsCdr(curr_mv);
	    	    MergeGprsMap.put(mergeKey, mv);
	        } else {
	        	curr_mv.up_usage += mv.up_usage;
		        curr_mv.down_usage += mv.down_usage;
		        curr_mv.timelong += mv.timelong;
		        curr_mv.mob_fee += mv.mob_fee;
		        curr_mv.dis_dura += mv.dis_dura;
		        curr_mv.discount_lastmon += mv.discount_lastmon;
		        
	    		cdrInfo.flag = CDR_NONE_EFFECTIVE;
	    	    return 1;
	        }
		}
	    return 0;
	}

	private void UpdateGprsCdr(MergeValue curr_mv) { 
		StringBuffer cdr = new StringBuffer(m_vecCDR.elementAt(curr_mv.index).cdr);

		cdr.replace(961, 972, String.format("%011d", curr_mv.up_usage));
		cdr.replace(972, 983, String.format("%011d", curr_mv.down_usage));
		cdr.replace(934, 941, String.format("%07d", curr_mv.timelong));
		cdr.replace(208, 216, String.format("%08d", curr_mv.mob_fee));
		cdr.replace(707, 719, String.format("%012d", curr_mv.dis_dura));
		cdr.replace(1024, 1030, String.format("%06d", curr_mv.discount_lastmon));
		
		m_vecCDR.elementAt(curr_mv.index).cdr = cdr.toString();
	}

	private int TranslateCDR() throws IOException {
		List<String> tmpValue = null;
		Vector<Integer> Mods = new Vector<Integer>();
		String[] subvalue_INF_MOD_COLUMN = null;
		String[] value_INF_COLUMN = null;
		String[] value_INF_MOD = null;
		StringBuffer outString = null;
		Integer seqid = null;
		Integer columnid = null;
		int rowcnt=0;
		int j=0;
		int tmp_mod_id = -1;
		int tmp_mod_id2 = -1; // 用于共用模板的判断， 如52查询，集团短菜和代付数据业务是共用模板12

		for (Integer mod : Constant.INF_QUERY_MAP.get(m_type).values()) {
			Mods.add(mod);
		}
		Collections.sort(Mods); 
		Iterator<Integer> iter_Mods = Mods.iterator();
		
		m_resbuf.setLength(0);
		m_resbuf.append("Count=");
		
		for (CDRINFO cdrInfo : m_vecCDR) {
			if (cdrInfo.flag == CDR_NONE_EFFECTIVE) {
				continue;
			}
			
			if (cdrInfo.modid != tmp_mod_id) { // 当清单是新的模板
				
				while (cdrInfo.modid != (tmp_mod_id = iter_Mods.next().intValue())) { // 当清单跳过了某些模板，需要打印这些模板头

					if (tmp_mod_id == tmp_mod_id2) { // 可能共用模板的情况
						continue;
					}
					
					tmp_mod_id2 = tmp_mod_id;
					if (Constant.INF_MOD_MAP.get(tmp_mod_id2) != null) {
						value_INF_MOD = Constant.INF_MOD_MAP.get(tmp_mod_id2).split(MysqlJdbcProcess.SPLITCHAR);
					} else {
						logger_trans.error("cbep0_inf_mod 没有配置列头信息3，int_mod_id="+tmp_mod_id2);
						throw new IllegalStateException();
					}
					m_resbuf.append(value_INF_MOD[1]+"&CDR=");
				}
				
				if (Constant.INF_MOD_MAP.get(tmp_mod_id) != null) {
					value_INF_MOD = Constant.INF_MOD_MAP.get(tmp_mod_id).split(MysqlJdbcProcess.SPLITCHAR);
				} else {
					logger_trans.error("cbep0_inf_mod 没有配置列头信息4，int_mod_id="+tmp_mod_id+",cdr="+cdrInfo.cdr.substring(0, 20));
					throw new IllegalStateException();
				}
				m_resbuf.append(value_INF_MOD[1]);
				
				if (Constant.INF_MOD_COLUMN_MAP.get(tmp_mod_id) != null){
					tmpValue = Constant.INF_MOD_COLUMN_MAP.get(tmp_mod_id);
				} else {
					logger_trans.error("cbep0_inf_mod_column 没有配置模板列信息，int_mod_id="+tmp_mod_id);
					throw new IllegalStateException();
				}
			}

			j = 0;
			int cdrtype = StringUtil.ParseInt(cdrInfo.cdr.substring(0, 2));
			outString = new StringBuffer("&CDR=");
			ColumnInfo columnInfo = new ColumnInfo();

			Integer tmp_modid = 0;
			for (String s : tmpValue){

				// 要先运行，因为假如先做了sp_use_type返回是空，再判断bus_name获得sp_use_type，是不会再回填进去的了
				if (s.contains("bus_name") && cdrtype != 6) {
					FindBusTranslate(cdrInfo.cdr);

					// 针对增值业务和SP业务，要填充主副号代付关系
					String special_flag = cdrInfo.cdr.substring(89, 91);
					if ((tmp_modid = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+MOBILESELF)) != null
							&& cdrInfo.modid == tmp_modid) {
						if(special_flag.equals("d3") || special_flag.equals("d7")){
							m_strSpInfos.use_type += "(特殊业务被代付)";
						} else if (special_flag.equals("d4") || special_flag.equals("d8")) {
							m_strSpInfos.use_type += "(" + ColumnMethod.translate_dis_id_real_msisdn(cdrInfo.cdr) + ")";
						}
					} else if ((tmp_modid = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+SP)) != null
							&& cdrInfo.modid == tmp_modid) {
						if(special_flag.equals("d3") || special_flag.equals("d7")){
							m_strSpInfos.use_type += "(SP业务被代付)";
						} else if (special_flag.equals("d4") || special_flag.equals("d8")) {
							m_strSpInfos.use_type += "(" + ColumnMethod.translate_dis_id_real_msisdn(cdrInfo.cdr) + ")";
						}
					}
					break;
				}
			}
			
			for (String s : tmpValue){
				subvalue_INF_MOD_COLUMN = s.split(MysqlJdbcProcess.SPLITCHAR);

				seqid = StringUtil.ParseInt(subvalue_INF_MOD_COLUMN[0]);
				columnid = StringUtil.ParseInt(subvalue_INF_MOD_COLUMN[1]);
				
				if (j != seqid) {
					logger_trans.error("cbep0_inf_mod_column 序列号配置不连续，int_mod_id="+tmp_mod_id+",int_seq_id="+j);
					throw new IllegalStateException();
				}
				
				// 取列字段对应的清单值
				if ((Constant.INF_COLUMN_MAP.get(columnid) != null) && (Constant.INF_COLUMN_MAP.get(columnid).get(columnid+MysqlJdbcProcess.SPLITCHAR+cdrtype) != null)) {
					value_INF_COLUMN = Constant.INF_COLUMN_MAP.get(columnid).get(columnid+MysqlJdbcProcess.SPLITCHAR+cdrtype).split(MysqlJdbcProcess.SPLITCHAR);
					
					// 翻译
					columnInfo.cbe_type = cdrtype;
					columnInfo.get_flag = StringUtil.ParseInt(value_INF_COLUMN[0]);
					columnInfo.offset = StringUtil.ParseInt(value_INF_COLUMN[1]);
					columnInfo.len = StringUtil.ParseInt(value_INF_COLUMN[2]);
					columnInfo.tips = subvalue_INF_MOD_COLUMN[2];
					columnInfo.cdr = cdrInfo.cdr;
					columnInfo.rawValue = columnInfo.cdr.substring(columnInfo.offset, columnInfo.offset+columnInfo.len);

					outString.append(GetColumnValue(columnInfo) + value_INF_MOD[0]);
				} else {
					logger_trans.error("cbep0_inf_column 没有配置此列信息，int_column_id="+columnid+",int_cbe_type="+cdrtype);
					outString.append("" + value_INF_MOD[0]); // 例如05单没有配置结转流量取值
				}
				
				j++;
			}
			
			outString.deleteCharAt(outString.length()-1);
			rowcnt++;
			m_resbuf.append(outString);
		}
		
		tmp_mod_id2 = -1;
		while (iter_Mods.hasNext()) {
			tmp_mod_id2 = iter_Mods.next().intValue();
			if (tmp_mod_id == tmp_mod_id2) { // 可能共用模板的情况
				continue;
			} else {
				tmp_mod_id = tmp_mod_id2;
			}
			
			if (Constant.INF_MOD_MAP.get(tmp_mod_id) != null) {
				value_INF_MOD = Constant.INF_MOD_MAP.get(tmp_mod_id).split(MysqlJdbcProcess.SPLITCHAR);
			} else {
				logger_trans.error("cbep0_inf_mod 没有配置列头信息5，int_mod_id="+tmp_mod_id);
				throw new IllegalStateException();
			}
			m_resbuf.append(value_INF_MOD[1]+"&CDR=");
			
		}
		
		m_resbuf.insert("Count=".length(), rowcnt);
		m_resbuf.append("&MSG=Successful");
        logger_trans.info("* TranslateCDR: 号码："+ m_subno + "，返回总行数为："+rowcnt); 
        logger.info("* TranslateCDR: 号码："+ m_subno + "，返回总行数为："+rowcnt); 
		logger_trans.info("************ translate end ************");
		return rowcnt;
	}

	private String GetColumnValue(ColumnInfo columnInfo) {
		String methodName = "get_" + columnInfo.tips;
		
		if (columnInfo.tips.equals("sp_use_type") && columnInfo.cbe_type != 6) {
			return m_strSpInfos.use_type;
		} else if (columnInfo.tips.equals("sp_fee_type")) {
			return m_strSpInfos.fee_type;
		} else if (columnInfo.tips.equals("bus_name") && columnInfo.cbe_type != 6) {
			return m_strSpInfos.bus_name;
		} else if (columnInfo.tips.equals("sp_name")) {
			return m_strSpInfos.sp_name;
		} else {
			// 通用逻辑

			Class<?> columnMethodClass = null;
			try {
				columnMethodClass = Class.forName("com.asiainfo.method.ColumnMethod");
			} catch (ClassNotFoundException e) {
				logger_trans.error("", e);
				return "";
			}
			
			Method method = null;
			@SuppressWarnings("rawtypes")
			Class[] argsClass = {columnInfo.getClass()};
			try {
				method = columnMethodClass.getMethod(methodName, argsClass);
			} catch (SecurityException e) {
				logger_trans.error("", e);
				return "";
			} catch (NoSuchMethodException e) {
				logger_trans.error("", e);
				return "";
			}
			
			String transValue = null;
			try {
				transValue = (String) method.invoke(null, columnInfo);
			} catch (StringIndexOutOfBoundsException e) {
				logger_trans.error("", e);
			} catch (NullPointerException e) {
				logger_trans.error("", e);
			} catch (IllegalArgumentException e) {
				logger_trans.error("", e);
			} catch (IllegalAccessException e) {
				logger_trans.error("", e);
			} catch (InvocationTargetException e) {
				logger_trans.warn(columnInfo.cdr.substring(0, 20)+":"+methodName, e);
			} catch (Exception e) {
				logger_trans.warn(columnInfo.cdr.substring(0, 20)+":"+methodName, e);
			}
			return transValue==null? "":transValue;
		}
	}
	
	private int ViceNumberQuery() {
		String specialFlag = null;	//特殊业务标志
		String realMsisdn = null;	//真实号码
		String m_type = null;		//清单类型
		String yearmon = null;		//话单开始年月
		Map<String, Map<String, ViceNumber>> realMap = new HashMap<String, Map<String, ViceNumber>>();//<realMsisdn, dataMap>
		Map<String, ViceNumber> dataMap = null;//<m_type, ViceNumber>
		ViceNumber viceNumber = null;
		
		for (CDRINFO oneCDRINFO : m_vecCDR) {
			
			if (oneCDRINFO.flag == CDR_NONE_EFFECTIVE) {
				continue;
			}
			
			m_type = oneCDRINFO.cdr.substring(0, 2).trim();
			specialFlag = oneCDRINFO.cdr.substring(89, 91);
			
			/*过滤清单*/
			if(!"06".equals(m_type) && !"d8".equals(specialFlag)){
				continue;
			}
			
			/*
			 * 01: 通话 [累加时长]
			 * 02: 短彩信 [累加条数]
			 * 03: GPRS[累加时长和流量]
			 * 05: WLAN [累加时长和流量]
			 * 06: 固定话费[累加账单科目acctid为699的金额]
			 */
			if("01".equals(m_type)) {
				realMsisdn = oneCDRINFO.cdr.substring(824, 848).trim();
			} else if("02".equals(m_type)) {
				realMsisdn = oneCDRINFO.cdr.substring(786, 810).trim();
			} else if("03".equals(m_type)) {
				realMsisdn = oneCDRINFO.cdr.substring(782, 806).trim();
			} else if("05".equals(m_type)) {
				realMsisdn = oneCDRINFO.cdr.substring(765, 789).trim();
			} else if("06".equals(m_type)) {
				realMsisdn = "";//06单无真实号码
			}
			
			/*得到账单日期*/
			if("06".equals(m_type)){
				yearmon = oneCDRINFO.cdr.substring(43, 49);
			} else {
				yearmon = oneCDRINFO.cdr.substring(75, 81);
			}
			
			dataMap = realMap.get(realMsisdn);
			if(dataMap == null){
				dataMap = new TreeMap<String, ViceNumber>();
				realMap.put(realMsisdn, dataMap);
			}
			
			/*
			 * 取清单数据以及逻辑
			 */
			int fee = 0;		//费用
			long duration = 0;	//通话时长
			int counts = 0;		//记录数
			long volume = 0;	//总流量
			String durVolFlag = "";
			if("01".equals(m_type)) {
				
				duration = StringUtil.ParseInt(oneCDRINFO.cdr.substring(763, 770).trim());
				
			} else if("02".equals(m_type)) {
				
				String cdrTag = oneCDRINFO.cdr.substring(810, 812).trim();
				if("01".equals(cdrTag) || "02".equals(cdrTag)){
					counts = 1;
				}
				
			} else if("03".equals(m_type)) {
				
				durVolFlag = oneCDRINFO.cdr.substring(769, 770).trim();//计费标识
				if("0".equals(durVolFlag)){
					//时长计费
					duration = StringUtil.ParseInt(oneCDRINFO.cdr.substring(934, 941).trim());
				} else {
					//流量计费
					long up_volume = StringUtil.ParseLong(oneCDRINFO.cdr.substring(961, 972).trim())/1024;
					long down_volume = StringUtil.ParseLong(oneCDRINFO.cdr.substring(972, 983).trim())/1024;
					volume = up_volume + down_volume;
				}
				
			} else if("05".equals(m_type)) {
				
				String xdType = oneCDRINFO.cdr.substring(649, 650).trim();
				if("3".equals(xdType)){
					durVolFlag = oneCDRINFO.cdr.substring(761, 762).trim();//计费标识
					if("1".equals(durVolFlag)){
						//时长计费
						duration = StringUtil.ParseInt(oneCDRINFO.cdr.substring(789, 796).trim());
					} else {
						//流量计费
						long up_volume = StringUtil.ParseLong(oneCDRINFO.cdr.substring(796, 809).trim())/1024;
						long down_volume = StringUtil.ParseLong(oneCDRINFO.cdr.substring(809, 822).trim())/1024;
						volume = up_volume + down_volume;
					}
				}
				
			} else if("06".equals(m_type)) {
				
				int acctid = StringUtil.ParseInt(oneCDRINFO.cdr.substring(62, 76).trim());
				if(acctid == 699){
					fee = StringUtil.ParseInt(oneCDRINFO.cdr.substring(76, 88).trim())/10;
				}
				
			}
			
			/*填充数据*/
			viceNumber = dataMap.get(m_type+"_"+yearmon);
			if(viceNumber == null){
				viceNumber = new ViceNumber(realMsisdn, Integer.parseInt(m_type), yearmon, fee, duration, counts, volume);
				dataMap.put(m_type+"_"+yearmon, viceNumber);
			} else {
				viceNumber.fee += fee;
				viceNumber.duration += duration;
				viceNumber.counts += counts;
				viceNumber.volume += volume;
			}
			
		}
		
		// 随便给一个cdr_type即可，例如语音1
		int mod_id = 0;
		String[] value_INF_MOD = null;
		if ((Constant.INF_QUERY_MAP.get(m_type) != null) && (Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1") != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1");
		} else {
			logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type=1");
			throw new IllegalStateException();
		}
		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger_trans.error("cbep0_inf_mod 没有配置列头信息2，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}
		
		m_resbuf.append("Count=");
		m_resbuf.append(value_INF_MOD[1]);
		
		//m_resbuf.append("Count=&Row=7&TYPE=副号号码,代付清单类型,账务周期,费用,时长,条数,流量");

		int count = 0;
		for (Map.Entry<String, Map<String, ViceNumber>> _realMap : realMap.entrySet()) {
			Map<String, ViceNumber> _dataMap = _realMap.getValue();
			Iterator<String> it = _dataMap.keySet().iterator();
			while (it.hasNext()) {
				String key = it.next().toString();
				ViceNumber vn = _dataMap.get(key);
				m_resbuf.append("&CDR=");
				m_resbuf.append(vn.toString());
				count++;
			}
		}
		m_resbuf.insert("Count=".length(), count);
		m_resbuf.append("&MSG=Successful");
		
		logger_trans.info("*********** translate end ***********");
		
		return m_resbuf.length();
	}
	
	/*
	 * BR201610190011 清账单中心向电子渠道提供历史流量数据查询的需求
	 * LLHistoryMonthQuery();
	 * LLHistoryDayQuery();
	 */
	private int LLHistoryMonthQuery(StringBuffer _resbuf, String _subno, String _beginDate, String _endDate) {
		m_resbuf = _resbuf;
		m_subno = _subno;

		int begmonth = StringUtil.ParseInt(_beginDate);
		int endmonth =  StringUtil.ParseInt(_endDate);
		int begyear = StringUtil.ParseInt(_beginDate.substring(0, 4));
		int count = 0;
		
		logger.info("实际的开始时间：" + begmonth + "，结束时间：" + endmonth);
		logger_trans.info("*********** translate begin ***********");
		
		// 1.获取模版头
		int mod_id = 0;
		String[] value_INF_MOD = null;if ((Constant.INF_QUERY_MAP.get(m_type) != null) && (Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1") != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1");
		} else {
			logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type=1");
			throw new IllegalStateException();
		}
		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger_trans.error("cbep0_inf_mod 没有配置列头信息2，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}
		m_resbuf.setLength(0);
		m_resbuf.append("Count=");
		m_resbuf.append(value_INF_MOD[1]);
		
		// 2.查询并且统计组装数据
		while (begmonth <= endmonth) {
			try {
				// 2.1 查询数据
				long startTime = System.currentTimeMillis();
				m_hbaseRow += GetHbase(_subno, "201", "202", String.valueOf(begmonth));
				long hbaseTime = System.currentTimeMillis() - startTime;
				m_hbaseTime += hbaseTime;
				logger.info("# hadoop查询完毕，计费月："+begmonth+"，号码："+_subno+"，行数："+m_vecCDR.size()+"，耗时："+hbaseTime+"ms");
				
				// 2.2 累计流量单
				startTime = System.currentTimeMillis();
				
				SplitCDR();// 清单拆分补全
				FilterCDR();// 清单过滤
				CombileReversalCDR();// 合并冲销单
				
				String date = String.valueOf(begmonth);
				String billtype = null;
				String filetype = null;
				long total_volume = 0;
				long dis_dura = 0;
				String tmp_total_volume = null;
				String tmp_dis_dura = null;
				String tmp_outside = null;

				for (CDRINFO cdrInfo : m_vecCDR) {
					if (cdrInfo.flag == CDR_NONE_EFFECTIVE) {
						continue;
					}
					
					if(cdrInfo.cdr.charAt(649) != '3') {
						continue;
					}
					
					billtype = cdrInfo.cdr.substring(164, 166);
					filetype = cdrInfo.cdr.substring(19, 21);
					
					long up_volume = StringUtil.ParseLong(cdrInfo.cdr.substring(961, 972));
					long down_volume = StringUtil.ParseLong(cdrInfo.cdr.substring(972, 983));
					
					if ("WC".contains(billtype) && filetype.equals("28") || "HC".contains(billtype)) {
						total_volume += ((up_volume + down_volume + 1023) / 1024);
					} else {
						total_volume += ((up_volume + down_volume) / 1024);
					}
					
					dis_dura += StringUtil.ParseLong(cdrInfo.cdr.substring(707, 719)) / 1024;
				}
				
				// 2.3 翻译
				tmp_total_volume = String.format("%.2f", total_volume/(double)(1024*1024));//总流量=上行+下行
				tmp_dis_dura = String.format("%.2f", dis_dura/(double)(1024*1024));
				tmp_outside = String.format("%.2f", StringUtil.ParseDouble(tmp_total_volume) - StringUtil.ParseDouble(tmp_dis_dura));//套餐外 = 总流量 - 套餐内
				
				m_resbuf.append("&CDR="+date + "," + tmp_total_volume + "GB," + tmp_outside + "GB," + tmp_dis_dura + "GB");
				count++;
				
				long transTime = System.currentTimeMillis() - startTime;
				m_transTime += transTime;
			} catch (IOException e) {
				logger.warn("Debug模式异常", e);
			} finally {
				if (begmonth % 100 == 12) {
					begmonth = (++begyear) * 100;
				}
				begmonth++;			
			}
		}
		
		m_resbuf.insert("Count=".length(), count);
		m_resbuf.append("&MSG=Successful");
		
		logger_trans.info("************ translate end ************");
		return count;
	}
	
	private int LLHistoryDayQuery() throws IOException {
		CombileReversalCDR();// 合并冲销单
		
		// 1.获取模版头
		int mod_id = 0;
		String[] value_INF_MOD = null;
		if ((Constant.INF_QUERY_MAP.get(m_type) != null) && (Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1") != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1");
		} else {
			logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type=1");
			throw new IllegalStateException();
		}
		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger_trans.error("cbep0_inf_mod 没有配置列头信息2，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}
		m_resbuf.setLength(0);
		m_resbuf.append("Count=");
		m_resbuf.append(value_INF_MOD[1]);
		
		// 2.整理数据
		int key = -1;
		int tmp_key = -1;
		int count = 0;
		String billtype = null;
		String filetype = null;
		long total_volume = 0;
		long dis_dura = 0;
		
		for (CDRINFO cdrInfo : m_vecCDR) {
			if (cdrInfo.flag == CDR_NONE_EFFECTIVE) {
				continue;
			}
			
			if(cdrInfo.cdr.charAt(649) != '3') {
				continue;
			}
			
			// 2.1 按日期分类03清单 
			key = Integer.parseInt(cdrInfo.cdr.substring(75, 83));// key = start_time(75, 75+8)-->20161201
			
			if (tmp_key == -1) {//第一次循环
				tmp_key = key;
			} else if(key != tmp_key) {
				// 2.3 翻译上一次累加结果
				LLHistoryDayTranslation(tmp_key, total_volume, dis_dura);
				count++;
				
				total_volume = dis_dura = 0L;
				tmp_key = key;
			}
			
			// 2.2 根据key累加
			billtype = cdrInfo.cdr.substring(164, 166);
			filetype = cdrInfo.cdr.substring(19, 21);
			
			long up_volume = StringUtil.ParseLong(cdrInfo.cdr.substring(961, 972));
			long down_volume = StringUtil.ParseLong(cdrInfo.cdr.substring(972, 983));
			
			if ("WC".contains(billtype) && filetype.equals("28") || "HC".contains(billtype)) {
				total_volume += ((up_volume + down_volume + 1023) / 1024);
			} else {
				total_volume += ((up_volume + down_volume) / 1024);
			}
			dis_dura += StringUtil.ParseLong(cdrInfo.cdr.substring(707, 719)) / 1024;
			
		}
		
		if(key != -1){
			// 2.3 翻译最后一次累加结果
			LLHistoryDayTranslation(tmp_key, total_volume, dis_dura);
			count++;
		}
		
		m_resbuf.insert("Count=".length(), count);
		m_resbuf.append("&MSG=Successful");
		
		logger_trans.info("************ translate end ************");
		return count;
	}
	
	private int LLHistoryMonthSoundQuery() throws IOException {
		CombileReversalCDR();// 合并冲销单
		
		// 1.获取模版头
		int mod_id = 0;
		String[] value_INF_MOD = null;
		if ((Constant.INF_QUERY_MAP.get(m_type) != null) && (Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1") != null)) {
			mod_id = Constant.INF_QUERY_MAP.get(m_type).get(m_type+MysqlJdbcProcess.SPLITCHAR+"1");
		} else {
			logger_trans.error("cbep0_inf_query 没有配置请求信息，int_querytype="+m_type+",int_cbe_type=1");
			throw new IllegalStateException();
		}
		if (Constant.INF_MOD_MAP.get(mod_id) != null) {
			value_INF_MOD = Constant.INF_MOD_MAP.get(mod_id).split(MysqlJdbcProcess.SPLITCHAR);
		} else {
			logger_trans.error("cbep0_inf_mod 没有配置列头信息2，int_mod_id="+mod_id);
			throw new IllegalStateException();
		}
		m_resbuf.setLength(0);
		m_resbuf.append("Count=");
		m_resbuf.append(value_INF_MOD[1]);
		
		// 2.整理数据
		TreeMap<String, Pair<Integer, Integer>> toalSound = new TreeMap<String, Pair<Integer, Integer>>();
		Pair<Integer, Integer> value;
		int count = 0;
		
		for (CDRINFO cdrInfo : m_vecCDR) {
			if (cdrInfo.flag == CDR_NONE_EFFECTIVE || !cdrInfo.cdr.startsWith("01") || !"3/5/6".contains(cdrInfo.cdr.substring(702,703))) {
				continue;
			}
			
			value = toalSound.get(cdrInfo.cdr.substring(75, 81));
			if (value == null) {
				value = new Pair<Integer, Integer>(0, 0);
				toalSound.put(cdrInfo.cdr.substring(75, 81), value);
			}
			
			Integer total = (int) Math.ceil(StringUtil.ParseInt(cdrInfo.cdr.substring(763, 770))/60.0);
			Integer dis_dura = StringUtil.ParseInt(cdrInfo.cdr.substring(716, 723));
			value.setFirst(value.getFirst() + total);
			value.setSecond(value.getSecond() + dis_dura);
		}
		
		// 3.打印
		Iterator<Entry<String, Pair<Integer, Integer>>> it = toalSound.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Pair<Integer, Integer>> entry = it.next();
			value = entry.getValue();
			m_resbuf.append("&CDR="+entry.getKey()+","+value.getFirst()+","+(value.getFirst()-value.getSecond())+","+value.getSecond());
		}
		
		m_resbuf.insert("Count=".length(), toalSound.size());
		m_resbuf.append("&CDR=");
		m_resbuf.append("&MSG=Successful");
		
		logger_trans.info("************ translate end ************");
		return count;
	}
	

	public void LLHistoryDayTranslation(int tmp_key, long total_volume, long dis_dura) {
		String tmp_total_volume = "";
		String tmp_dis_dura = "";
		String tmp_outside = "";
		tmp_total_volume = String.format("%.2f", total_volume/(double)1024);
		tmp_dis_dura = String.format("%.2f", dis_dura/(double)1024);
		tmp_outside = String.format("%.2f", StringUtil.ParseDouble(tmp_total_volume) - StringUtil.ParseDouble(tmp_dis_dura));//套餐外 = 总流量 - 套餐内
		m_resbuf.append("&CDR="+tmp_key + "," + tmp_total_volume + "MB," + tmp_outside + "MB," + tmp_dis_dura + "MB");
	}
	
	private Vector<CDRINFO> m_vecCDR=null; 
	private HashMap<String, MergeValue> MergeGprsMap=null;
	private HashSet<String> rep_set = null;
	private TreeMap<String, LLSum> ll_map = null;
	private int m_type=-1;
	private String m_subno = null;
	private StringBuffer m_resbuf = null;
	private TmpSpInfos m_strSpInfos = null;
	private String realMsisdn = null;
	
	private final static int NOT_SURE = 99;		// need get the cdr_type form the sp info
	private final static int VOICE = 1;			// 语音通话
	private final static int SMS = 2;			// 短/彩信
	private final static int GPRS_WLAN = 3;		// 上网
	private final static int MOBILESELF = 4;	// 增值业务
	private final static int SP = 5;			// 代收费
	private final static int FIX = 6;			// 套餐及固定费
	private final static int OTHER = 7;			// 其他扣费
	private final static int GROUP_VOICE = 8;	// 普通集团语音产品
	private final static int GROUP_DATA = 9;	// 集团短彩产品
	private final static int GROUP_DATA_DF = 10;// 数据业务集团代付
	
	private static class TOLLINFO{
		public String b_switch_flag;// 2
		public String b_operator;	// 2
		public String b_user_type;	// 2
		public String b_brand;		// 1
		public String b_area;		// 10
		public String msisdnC;		// 24
		public String start_time;	// 14
		public String bill_period;	// 6
		public String toll_type;	// 1
		public String roam_type;	// 1
		public String visit_switch_flag;// 2
		
		public TOLLINFO(){
			b_switch_flag="";
			b_operator="";
			b_user_type="";
			b_brand="";
			b_area="";
			msisdnC="";
			start_time="";
			bill_period="";
			toll_type="";
			roam_type="";
			visit_switch_flag="";
		}
	}
	
	private static class KeyInfo {
		String negw_file_type;	//话单格式 2
		String billtype;	//帐单类型 2
		String bustype;		//通信业务类型 2
		String subbustype;	//子业务类型 2
		String buscode;		//业务代码 5
		String icpcode;		//ICP代码 8
		String filetype;	//文件类型 2
		String tolltype;	//长途类型 1
		String switchflag;	//计费号码归属局 2
		String subno;		//号码 24
		String ratetype;	//计费类型 1
		long mobfee;		//长途费
		long tollfee;		//长途费
		
		public KeyInfo(){
			negw_file_type="";
			billtype="";
			bustype="";
			subbustype="";
			buscode="";
			icpcode="";
			filetype="";
			tolltype="";
			switchflag="";
			subno="";
			ratetype="";
			mobfee=0L;
			tollfee=0L;
		}
	}
	
	private static class TmpSpInfos {
		String bus_name;	// 100
		String sp_name;		// 100
		String use_type;	// 50
		String fee_type;	// 50
		String flag;		// 2
		
		public TmpSpInfos(){
			bus_name = "";
			sp_name = "";
			use_type = "";
			fee_type = "";
			flag = "";
		}
	};
		
	private static class MergeValue{
		long up_usage;
		long down_usage;
		int timelong;
		int index;
		int mob_fee;
		long dis_dura;
		long discount_lastmon;
	}
	
	public static class ColumnInfo{
		public int cbe_type;
		public int get_flag;
		public int offset;
		public int len;
		public String cdr;
		public String tips;
		public String rawValue;
		
		public ColumnInfo(){
			cdr="";
			tips="";
			rawValue="";
		}
	}
	
	private static class LLSum{
		String yearmon;		// 年月
		String nettype; 	// 上网方式: 2G,3G,4G,WLAN
		int usetype;		// 使用方式: 0-按流量计费,1-按时长计费
		long total;			// 使用总量，单位B或s
		long inside;		// 套餐内
		long lastturn; 		// 是否为结转: 0-非结转,1-结转 03清单才填此值
		
		public LLSum(){
			yearmon="";
			nettype="";
		}
	}

	private static class ViceNumber {
		//副号号码, 代付清单类型, 账务周期, 费用, 时长，条数，流量
		private String realMsisdn;		//副号号码
		private int m_type;			//清单类型
		private String billPeriod;		//账务周期
		private int fee;				//费用
		private long duration;			//时长
		private int counts;				//条数
		private long volume;			//流量
		public ViceNumber(String realMsisdn, int m_type, String billPeriod, int fee, long duration, int counts, long volume) {
			this.realMsisdn = realMsisdn;
			this.m_type = m_type;
			this.billPeriod = billPeriod;
			this.fee = fee;
			this.duration = duration;
			this.counts = counts;
			this.volume = volume;
		}
		public String toString() {
			return realMsisdn + ","
					+ m_type + ","
					+ billPeriod + ","
					+ (fee==0?m_type==6?"0分":"":fee+"分") + ","
					+ (duration==0?"":duration+"秒") + ","
					+ (counts==0?"":counts+"条") + ","
					+ (volume==0?"":volume+"KB");
		}
	}
	

	public class Pair<E, F> {
		private E first;
		private F second;
	
		public Pair() {
		}
	
		public Pair(E _first, F _second) {
			this.first = _first;
			this.second = _second;
		}
	
		public E getFirst() {
			return first;
		}
	
		public void setFirst(E first) {
			this.first = first;
		}
	
		public F getSecond() {
			return second;
		}
	
		public void setSecond(F second) {
			this.second = second;
		}
	}

}
