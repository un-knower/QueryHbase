package com.asiainfo.queryhbase.method;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.queryhbase.Constant;
import com.asiainfo.queryhbase.resource.BOSSRecord.ColumnInfo;
import com.asiainfo.queryhbase.util.MysqlJdbcProcess;
import com.asiainfo.queryhbase.util.StringUtil;

public class ColumnMethod {
	public static final Logger logger = LoggerFactory.getLogger("translate");

	/*
	 * 按column_id排序，假如有新增方法，请补在最后
	 * 入口的方法名必须以get_[vc_tip]的格式命名，其余的方法不要求，否则无法调用到
	 */

	public static final String directType[] = {
			    "无方向",
			    "移动终端发起(上行)",
			    "移动终端终止(下行)",
			    "主叫",
			    "被叫", 
			    "无条件及关机呼转主叫",
			    "有条件呼转主叫",
			    "无条件及关机呼转被叫", 
			    "有条件呼转被叫",
			    "网站短信/移动终端发起，E-Mail终止(上行)",
			    "E-Mail发起(上行)",
			    "E-Mail终止(下行)",
				""
			};

	// int_column_id=0
	public static String get_time(ColumnInfo columnInfo) {
		if (columnInfo.len != 14) {
			return null;
		}   

		return String.format("%s-%s %s:%s:%s", 
				columnInfo.rawValue.substring(4, 6),
				columnInfo.rawValue.substring(6, 8),
				columnInfo.rawValue.substring(8, 10),
				columnInfo.rawValue.substring(10, 12),
				columnInfo.rawValue.substring(12, 14));
	}
	
	// int_column_id=1
	public static String get_fix_cycle(ColumnInfo columnInfo) {
		if (columnInfo.len != 16) {
			return null;
		}
		
		return String.format("%s-%s—%s-%s", 
				columnInfo.rawValue.substring(4, 6),
				columnInfo.rawValue.substring(6, 8),
				columnInfo.rawValue.substring(12, 14),
				columnInfo.rawValue.substring(14, 16));
	}

	// int_column_id=2
	public static String get_dis_id(ColumnInfo columnInfo) {
		String special_flag = columnInfo.cdr.substring(89, 91).trim();
		
		if (columnInfo.cbe_type == 3) {
			if (special_flag.compareTo("b1") >= 0 && special_flag.compareTo("b8") <=0 
					|| special_flag.compareTo("c1") >= 0 && special_flag.compareTo("c8") <=0 ) {
				String subsid = columnInfo.cdr.substring(350, 374).trim();
				return translate_gprs_tongfu(subsid, special_flag);
			} else if (special_flag.equals("d2")) {
				return "流量代付";
			} else if (special_flag.equals("d1") || special_flag.equals("d3") || special_flag.equals("d5") || special_flag.equals("d7")) {
				return "流量被代付";
			} else if (special_flag.equals("d8") || special_flag.equals("d4")) {
				return translate_dis_id_real_msisdn(columnInfo.cdr);
			}
		}
		
		// 12-10 新增需求 详设3.3
		if(columnInfo.cbe_type == 1){
			if(special_flag.equals("d3") || special_flag.equals("d7")){
				return "语音被代付";
			} else if (special_flag.equals("d8") || special_flag.equals("d4")) {
				return translate_dis_id_real_msisdn(columnInfo.cdr);
			}
		}
		
		if(columnInfo.cbe_type == 2){
			if(special_flag.equals("d3") || special_flag.equals("d7")){
				return "短彩信被代付";
			} else if (special_flag.equals("d8") || special_flag.equals("d4")) {
				return translate_dis_id_real_msisdn(columnInfo.cdr);
			}
		}

		if(columnInfo.cbe_type == 5){
			if(special_flag.equals("d3") || special_flag.equals("d7")){
				return "WLAN被代付";
			} else if (special_flag.equals("d8") || special_flag.equals("d4")) {
				return translate_dis_id_real_msisdn(columnInfo.cdr);
			}
		}
		
		String bus_subus_type = null;
		String dis_id = columnInfo.rawValue.trim();
		String subject_id = "";

		if (columnInfo.cbe_type != 6) { // 只用于translate_dis_id_special，06均不会走到此方法
			bus_subus_type = columnInfo.cdr.substring(665, 669).trim();
		}
		
		if (dis_id.equals("") && columnInfo.cbe_type != 6) {
			return translate_dis_id_special(bus_subus_type);
		}
		
		// {[sp0,sp1];[sp2,sp3]} 或者 {[sp0,sp1]}
		String[] value_dis_id = null;
		String[] subvalue_dis_id1 = null;
		String[] subvalue_dis_id2 = null;
		String value = "";
		String tmp1_value = "";
		String tmp2_value = "";
		
		if (dis_id.contains(",")) {
			value_dis_id = dis_id.split(";");
			
			if (value_dis_id.length == 2) { // 两个
				subvalue_dis_id1 = value_dis_id[0].split(",");
				subvalue_dis_id2 = value_dis_id[1].split(",");
				
				if (subvalue_dis_id1[0].trim().equals("") && subvalue_dis_id2[0].trim().equals("")) {
					return translate_dis_id_special(bus_subus_type);
				} else {
					tmp1_value = translate_dis_id(subvalue_dis_id1[0].trim()); 
					tmp2_value = translate_dis_id(subvalue_dis_id2[0].trim());
					
					if (tmp1_value.equals("") || tmp2_value.equals("")) {
						value = tmp1_value+tmp2_value;
					} else {
						value = tmp1_value+"|"+tmp2_value;
					}
				}
			} else { // 一个
				subvalue_dis_id1 = value_dis_id[0].split(",");
				
				if (subvalue_dis_id1[0].trim().equals("")) {
					return translate_dis_id_special(bus_subus_type);
				} else {
					value = translate_dis_id(subvalue_dis_id1[0].trim());
				}
			}
		} else {
			if(dis_id.equals("gprsdailylist")){
				return "手机流量日租单";
			}
			
			if (columnInfo.len == 64) {
				if (dis_id.length() > 32) {
					tmp1_value = translate_dis_id(dis_id.substring(0, 32)); 
					tmp2_value = translate_dis_id(dis_id.substring(32));

					if (tmp1_value.equals("") || tmp2_value.equals("")) {
						value = tmp1_value+tmp2_value;
					} else {
						value = tmp1_value+"|"+tmp2_value;
					}
				} else {
					value = translate_dis_id(dis_id);
				}
			} else {
				value = translate_dis_id(dis_id);
			}
			
			if (value.equals("") && columnInfo.cbe_type==6) {
				subject_id = columnInfo.cdr.substring(62, 76).trim();
				if (subject_id.equals("")) {
					return "月租费";
				}
				
				value = translate_subject_id(subject_id); 
				if (value.equals("")) {
					return "月租费";
				}
			}
		}
		
		if (value.equals("")) {
			return null;
		} else {
			return value+IsReversal(columnInfo.cdr);
		}
	}

	public static String translate_gprs_tongfu(String subsid, String special_flag) {
		String servtype = "";
		String tail = "";
		String servinfo = null;

		if (special_flag.compareTo("b1") >= 0 && special_flag.compareTo("b5") <=0 
				|| special_flag.compareTo("c1") >= 0 && special_flag.compareTo("c5") <=0 ) { 
			servtype = "07";
			tail = "业务专属流量统付";
		} else if (special_flag.compareTo("b6") >= 0 && special_flag.compareTo("b8") <=0 
				|| special_flag.compareTo("c6") >= 0 && special_flag.compareTo("c8") <=0 ) {
			servtype = "08";
			tail = "通用流量统付";
		}

		servinfo = Constant.INF_ECDETAILINFO_MAP.get(subsid+MysqlJdbcProcess.SPLITCHAR+servtype);
		if (servinfo != null) {
			return servinfo+tail;	
		} else {	
			logger.warn("Not found in cbep0_inf_ecdetailinfo, subsid="+subsid+",special_flag="+special_flag);
			return null;	
		}
	}
	
	public static String translate_dis_id_special(String bus_subus_type) {
		String pro_name = Constant.SPECIAL_PRODUCT_MAP.get(bus_subus_type);
		
		if (pro_name == null) {
			logger.warn("Not found in cbep0_special_product, bus_subus_type=", bus_subus_type);
		}

		return pro_name;
	}

	public static String translate_dis_id(String vc_product_id) {
		String vc_product_name = Constant.PRODUCT_TRANSLATE_MAP.get(vc_product_id);

		if (vc_product_name == null) {
			vc_product_name = "";
			logger.warn("Not found in cbep0_product_translate, vc_product_id=", vc_product_id);
		}
		return vc_product_name;
	}

	public static String translate_subject_id(String subject_id) {
		String vc_subject_name = Constant.SUBJECT_TRANSLATE_MAP.get(subject_id);

		if (vc_subject_name != null) {
			return vc_subject_name;
		} else {
			logger.warn("Not found in cbep0_subject_translate, vc_subject_id=", subject_id);
			return "";
		}
	}
	
	// BR201611010021
	public static String translate_dis_id_real_msisdn(String cdr) {
		int offset = 0;
		int len = 24;
		
		if(cdr.startsWith("01")){
			offset = 824;
		} else if(cdr.startsWith("02")) {
			offset = 786;
		} else if(cdr.startsWith("03")) {
			offset = 782;
		} else if(cdr.startsWith("04")) {
			offset = 840;
		} else if(cdr.startsWith("05")) {
			offset = 765;
		}
		
		String real_msisdn = cdr.substring(offset, offset+len);
		String tmp = "";
		
		if(real_msisdn.indexOf(" ") == -1) {
			tmp = real_msisdn.substring(real_msisdn.length()-4);
		} else {
			tmp = real_msisdn.split(" ", -1)[0];
			if(tmp.length() > 4){
				tmp = tmp.substring(tmp.length()-4);
			}
		}
		tmp = "主号" + tmp;
		
		return tmp;
	}
	
	public static String IsReversal(String cdr) {
		String fee = null;

		if (cdr.startsWith("02") && cdr.charAt(702) == '2' && cdr.charAt(669) == '7' && cdr.substring(19,21).equals("5D")) {
			
	        if (cdr.substring(164,166).equals("CC")) {
	            fee = cdr.substring(216, 224);
	        } else if(cdr.substring(164,166).equals("DC")) {
	            fee = cdr.substring(224, 232);
	        } else {
	            return "";
	        }
	        
	        if (StringUtil.ParseLong(fee) < 0){
				return "退费";
	        }
	    }
		
		return "";
	}

	// int_column_id=3
	public static String get_fee(ColumnInfo columnInfo) {
		String tmp_fee=null;
		long total_fee = 0;

		if (columnInfo.len == 12) {
			tmp_fee = columnInfo.cdr.substring(columnInfo.offset, columnInfo.offset+12).trim();
			total_fee = StringUtil.ParseLong(tmp_fee);
		} else {
			for (int i = 0; i < columnInfo.len / 8; i++) {
				tmp_fee = columnInfo.cdr.substring(columnInfo.offset + i*8, columnInfo.offset + i*8 + 8).trim();
				total_fee += StringUtil.ParseLong(tmp_fee);
			}
		}

		return String.format("%.2f", total_fee*1.0 / 1000);
	}

	// int_column_id=4
	public static String get_voice_area(ColumnInfo columnInfo) {
		String vc_area;
		String vc_switch_flag;
		String value;

		vc_area = columnInfo.cdr.substring(132, 140).trim();
		vc_switch_flag = columnInfo.rawValue.trim();

		if (vc_area.startsWith("00")) {
			value = translate_international_area(vc_switch_flag);
		} else {
			value = translate_china_area(vc_area);
		}

		if (value.equals("")) {
			value = vc_area;
		}
		return value;
	}
	
	public static String translate_international_area(String vc_switch_flag) {
		String vc_switch_name = Constant.INF_SWITCH_MAP.get(vc_switch_flag);

	    if (vc_switch_name != null) {
	    	return vc_switch_name;
	    } else {
	        logger.warn("Not found in sysp0_Inf_switch, switch_flag="+vc_switch_flag);
	        return vc_switch_flag;
	    }
	}

	public static String translate_china_area(String vc_area) {
		String vc_area_name = Constant.INF_NAT_MAP.get(vc_area);
		if (vc_area_name != null) {
			return vc_area_name;
		} else {
			logger.warn("Not found in ratp0_Inf_nat, vc_area=", vc_area);
			return vc_area;
		}
	}

	// int_column_id=5
	public static String get_voice_call_type(ColumnInfo columnInfo) {
		String direct = "3"; //如果字段不能直接获取，取默认值3
		String bus_name = "";

		if (columnInfo.get_flag == 1) {
			direct = columnInfo.rawValue.trim();
		}

		if (columnInfo.cdr.substring(665, 669).equals("0K0I")) {
			bus_name = "可视";
		} else if (columnInfo.cdr.substring(665, 667).equals("02")) {
			bus_name = "DATA/FAX";
		} else if (columnInfo.cdr.substring(164, 166).equals("AA")) {
	    	bus_name = "IVR";
		}
		
		if (direct.equals("4")) {
			return bus_name+"被叫";
		} else if (direct.equals("5")) {
			return "无条件及不可及呼叫转移主叫";
		} else if (direct.equals("6")) {
			return "有条件呼叫转移主叫";
		} else if (direct.equals("7")) {
			return "无条件及不可及呼叫转移被叫";
		} else if (direct.equals("8")) {
			return "有条件呼叫转移被叫";
		} else {
			return bus_name+"主叫";
		}
	}

	// int_column_id=6
	public static String get_b_subno(ColumnInfo columnInfo) {
		String value = columnInfo.rawValue.trim();
		
		if (columnInfo.get_flag == 1) {
			if (columnInfo.cbe_type == 1) {
	            if ("1/2/3".contains(columnInfo.cdr.substring(705, 706))) {
	            	int flag = 0;
	                if (value.startsWith("86") && value.length() == 13) {
	                	flag = 2;
	                } else if (value.startsWith("0086") && value.length() == 15) {
	                	flag = 4;
	                }
	                value = value.substring(flag);
	            }
	        } else if (columnInfo.cbe_type == 2) {
	        	try {
					if (columnInfo.cdr.substring(21, 32).equals(value.substring(value.length()-11))) {
						value = "-";
					}
	        	} catch (StringIndexOutOfBoundsException e){
	        	}
			}
		}
		return value;
	}

	// int_column_id=7
	public static String get_voice_call_long(ColumnInfo columnInfo) {
		long call_long = 0L;
		long hour = 0;
		long min = 0;
		long sec = 0;

		if (columnInfo.get_flag == 1)  {
			call_long = StringUtil.ParseLong(columnInfo.rawValue.trim());
		}

		if ((hour = call_long / 3600) != 0) {
			call_long -= hour * 3600;
		}
		if ((min = call_long / 60) != 0) {
			call_long -= min * 60;
		}
		sec = call_long;

		if (hour != 0) {
			return String.format("%02d时%02d分%02d秒", hour, min, sec);
		} else if (min != 0) {
			return String.format("%02d分%02d秒", min, sec);
		} else {
			return String.format("%02d秒", sec);
		}
	}

	// int_column_id=8
	public static String get_call_roam_toll_type(ColumnInfo columnInfo) {
		String roam = "0";
		String direct = "3"; 
		String toll = "0";
		String bill_type = null; 		// 2
		String bus_subus_type = "    "; // 4
		String fill_type = null;		// 2
		String carry_type = null; 		// 2
		boolean vpmn = false;
		String value = "";

		if (columnInfo.cdr.charAt(771) == '2') {
			value += "4G+高清语音(VoLTE)";
		} else if (columnInfo.cdr.charAt(771) == '3') {
			value += "4G+高清视频(VoLTE)";
		}
		
		
		if (columnInfo.get_flag == 1) {
			roam = columnInfo.rawValue;
			bill_type = columnInfo.cdr.substring(164, 166);
			bus_subus_type = columnInfo.cdr.substring(665, 669);
			fill_type = columnInfo.cdr.substring(19, 21);
		}

		if ("0P0S".equals(bus_subus_type)) {
			return value+"移动多人通话";
		}
		
		if (columnInfo.cbe_type == 1 || columnInfo.cbe_type == 4) {
			toll = columnInfo.cdr.substring(706, 707);
			carry_type = columnInfo.cdr.substring(707, 709);
		}

		if (columnInfo.cbe_type == 1 || columnInfo.cbe_type == 2 || columnInfo.cbe_type == 4) {
			direct = columnInfo.cdr.substring(702, 703);
		}

		if (columnInfo.cbe_type == 1 && columnInfo.cdr.substring(761, 763).equals("01")) {
			vpmn = true;
		}

		if ("3".equals(direct) && "11".equals(carry_type) && "IB".equals(bill_type)) {
			if ("09/0A".contains(bus_subus_type.substring(0,2))) {
				return value+"集团IP后付费/IP直通车";
			} else {
				return value+"中国移动ip";
			}
		}

		if ("QB".equals(bill_type)) {
			return value+"主叫付费业务";
		} else if ("RB".equals(bill_type)) {
			return value+"被叫付费业务";
		} else if ("3".equals(direct) && "31".equals(carry_type)) {
			return value+"中国电信ip";
		} else if ("3".equals(direct) && "3A".equals(carry_type)) {
			return value+"网通ip";
		} else if (bus_subus_type.charAt(0) =='0' && "0G".equals(bus_subus_type.substring(2))) {
			return value+"主叫(139)";
		} else if ("01/OK".contains(bus_subus_type.substring(0,2)) && "29".equals(bus_subus_type.substring(2))) {
			return value+"代MID付费";
		}
	    

		if("1S".equals(fill_type) && "0QAQ/0QAR".contains(bus_subus_type)) {
			value += "多媒体家庭电话(IMS固话)";
		}

		if ("9".equals(roam)) {
			return value+"港澳台漫游";
		} else if ("4".equals(roam)) {
			return value+"国际漫游";
		}

		if (toll.charAt(0) == 'D') {
			if (vpmn) {
				return value+"VPMN(国际长途)";
			} else {
				return value+"国际长途";
			}
		} else if (toll.charAt(0)>='A' && toll.charAt(0)<='C') {
			if (vpmn) {
				return value+"VPMN(港澳台长途)";
			} else {
				return value+"港澳台长途";
			}
		} else if (toll.charAt(0)>='6' && toll.charAt(0)<='9') {
			if (vpmn) {
				return value+"VPMN(国内长途)";
			} else {
				return value+"国内长途";
			}
		} else {
			if (vpmn) {
				return value+"VPMN(本地)";
			} else {
				return value+"本地";
			}
		}
	}

	// int_column_id=9
	public static String get_sms_area(ColumnInfo columnInfo) {
		String area = columnInfo.cdr.substring(132, 140).trim();
		String vc_switch_flag = columnInfo.rawValue.trim();
		String value=null;

		if (vc_switch_flag.charAt(0) >= '0' && vc_switch_flag.charAt(0) <= '9') {
			return "内地";
	    }

		value = translate_international_area(vc_switch_flag);
		if (value.equals("")) {
			if (area.charAt(0) != '0') {
				area = "0"+area;
			}
			value = translate_china_area(area);
		}

		if (value.equals("")) {
			value = area;
		}

		return value;
	}

	// int_column_id=10
	public static String get_sms_direct(ColumnInfo columnInfo) {
		String direct = "1";

		if (columnInfo.get_flag == 1) {
			direct = columnInfo.rawValue;
		}

		if (direct.charAt(0) == '2') {
			return "接收";
		} else {
			return "发送";
		}
	}

	// int_column_id=11
	public static String get_sms_inf_type(ColumnInfo columnInfo) {
		
		if (columnInfo.get_flag == 1) {
			String bus_type = columnInfo.rawValue;
			if (bus_type.compareTo("51") >= 0 && bus_type.compareTo("55") <= 0) {
				if (columnInfo.cdr.substring(713, 715).equals("04") && columnInfo.cbe_type == 2) {
					return "短信（长）";
				} else {
					return "短信";
				}
			} else if (bus_type.compareTo("56") >= 0 && bus_type.compareTo("59") <= 0) {
				return "彩信";
			}
		}

		return null;
	}

	/*
	 * 为了减少查找bus表的次数，06以外的业务名称get_bus_name、费用类型get_sp_fee_type、使用方式get_sp_use_type 
	 * 总共三个字段名称的查找，用FindBusTranslate来获取
	 */
	// int_column_id=12
	public static String get_bus_name(ColumnInfo columnInfo) {
		if (columnInfo.cbe_type == 6) {
			return translate_pf_acct(columnInfo.cdr.substring(62, 76).trim());
		}
		return null;
	}

	public static String translate_pf_acct(String acctid) {
		String printname = Constant.PF_ACCT_MAP.get(acctid);

		if (printname != null) {
			return printname.trim();
		} else {
			logger.warn("Not found in cbep0_pf_acct, acctid=", acctid);
			return acctid;
		}
	}

	// int_column_id=13
	public static String get_net_area(ColumnInfo columnInfo) {
		String vc_switch_flag = columnInfo.rawValue.trim();
		String value;

		if (vc_switch_flag.compareTo("00") == 0) {
			return "本地";
		}
		
		if (vc_switch_flag.compareTo("99") > 0) {
			value = translate_international_area(vc_switch_flag);
		} else {
			value = translate_china_prov(vc_switch_flag);
		}
		
		if (value==null || value.equals("")) {
			value = vc_switch_flag;
		}

		return value;
	}

	public static String translate_china_prov(String vc_switch_flag) {
		String vc_prov_name = Constant.PROV_TRANSLATE_MAP.get(vc_switch_flag);
		
		if (vc_prov_name != null) {
			return vc_prov_name;
	    } else {
			return "广东省";
		}
	}

	// int_column_id=14
	public static String get_net_use_type(ColumnInfo columnInfo) {
		/*
		 * 61都是WLAN
		 * 03格式是GPRS的，直接从APN名称中获取
		 * 05格式都是61格式的WLAN，如果是非61的，这里返回的是空
		 */
		String value = "";
		if (columnInfo.cdr.substring(665, 667).equals("61")) {
			value = "WLAN";
		} else {
			if (columnInfo.cdr.charAt(991) == '3') {
				value = "3G";
			} else if (columnInfo.cdr.charAt(991) == '4') {
				value = "4G";
			} else {
				value = "2G";
			}
			
			if (columnInfo.cdr.substring(665, 669).equals("3017")){
				value += "(和飞信)";
			}
		}

		return value;
	}

	// int_column_id=15
	public static String get_net_long(ColumnInfo columnInfo) {
		long net_long = 0;
		long hour = 0;
		long min = 0;
		long sec = 0;

		if (columnInfo.get_flag == 1) {
			net_long = StringUtil.ParseLong(columnInfo.rawValue.trim());
	    }

		if ((hour = net_long / 3600) != 0) {
			net_long -= hour * 3600;
		}

		if ((min = net_long / 60) != 0) {
			net_long -= min * 60;
		}

		sec = net_long;
		
		return String.format("%02d:%02d:%02d", hour, min, sec);
	}

	// int_column_id=16
	public static String get_net_usage(ColumnInfo columnInfo) {
		long net_usage = 0L;
		long M_byte = 0L;
		long K_byte = 0L;
		String billtype = columnInfo.cdr.substring(164, 166);
		String filetype = columnInfo.cdr.substring(19, 21);

		// 累加上下行流量
		// 只配置上行流量的位置和长度，下行是上行的下一个字段并且长度一致，直接根据上行提取
		if (columnInfo.get_flag == 1) {
			if ("WC".contains(billtype) && filetype.equals("28") || "HC".contains(billtype)) {
				net_usage += (StringUtil.ParseLong(columnInfo.rawValue.trim()) + 1023) / 1024 * 1024;
			} else {
				net_usage += StringUtil.ParseLong(columnInfo.rawValue.trim()) / 1024 * 1024;
			}

			if ("WC".contains(billtype) && filetype.equals("28") || "HC".contains(billtype)) {
				net_usage += (StringUtil.ParseLong(columnInfo.cdr.substring(columnInfo.offset+columnInfo.len, columnInfo.offset+columnInfo.len*2)) + 1023) / 1024 * 1024;
			} else {
				net_usage += StringUtil.ParseLong(columnInfo.cdr.substring(columnInfo.offset+columnInfo.len, columnInfo.offset+columnInfo.len*2)) / 1024 * 1024;
			}
		}

		if ((M_byte = net_usage / (1024*1024)) != 0) {
			net_usage -= M_byte * 1024 * 1024;
		}

		K_byte = net_usage / 1024;
		
		if (billtype.equals("HC") && M_byte == 0 && K_byte == 0) {
			return String.format("%.3fKB", net_usage / 1024.0);
		} else {
			return String.format("%dMB%dKB", M_byte, K_byte);
		}
	}

	// int_column_id=17
	public static String get_sp_use_type(ColumnInfo columnInfo) {
		if (columnInfo.cbe_type==6) {
			return "-";
		}
		return null;
	}

	// int_column_id=18
	public static String get_sp_port(ColumnInfo columnInfo) {
		if (columnInfo.cbe_type==6) {
			return "-";
		} else if (columnInfo.get_flag == 1) {
			return columnInfo.rawValue.trim();
		} else {
			return null;
		}
	}

	// int_column_id=19
	public static String get_sp_name(ColumnInfo columnInfo) {
		return null;
	}

	// int_column_id=20
	public static String get_sp_code(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return columnInfo.rawValue.trim();
		} else {
			return null;
		}
	}

	// int_column_id=21
	public static String get_sp_fee_type(ColumnInfo columnInfo) {
		return null;
	}

	// int_column_id=22
	public static String get_other_fee_type(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return translate_other_fee_type(columnInfo.rawValue.trim());
		}
		
		return null;
	}

	public static String translate_other_fee_type(String vc_feetype_code) {
		String vc_feetype_name = Constant.INF_FEETYPE_TRANSLATE_MAP.get(vc_feetype_code);
		
		if (vc_feetype_name != null) {
			return vc_feetype_name;
		} else {
	        logger.warn("Not found in cbep0_inf_feetype_translate, vc_feetype_code="+vc_feetype_code);
			return vc_feetype_code;
		}
	    
	}
	
	// int_column_id=23
	public static String get_dis_or_derate(ColumnInfo columnInfo) {
		if (columnInfo.cbe_type == 1 || columnInfo.cbe_type == 2 || columnInfo.cbe_type == 4) {
			return "-";
		}

		int flag = 0;
		String billtype = columnInfo.cdr.substring(164, 166).trim();
		String mob_fee = columnInfo.cdr.substring(208, 216).trim();
		String toll_fee = columnInfo.cdr.substring(216, 224).trim();
		String inf_fee = columnInfo.cdr.substring(224, 232).trim();
		String filetype = columnInfo.cdr.substring(19, 21).trim();
		String dur_vol_flag = columnInfo.cdr.substring(761, 762).trim(); // 05
		long sum = StringUtil.ParseLong(mob_fee)+StringUtil.ParseLong(toll_fee)+StringUtil.ParseLong(inf_fee);
		
		if (columnInfo.cbe_type == 3) {
			String dis_dura = columnInfo.cdr.substring(707, 719).trim();
			long disdura = StringUtil.ParseLong(dis_dura);
			
			if (billtype.equals("GC")) {
				if (disdura == 0) {
					flag = 3;
				} else if (disdura > 0 && sum > 0) {
					flag = 2;
				}
			} else if (billtype.equals("HC")) {
				flag = 4;
			}
		} else if (columnInfo.cbe_type == 5) {
			String dis_dura = columnInfo.cdr.substring(707, 714).trim();
			if (dur_vol_flag.equals("2")) {
				dis_dura = Long.toString(StringUtil.ParseLong(dis_dura)/1024);
			}
			
			if (billtype.equals("WC") && "25/26/2B/2U".contains(filetype)) {
				if (StringUtil.ParseLong(dis_dura) == 0) {
					flag = 3; // 无扣费为不涉及免费资源，有扣费为套餐外
				} else if (StringUtil.ParseLong(dis_dura) > 0 && sum > 0) {
					flag = 2;
				}
			} else if (billtype.equals("WC") && filetype.equals("28")) {
				flag = 4;
			}
		}
		
		if (flag == 4) {
			return "-";
		} else if (flag == 3) {
			return "套餐外";
		} else if (flag == 2) {
			return "部分套餐外";
		} else {
			return "套餐内";
		}
	}

	// int_column_id=24
	public static String get_net_ssid(ColumnInfo columnInfo) {
	    if (columnInfo.cdr.substring(665,667).equals("61")) {
			String ssid = columnInfo.cdr.substring(1000, 1003).trim();
			if (ssid.length() == 0) {
				return "";
			} else if (ssid.equals("000")) {
				return "";
			} else if (ssid.equals("001")) {
				return "OTHER";
			} else {
				return translate_ssid_num(ssid.trim());
			}
		} else {
			return columnInfo.rawValue.trim();
		}
	}

	public static String translate_ssid_num(String ssid) {
		String ssidname = Constant.HSC_SSID_TRANS_MAP.get(ssid);

		if (ssidname != null) {
			return ssidname;
		} else {
			logger.warn("Not found in cbep0_hsc_ssid_trans, ssid=", ssid);
			return ssid;
		}
	}

	// int_column_id=25
	public static String get_context_id(ColumnInfo columnInfo) {
		return columnInfo.rawValue.trim();
	}

	// int_column_id=26
	public static String  get_channel_id(ColumnInfo columnInfo) {
		return columnInfo.rawValue.trim();
	}

	// int_column_id=27
	public static String get_group_subno(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return columnInfo.rawValue.trim();
		}

		return null;
	}

	// int_column_id=28
	public static String get_real_subno(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return columnInfo.rawValue.trim();
		}

		return null;
	}

	// int_column_id=29
	public static String get_visit_area(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return columnInfo.rawValue.trim();
		}

		return null;
	}

	// int_column_id=30
	public static String get_group_time(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%.2s年%.2s月%.2s日 %.2s时%.2s分%.2s秒", 
					columnInfo.rawValue.substring(2, 4),
					columnInfo.rawValue.substring(4, 6),
					columnInfo.rawValue.substring(6, 8),
					columnInfo.rawValue.substring(8, 10),
					columnInfo.rawValue.substring(10, 12),
					columnInfo.rawValue.substring(12, 14));
		}
		
		return null;
	}

	// int_column_id=31
	public static String get_group_direct(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			String direct = columnInfo.rawValue.trim();

			if (direct.charAt(0) == 'A') {
				return directType[10];
			} else if (direct.charAt(0) == 'B') {
				return directType[11];
			} else if (direct.charAt(0) >= '0' && direct.charAt(0) <= '9') {
				return directType[StringUtil.ParseInt(direct)];
			} else {
				return "";
			}
		}
		
		return null;
	}

	// int_column_id=32
	public static String get_duration(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%011d", StringUtil.ParseLong(columnInfo.rawValue.trim()));
		}
		
		return null;
	}

	// int_column_id=33
	public static String get_bill_type(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			String value;
			if ((value = group_trans_bill_type_first(columnInfo.cdr)) != null) {
				return value;
			}

			return group_trans_bill_type_second(columnInfo.rawValue.trim());
		}
		
		return null;
	}
	
	public static String group_trans_bill_type_first(String cdr) {
		
		String bill_type=cdr.substring(188,190);
		String bustype=cdr.substring(689,693);
		String toll_type=" ";	// 免得每个分支都判断一次
		String b_operator="";
		String b_usr_type="";

	    if (cdr.startsWith("11")){
	        toll_type=cdr.substring(730,731);
	        b_operator=cdr.substring(733,735);
	        b_usr_type=cdr.substring(738,740);
	    }
	    
	    if (bill_type.equals("AB")) {
	        if (bustype.equals("5095")) {
	            return "固定月租费";
	        } else if (bustype.equals("5096")) {
	            return "增值业务固定费";
	        }
	    }
	    
		if (cdr.startsWith("12")) {
			return null;
		}
		
	    if (toll_type.charAt(0) >= 'A' && toll_type.charAt(0) <= 'D') {
	        return "国际";
	    }
	    
	    if (toll_type.charAt(0) >= '1' && toll_type.charAt(0) < '6') {
	        if (!("01/02".contains(b_operator) || "A2/A3".contains(b_usr_type))) {
	            return "本地固话";
	        } else {
	            return "本地手机";
	        }
	    }
	    
	 	if (toll_type.charAt(0) >= '6' && toll_type.charAt(0) <= '9') {
	        if (!("01/02".contains(b_operator) || "A2/A3".contains(b_usr_type))) {
	            return "国内固话";
	        } else {
	            return "国内手机";
	        }
	    }
	 	
		return null;	
	}

	public static String group_trans_bill_type_second(String vc_bill_type) {
		String vc_trans_name = Constant.GROUP_TRANSLATE_MAP.get(vc_bill_type.substring(0,2));

		if (vc_trans_name != null) {
			return vc_trans_name;
		} else {
			logger.warn("Not found in datp0_group_translate, vc_bill_type="+vc_bill_type);
			return null;
		}
	}

	// int_column_id=34
	public static String get_mob_fee(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%.2f", StringUtil.ParseLong(columnInfo.rawValue.trim())/1000.0);
		}
		
		return null;
	}

	// int_column_id=35
	public static String get_toll_fee(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%.2f", StringUtil.ParseLong(columnInfo.rawValue.trim())/1000.0);
		}
		
		return null;
	}

	// int_column_id=36
	public static String get_inf_fee(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%.2f", StringUtil.ParseLong(columnInfo.rawValue.trim())/1000.0);
		}
		
		return null;
	}

	// int_column_id=37
	public static String get_dis_fee(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%.2f", StringUtil.ParseLong(columnInfo.rawValue.trim())/1000.0);
		}
		
		return null;
	}

	// int_column_id=38
	public static String get_dis_dura(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
			return String.format("%07d", StringUtil.ParseLong(columnInfo.rawValue.trim()));
		}
		
		return null;
	}


	// int_column_id=39
	public static String get_bus_translate(ColumnInfo columnInfo) {
		if (columnInfo.get_flag == 1) {
		    String value;
		    if ((value=bus_translate(columnInfo.cdr.substring(columnInfo.offset,columnInfo.offset+20).trim(), 
		    		columnInfo.cdr.substring(columnInfo.offset+20,columnInfo.offset+50).trim())) == null) {
		    	return "-";
		    } else {
		    	return value;
		    }
		}
		
		return null;
	}

	public static String bus_translate(String vc_sp_code, String vc_buscode) {
		String spcodeFromTable = null;
		ArrayList<String> value_Bus_Translate = Constant.BUS_TRANSLATE_MAP.get(vc_buscode);
		String[] subvalue_Bus_Translate = null;

		if (value_Bus_Translate == null) { // 没有数据
	        logger.warn("Not found in cbep0_bus_translate, vc_buscode=", vc_buscode);
			return null;
		} else {
			for (String s : value_Bus_Translate) {
				subvalue_Bus_Translate = s.split(MysqlJdbcProcess.SPLITCHAR);
				spcodeFromTable = subvalue_Bus_Translate[0].trim();

				if (spcodeFromTable.equals(vc_sp_code)) {
					return subvalue_Bus_Translate[1];
				} 
			}
			
		}
		
		return null;
	}
	
	// int_column_id=40
	public static String get_net_visit_address(ColumnInfo columnInfo) {
		
		if (columnInfo.cdr.substring(665, 667).equals("61")) {
			return "-";
		} else {
			String net_address = Constant.NET_ADDRESS_MAP.get(columnInfo.rawValue.trim());

			if (net_address != null) {
				return net_address;
			} else {
				logger.warn("Not found in NET_ADDRESS, service_code="+columnInfo.rawValue.trim());
				return "-";
			}
		}
	}

	// int_column_id=41
	public static String get_up_usage(ColumnInfo columnInfo) {
		long usage = 0;
		long M_usage = 0;
		long K_usage = 0;

		if (columnInfo.cdr.substring(164,166).equals("WC") && columnInfo.cdr.substring(19,21).equals("28")
				|| columnInfo.cbe_type==3 && columnInfo.cdr.substring(164,166).equals("HC")){
			usage = StringUtil.ParseLong(columnInfo.rawValue.trim()) + 1023;
		} else {
			usage = StringUtil.ParseLong(columnInfo.rawValue.trim());
		}

		if ((M_usage = usage / (1024*1024)) != 0) {
			usage -= M_usage * 1024 * 1024;
		}
		K_usage = usage / 1024;

		return String.format("%dMB%dKB", M_usage, K_usage);
	}

	// int_column_id=42
	public static String get_down_usage(ColumnInfo columnInfo) {
		long usage = 0;
		long M_usage = 0;
		long K_usage = 0;
		if (columnInfo.cdr.substring(164,166).equals("WC") && columnInfo.cdr.substring(19,21).equals("28")
				|| columnInfo.cbe_type==3 && columnInfo.cdr.substring(164,166).equals("HC")) {
			usage = StringUtil.ParseLong(columnInfo.rawValue.trim()) + 1023;
		} else {
			usage = StringUtil.ParseLong(columnInfo.rawValue.trim());
		}

		if ((M_usage = usage / (1024*1024)) != 0) {
			usage -= M_usage * 1024 * 1024;
		}
		K_usage = usage / 1024;

		return String.format("%dMB%dKB", M_usage, K_usage);
	}

	// int_column_id=43
	public static String get_price(ColumnInfo columnInfo) {
		String unitcode = columnInfo.rawValue.charAt(columnInfo.rawValue.length()-1) + "";
		String unit_name = Constant.INF_PRICE_MAP.get(unitcode);
		
		if (unit_name != null) {
			return String.format("%.2f%s", StringUtil.ParseLong(columnInfo.rawValue) / 1000.0, unit_name);
		} else {
			logger.warn("Not found in cbep0_inf_price, unitcode=", unitcode);
			return null;
		}
	}

	// int_column_id=44
	public static String get_rate_type(ColumnInfo columnInfo) {
		String dua_vol_flag = columnInfo.rawValue.trim();
		
		if (columnInfo.cbe_type==5) {
			if (dua_vol_flag.charAt(0)=='1')
				return "时长";
			if (dua_vol_flag.charAt(0)=='2')
				return "流量";
		}
		
		if (columnInfo.cbe_type==3) {
			if (dua_vol_flag.charAt(0)=='0')
				return "时长";
			if (dua_vol_flag.charAt(0)=='1')
				return "流量";
		}
		
		if (dua_vol_flag.charAt(0)=='a')
			return "闲时时长";
		
		if (dua_vol_flag.charAt(0)=='b')
			return "闲时流量";
		
		return null;
	}

	// int_column_id=45
	public static String get_calling_cellid(ColumnInfo columnInfo) {  // 计费方小区编码 873 8 信安需求 后4位
		if (columnInfo.len != 4) {
			return null;
		}
		return columnInfo.rawValue;
	}

	// int_column_id=46
	public static String get_calling_lac(ColumnInfo columnInfo) { // 计费方位置区编码 869 4
		if (columnInfo.len != 4) {
			return null;
		}
		return columnInfo.rawValue;
	}

	// int_column_id=47
	public static String get_imsi(ColumnInfo columnInfo) {
		if (columnInfo.len != 15) {
			return null;
		}
		return columnInfo.rawValue;
	}

	// int_column_id=48
	public static String get_imei(ColumnInfo columnInfo) { // IMEI码  60 15 信安需求 前14位
		if (columnInfo.len != 14) {
			return null;
		}
		return columnInfo.rawValue;
	}

	/* int_column_id=49 废弃
	public static String get_real_msisdn(ColumnInfo columnInfo) {
		if (columnInfo.len != 24) {
			return null;
		}
		return columnInfo.rawValue;
	} */
	
	// int_column_id=50
	public static String get_bus_code(ColumnInfo columnInfo) {
		if (columnInfo.len != 15) {
			return null;
		}
		return columnInfo.rawValue;
	}

	// int_column_id=51
	public static String get_subbus_type(ColumnInfo columnInfo) {
		if (columnInfo.len != 2) {
			return null;
		}
		return columnInfo.rawValue;
	}

	// int_column_id=52
	public static String get_time_year(ColumnInfo columnInfo) {
		if (columnInfo.len != 14) {
			return null;
		}
		
		return String.format("%s-%s-%s %s:%s:%s", 
				columnInfo.rawValue.substring(0, 4),
				columnInfo.rawValue.substring(4, 6),
				columnInfo.rawValue.substring(6, 8),
				columnInfo.rawValue.substring(8, 10),
				columnInfo.rawValue.substring(10, 12),
				columnInfo.rawValue.substring(12, 14));
	}

	// int_column_id=53
	public static String get_voice_call_long_s(ColumnInfo columnInfo) {
		return String.format("%d", StringUtil.ParseLong(columnInfo.rawValue.trim()));
	}

	// int_column_id=54
	public static String get_b_subno_fill(ColumnInfo columnInfo){
		if (columnInfo.get_flag == 1) {
			return columnInfo.rawValue.trim();
		}
		return null;
	}
	
	// int_column_id=55
	public static String get_discount_lastmon(ColumnInfo columnInfo){
		return String.format("%.2fMB", StringUtil.ParseDouble(columnInfo.rawValue)/1024);
	}
}
