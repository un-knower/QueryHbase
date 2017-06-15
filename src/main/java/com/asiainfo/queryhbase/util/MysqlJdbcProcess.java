package com.asiainfo.queryhbase.util;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.queryhbase.Constant;
import com.asiainfo.queryhbase.Constant.Sysp1InfMsi;
import com.asiainfo.queryhbase.Constant.Sysp1InfPbs;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;


public class MysqlJdbcProcess {
	
	public static final Logger logger = LoggerFactory.getLogger(MysqlJdbcProcess.class);
	public static Connection conn = null;
	public static String DBURL = "jdbc:mysql://172.16.1.12:3306/paas"; 
	public static String DBNAME = "test";
	public static String DBPWD = "123456";
	public static String DBFILE_DIR = "D:/TranslateData";
	public static String SPLITCHAR = new String((char) 1 +"");
	
	//目录和后缀
	public static final String CURR_DIR = "curr";
	public static final String BAK_DIR = "backup";
	public static final String CURR_SUFFIX = ".curr";
	public static final String BAK_SUFFIX = ".bak";
	
	//文件名
	public static final String CBEP0_INF_QUERY_FILENAME = "cbep0_inf_query";
	public static final String CBEP0_INF_MOD_FILENAME = "cbep0_inf_mod";
	public static final String CBEP0_INF_MOD_COLUMN_FILENAME = "cbep0_inf_mod_column";
	public static final String CBEP0_INF_COLUMN_FILENAME = "cbep0_inf_column";
	public static final String HENMS_TRAN_FILENAME = "HENMS_Tran";
	public static final String RATP0_INF_NAT_FILENAME = "ratp0_Inf_nat";
	public static final String CBEP0_CBP_BILLACCTNEW_AND_CBEP0_CBP_BILLITEMNEW_FILENAME = "cbep0_cbp_billacctnew#cbep0_cbp_billitemnew";
	public static final String CBEP0_BUS_TRANSLATE_FILENAME = "cbep0_bus_translate";
	public static final String DATP0_IB_MM_CONTENT_INFO_FILENAME = "datp0_ib_mm_content_info";
	public static final String DATP0_IB_12582_CONTENTINFO_FILENAME = "datp0_ib_12582_contentinfo";
	public static final String RPTP0_INF_BUS_AND_RPTP0_INF_C2N_FILENAME = "rptp0_inf_bus#rptp0_inf_c2n";
	public static final String DATP0_IB_GAME_CONTENT_INFO_FILENAME = "datp0_ib_game_content_info";
	public static final String CBEP0_INF_ECDETAILINFO_FILENAME = "cbep0_inf_ecdetailinfo";
	public static final String CBEP0_SPECIAL_PRODUCT_FILENAME = "cbep0_special_product";
	public static final String CBEP0_PRODUCT_TRANSLATE_FILENAME = "cbep0_product_translate";
	public static final String CBEP0_SUBJECT_TRANSLATE_FILENAME = "cbep0_subject_translate";
	public static final String SYSP0_INF_SWITCH_FILENAME = "sysp0_Inf_switch";
	public static final String CBEP0_PF_ACCT_FILENAME = "cbep0_pf_acct";
	public static final String CBEP0_PROV_TRANSLATE_FILENAME = "cbep0_prov_translate";
	public static final String CBEP0_INF_FEETYPE_TRANSLATE_FILENAME = "cbep0_inf_feetype_translate";
	public static final String CBEP0_HSC_SSID_TRANS_FILENAME = "cbep0_hsc_ssid_trans";
	public static final String DATP0_GROUP_TRANSLATE_FILENAME = "datp0_group_translate";
	public static final String CBEP0_NET_ADDRESS_FILENAME = "cbep0_net_address";
	public static final String CBEP0_INF_PRICE_FILENAME = "cbep0_inf_price";
	public static final String SYSP1_INF_MSI_FILENAME = "sysp1_inf_msi";
	public static final String SYSP1_INF_PBS_FILENAME = "sysp1_inf_pbs";
	
	//字段长度
	public static final int CBEP0_INF_QUERY_LEN = 3;
	public static final int CBEP0_INF_MOD_LEN = 3;
	public static final int CBEP0_INF_MOD_COLUMN_LEN = 4;
	public static final int CBEP0_INF_COLUMN_LEN = 5;
	public static final int HENMS_TRAN_LEN = 3;
	public static final int RATP0_INF_NAT_LEN = 2;
	public static final int CBEP0_CBP_BILLACCTNEW_AND_CBEP0_CBP_BILLITEMNEW_LEN = 2;
	public static final int CBEP0_BUS_TRANSLATE_LEN = 7;
	public static final int DATP0_IB_MM_CONTENT_INFO_LEN = 6;
	public static final int DATP0_IB_12582_CONTENTINFO_LEN = 6;
	public static final int RPTP0_INF_BUS_AND_RPTP0_INF_C2N_FILENAME_LEN = 3;
	public static final int DATP0_IB_GAME_CONTENT_INFO_LEN = 4;
	public static final int CBEP0_INF_ECDETAILINFO_LEN = 3;
	public static final int CBEP0_SPECIAL_PRODUCT_LEN = 2;
	public static final int CBEP0_PRODUCT_TRANSLATE_LEN = 2;
	public static final int CBEP0_SUBJECT_TRANSLATE_LEN = 2;
	public static final int SYSP0_INF_SWITCH_LEN = 2;
	public static final int CBEP0_PF_ACCT_LEN = 2;
	public static final int CBEP0_PROV_TRANSLATE_LEN = 2;
	public static final int CBEP0_INF_FEETYPE_TRANSLATE_LEN = 2;
	public static final int CBEP0_HSC_SSID_TRANS_LEN = 2;
	public static final int DATP0_GROUP_TRANSLATE_LEN = 2;
	public static final int CBEP0_NET_ADDRESS_LEN = 2;
	public static final int CBEP0_INF_PRICE_LEN = 2;
	public static final int SYSP1_INF_MSI_LEN = 14;
	public static final int SYSP1_INF_PBS_LEN = 13;

	public static void loadFile() throws IOException, SQLException {
		logger.info("* load file begin");
		LOAD_INF_QUERY_FILE();
		LOAD_INF_MOD_FILE();
		LOAD_INF_MOD_COLUMN_FILE();
		LOAD_INF_COLUMN_FILE();
		LOAD_INF_HENMS_TRAN_FILE();
		LOAD_INF_NAT_FILE();
		LOAD_CBP_BILLACCTNEW_FILE();
		LOAD_BUS_TRANSLATE_FILE();
		LOAD_IB_MM_CONTENT_INFO_FILE();
		LOAD_IB_12582_CONTENTINFO_FILE();
		LOAD_INF_BUS_FILE();
		LOAD_IB_GAME_CONTENT_INFO_FILE();
		LOAD_INF_ECDETAILINFO_FILE();
		LOAD_SPECIAL_PRODUCT_FILE();
		LOAD_PRODUCT_TRANSLATE_FILE();
		LOAD_SUBJECT_TRANSLATE_FILE();
		LOAD_INF_SWITCH_FILE();
		LOAD_PF_ACCT_FILE();
		LOAD_PROV_TRANSLATE_FILE();
		LOAD_INF_FEETYPE_TRANSLATE_FILE();
		LOAD_HSC_SSID_TRANS_FILE();
		LOAD_GROUP_TRANSLATE_FILE();
		LOAD_NET_ADDRESS_FILE();
		LOAD_INF_PRICE_FILE();
		LOAD_INF_MSI_FILE();
		LOAD_INF_PBS_FILE();
		logger.info("* load file end");
	}
	
	public static void loadMemory() throws IOException {
		logger.info("* load memory begin");
		Constant.INF_QUERY_MAP = GET_INF_QUERY_MAP();
		Constant.INF_MOD_MAP = GET_INF_MOD_MAP();
		Constant.INF_MOD_COLUMN_MAP = GET_INF_MOD_COLUMN_MAP();
		Constant.INF_COLUMN_MAP = GET_INF_COLUMN_MAP();
		Constant.INF_HENMS_TRAN_MAP = GET_INF_HENMS_TRAN_MAP();
		Constant.INF_NAT_MAP = GET_INF_NAT_MAP();
		Constant.CBP_BILLACCTNEW_MAP = GET_CBP_BILLACCTNEW_MAP();
		Constant.BUS_TRANSLATE_MAP = GET_BUS_TRANSLATE_MAP();
		Constant.IB_MM_CONTENT_INFO_MAP = GET_IB_MM_CONTENT_INFO_MAP();
		Constant.IB_12582_CONTENTINFO_MAP = GET_IB_12582_CONTENTINFO_MAP();
		Constant.INF_BUS_MAP = GET_INF_BUS_MAP();
		Constant.IB_GAME_CONTENT_INFO_MAP = GET_IB_GAME_CONTENT_INFO_MAP();
		Constant.INF_ECDETAILINFO_MAP = GET_INF_ECDETAILINFO_MAP();
		Constant.SPECIAL_PRODUCT_MAP = GET_SPECIAL_PRODUCT_MAP();
		Constant.PRODUCT_TRANSLATE_MAP = GET_PRODUCT_TRANSLATE_MAP();
		Constant.SUBJECT_TRANSLATE_MAP = GET_SUBJECT_TRANSLATE_MAP();
		Constant.INF_SWITCH_MAP = GET_INF_SWITCH_MAP();
		Constant.PF_ACCT_MAP = GET_PF_ACCT_MAP();
		Constant.PROV_TRANSLATE_MAP = GET_PROV_TRANSLATE_MAP();
		Constant.INF_FEETYPE_TRANSLATE_MAP = GET_INF_FEETYPE_TRANSLATE_MAP();
		Constant.HSC_SSID_TRANS_MAP = GET_HSC_SSID_TRANS_MAP();
		Constant.GROUP_TRANSLATE_MAP = GET_GROUP_TRANSLATE_MAP();
		Constant.NET_ADDRESS_MAP = GET_NET_ADDRESS_MAP();
		Constant.INF_PRICE_MAP = GET_INF_PRICE_MAP();
		Constant.INF_MSI_VEC = GET_INF_MSI_VEC();
		Constant.INF_PBS_VEC = GET_INF_PBS_VEC();
		logger.info("* load memory end");
	}
	
	/*
	 * cbep0_inf_query
	 */
	public static void LOAD_INF_QUERY_FILE() throws SQLException, IOException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_QUERY_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select int_querytype, int_cdrtype, int_modelid from cbep0_inf_query";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(result.getInt(1) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(result.getInt(3));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<Integer, HashMap<String, Integer>> GET_INF_QUERY_MAP() throws IOException {
		HashMap<Integer, HashMap<String, Integer>> map = new HashMap<Integer, HashMap<String, Integer>>();
		HashMap<String, Integer> sub_map = null;
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_QUERY_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<int_querytype, map<int_querytype#int_cdrtype, int_modelid>>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_QUERY_LEN) {
				continue;
			}
			
			//0--int_querytype, 1--int_cdrtype, 2--int_modelid
			int int_querytype = StringUtil.ParseInt(strArr[0]);
			String int_cdrtype = strArr[1];
			int int_modelid = StringUtil.ParseInt(strArr[2]);
			
			sub_map = map.get(int_querytype);
			if (sub_map == null) {
				sub_map = new HashMap<String, Integer>();
				map.put(int_querytype, sub_map);
			}
			String[] value_int_cdrtype = int_cdrtype.split(",");
			for (String str : value_int_cdrtype) {
				sub_map.put(int_querytype + SPLITCHAR + str, int_modelid);
			}
		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_inf_mod
	 */
	public static void LOAD_INF_MOD_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_MOD_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select int_mod_id, vc_delimiter, vc_head from cbep0_inf_mod";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(result.getInt(1) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)));
			bw.write(sb.toString());
			bw.newLine();
		}
		bw.flush();
		
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<Integer, String> GET_INF_MOD_MAP() throws IOException {
		HashMap<Integer, String> map = new HashMap<Integer, String>();

		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_MOD_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<int_mod_id, vc_delimiter#vc_head>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_MOD_LEN) {
				continue;
			}
			
			//0--int_mod_id, 1--vc_delimiter, 2--vc_head
			int int_mod_id = StringUtil.ParseInt(strArr[0]);
			String vc_delimiter = strArr[1];
			String vc_head = strArr[2];
			
			map.put(int_mod_id, vc_delimiter + SPLITCHAR + vc_head);
		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_inf_mod_column
	 */
	public static void LOAD_INF_MOD_COLUMN_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_MOD_COLUMN_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select int_mod_id, int_seq_id, int_column_id, vc_tip from cbep0_inf_mod_column order by int_mod_id, int_seq_id";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(result.getInt(1) + SPLITCHAR);
			sb.append(result.getInt(2) + SPLITCHAR);
			sb.append(result.getInt(3) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(4)));
			bw.write(sb.toString());
			bw.newLine();
		}
		bw.flush();//刷新数据到文件
		
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<Integer, ArrayList<String>> GET_INF_MOD_COLUMN_MAP() throws IOException {
		HashMap<Integer, ArrayList<String>> map = new HashMap<Integer, ArrayList<String>>();
		ArrayList<String> sub_List = null;
		int tmp_modid = -1;

		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_MOD_COLUMN_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<int_column_id, map<int_column_id#int_cbe_type, int_get_flag#int_offset#int_len>>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_MOD_COLUMN_LEN) {
				continue;
			}
			
			//0--int_mod_id, 1--int_seq_id, 2--int_column_id, 3--vc_tip
			int int_mod_id = StringUtil.ParseInt(strArr[0]);
			int int_seq_id = StringUtil.ParseInt(strArr[1]);
			int int_column_id = StringUtil.ParseInt(strArr[2]);
			String vc_tip = strArr[3];
			
			if (tmp_modid != int_mod_id) {
				tmp_modid = int_mod_id;
				sub_List = new ArrayList<String>();
				map.put(tmp_modid, sub_List);
			}
			sub_List.add(int_seq_id + SPLITCHAR + int_column_id + SPLITCHAR + vc_tip);
		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_inf_column
	 */
	public static void LOAD_INF_COLUMN_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_COLUMN_FILENAME + CURR_SUFFIX;//输出文件的全路径
		StringBuffer sb = new StringBuffer();
		
		String sql = "select int_column_id, int_cbe_type, int_get_flag, int_offset, int_len from cbep0_inf_column";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(result.getInt(1) + SPLITCHAR);
			sb.append(result.getInt(2) + SPLITCHAR);
			sb.append(result.getInt(3) + SPLITCHAR);
			sb.append(result.getInt(4) + SPLITCHAR);
			sb.append(result.getInt(5));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<Integer, HashMap<String, String>> GET_INF_COLUMN_MAP() throws IOException {
		HashMap<Integer, HashMap<String, String>> map = new HashMap<Integer, HashMap<String, String>>();
		HashMap<String, String> sub_map = null;
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_COLUMN_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<int_column_id, map<int_column_id#int_cbe_type, int_get_flag#int_offset#int_len>>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_COLUMN_LEN) {
				continue;
			}
			
			//0--int_column_id, 1--int_cbe_type, 2--int_get_flag, 3--int_offset, 4--int_len
			int int_column_id = StringUtil.ParseInt(strArr[0]);
			int int_cbe_type = StringUtil.ParseInt(strArr[1]);
			int int_get_flag = StringUtil.ParseInt(strArr[2]);
			int int_offset = StringUtil.ParseInt(strArr[3]);
			int int_len = StringUtil.ParseInt(strArr[4]);
			
			sub_map = map.get(int_column_id);
			if (sub_map == null){
				sub_map = new HashMap<String, String>();
				sub_map.put(int_column_id + SPLITCHAR + int_cbe_type, int_get_flag + SPLITCHAR + int_offset + SPLITCHAR + int_len);
				map.put(int_column_id, sub_map);
			} else {
				sub_map.put(int_column_id + SPLITCHAR + int_cbe_type, int_get_flag + SPLITCHAR + int_offset + SPLITCHAR + int_len);
			}

		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * HENMS_Tran
	 */
	public static void LOAD_INF_HENMS_TRAN_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + HENMS_TRAN_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select Col_id, Col_val, Col_tran from HENMS_Tran";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(result.getInt(1) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<Integer, HashMap<String, String>> GET_INF_HENMS_TRAN_MAP() throws IOException {
		HashMap<Integer, HashMap<String, String>> map = new HashMap<Integer, HashMap<String, String>>();
		HashMap<String, String> sub_map = null;
		
		String filePath = getDBFilePath(CURR_DIR) + HENMS_TRAN_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<Col_id, map<Col_id#Col_val, Col_tran>>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != HENMS_TRAN_LEN) {
				continue;
			}
			
			//0--Col_id, 1--Col_val, 2--Col_tran
			int col_id = StringUtil.ParseInt(strArr[0]);
			String col_val = strArr[1];
			String col_tran = strArr[2];
			
			sub_map = map.get(col_id);
			if (sub_map == null){
				sub_map = new HashMap<String, String>();
				sub_map.put(col_id + SPLITCHAR + col_val, col_tran);
				map.put(col_id, sub_map);
			} else {
				sub_map.put(col_id + SPLITCHAR + col_val, col_tran);
			}
		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * ratp0_Inf_nat
	 */
	public static void LOAD_INF_NAT_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + RATP0_INF_NAT_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_area, vc_area_name from ratp0_Inf_nat";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_INF_NAT_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();

		String filePath = getDBFilePath(CURR_DIR) + RATP0_INF_NAT_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_area, vc_area_name> 只保留用到的字段
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != RATP0_INF_NAT_LEN) {
				continue;
			}
			
			//0--vc_area, 1--vc_area_name
			String vc_area = strArr[0];
			String vc_area_name = strArr[1];
			map.put(vc_area, vc_area_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_cbp_billacctnew
	 * cbep0_cbp_billitemnew
	 */
	public static void LOAD_CBP_BILLACCTNEW_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_CBP_BILLACCTNEW_AND_CBEP0_CBP_BILLITEMNEW_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select a.acctid, b.flag from cbep0_cbp_billacctnew a, cbep0_cbp_billitemnew b where a.billitemid = b.billitemid order by a.flag";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_CBP_BILLACCTNEW_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();

		String filePath = getDBFilePath(CURR_DIR) + CBEP0_CBP_BILLACCTNEW_AND_CBEP0_CBP_BILLITEMNEW_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_acctid, item_flag> 只保留用到的字段
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_CBP_BILLACCTNEW_AND_CBEP0_CBP_BILLITEMNEW_LEN) {
				continue;
			}
			
			//a.acctid, b.flag
			String acctid = strArr[0];
			String flag = strArr[1];
			map.put(acctid, flag);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_bus_translate
	 * 此表数据量大
	 */
	public static void LOAD_BUS_TRANSLATE_FILE() throws IOException, SQLException {
		logger.info("huge table cbep0_bus_translate begin");
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_BUS_TRANSLATE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		int count = 0;
		
		String sql = "select vc_buscode, vc_sp_code, vc_bus_name, vc_sp_name, vc_use_type, vc_fee_type, vc_flag from cbep0_bus_translate";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);

		logger.info("huge table cbep0_bus_translate start");
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(4)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(5)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(6)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(7)));
			bw.write(sb.toString());
			bw.newLine();
			
			count++;
			if(count % 100000 == 0){
				bw.flush();
				logger.info("huge table cbep0_bus_translate have load:" + count);
			}
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
		logger.info("huge table cbep0_bus_translate end");
	}
	
	public static HashMap<String, ArrayList<String>> GET_BUS_TRANSLATE_MAP() throws IOException {
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		ArrayList<String> sub_List = null;
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_BUS_TRANSLATE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_buscode, List<vc_sp_code#vc_bus_name#vc_sp_name#vc_use_type#vc_fee_type#vc_flag>>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_BUS_TRANSLATE_LEN) {
				continue;
			}
			
			//0--vc_buscode, 1--vc_sp_code, 2--vc_bus_name, 3--vc_sp_name, 4--vc_use_type, 5--vc_fee_type, 6--vc_flag
			String vc_buscode = strArr[0];
			String vc_sp_code = strArr[1];
			String vc_bus_name = strArr[2];
			String vc_sp_name = strArr[3];
			String vc_use_type = strArr[4];
			String vc_fee_type = strArr[5];
			String vc_flag = strArr[6];
			
			sub_List = map.get(vc_buscode);
			if (sub_List == null) {
				sub_List = new ArrayList<String>();
				map.put(vc_buscode, sub_List);
			}
			
			sub_List.add(vc_sp_code + SPLITCHAR + vc_bus_name + SPLITCHAR + vc_sp_name + SPLITCHAR + vc_use_type + SPLITCHAR + vc_fee_type + SPLITCHAR+ vc_flag);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * datp0_ib_mm_content_info
	 */
	public static void LOAD_IB_MM_CONTENT_INFO_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + DATP0_IB_MM_CONTENT_INFO_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_content_id, vc_content_name, vc_provider_id, vc_provider_name, vc_valid_date, vc_expire_date from datp0_ib_mm_content_info";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(4)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(5)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(6)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_IB_MM_CONTENT_INFO_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();

		String filePath = getDBFilePath(CURR_DIR) + DATP0_IB_MM_CONTENT_INFO_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_content_id, vc_content_name#vc_provider_id#vc_provider_name#vc_valid_date#vc_expire_date> 
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != DATP0_IB_MM_CONTENT_INFO_LEN) {
				continue;
			}
			
			//0--vc_content_id, 1--vc_content_name, 2--vc_provider_id, 3--vc_provider_name, 4--vc_valid_date, 5--vc_expire_date
			String vc_content_id = strArr[0];
			String vc_content_name = strArr[1];
			String vc_provider_id = strArr[2];
			String vc_provider_name = strArr[3];
			String vc_valid_date = strArr[4];
			String vc_expire_date = strArr[5];
			
			map.put(vc_content_id, vc_content_name + SPLITCHAR + vc_provider_id + SPLITCHAR + vc_provider_name + SPLITCHAR + vc_valid_date + SPLITCHAR + vc_expire_date);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * datp0_ib_12582_contentinfo
	 */
	public static void LOAD_IB_12582_CONTENTINFO_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + DATP0_IB_12582_CONTENTINFO_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();

		String sql = "select vc_content_id, vc_content_name, vc_provider_id, vc_provider_name, vc_valid_date, vc_expire_date from datp0_ib_12582_contentinfo";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(4)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(5)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(6)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_IB_12582_CONTENTINFO_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();

		String filePath = getDBFilePath(CURR_DIR) + DATP0_IB_12582_CONTENTINFO_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_content_id, vc_content_name#vc_provider_id#vc_provider_name#vc_valid_date#vc_expire_date> 
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != DATP0_IB_12582_CONTENTINFO_LEN) {
				continue;
			}
			
			//0--vc_content_id, 1--vc_content_name, 2--vc_provider_id, 3--vc_provider_name, 4--vc_valid_date, 5--vc_expire_date
			String vc_content_id = strArr[0];
			String vc_content_name = strArr[1];
			String vc_provider_id = strArr[2];
			String vc_provider_name = strArr[3];
			String vc_valid_date = strArr[4];
			String vc_expire_date = strArr[5];
			
			map.put(vc_content_id, vc_content_name + SPLITCHAR + vc_provider_id + SPLITCHAR + vc_provider_name + SPLITCHAR + vc_valid_date + SPLITCHAR + vc_expire_date);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * rptp0_inf_bus
	 * rptp0_inf_c2n
	 */
	public static void LOAD_INF_BUS_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + RPTP0_INF_BUS_AND_RPTP0_INF_C2N_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();

		String sql = "select sp_code, buscname, (case when sp_type = '3' then '1' else '0' end ) sp_type from rptp0_inf_bus where start_date <= sysdate() and (stop_date is null or stop_date >= sysdate()) union select code, cname, (case when sp_type = '1' then '1' else '0' end) from rptp0_inf_c2n where stop_date is null or stop_date > sysdate()";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_INF_BUS_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();

		String filePath = getDBFilePath(CURR_DIR) + RPTP0_INF_BUS_AND_RPTP0_INF_C2N_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<sp_code, buscname#sp_type> 
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != RPTP0_INF_BUS_AND_RPTP0_INF_C2N_FILENAME_LEN) {
				continue;
			}
			
			//0--sp_code, 1--buscname, 2--sp_type
			String sp_code = strArr[0];
			String buscname = strArr[1];
			String sp_type = strArr[2];
			
			map.put(sp_code, buscname + SPLITCHAR + sp_type);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * datp0_ib_game_content_info
	 */
	public static void LOAD_IB_GAME_CONTENT_INFO_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + DATP0_IB_GAME_CONTENT_INFO_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		

		String sql = "select content_code, content_name, date_format(valid_date,'%Y%m%d') start_date, date_format(expire_date,'%Y%m%d') end_date from datp0_ib_game_content_info order by content_code, start_date desc";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(4)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, ArrayList<String>> GET_IB_GAME_CONTENT_INFO_MAP() throws IOException {
		HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
		ArrayList<String> sub_List = null;
		String tmp_content_code = null;

		String filePath = getDBFilePath(CURR_DIR) + DATP0_IB_GAME_CONTENT_INFO_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<content_code, list<content_name#start_date#end_date>>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != DATP0_IB_GAME_CONTENT_INFO_LEN) {
				continue;
			}
			
			//0--content_code, 1--content_name, 2--start_date, 3--end_date
			String content_code = strArr[0];
			String content_name = strArr[1];
			String start_date = strArr[2];
			String end_date = strArr[3];
			
			if (!content_code.equals(tmp_content_code)){
				tmp_content_code = content_code;
				sub_List = new ArrayList<String>();
				map.put(tmp_content_code, sub_List);
			}
			
			sub_List.add(content_name + SPLITCHAR + start_date + SPLITCHAR + end_date);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_inf_ecdetailinfo
	 */
	public static void LOAD_INF_ECDETAILINFO_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_ECDETAILINFO_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();

		String sql = "select subsid, servtype, servinfo from cbep0_inf_ecdetailinfo where sysdate() between starttime and endtime";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(3)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_INF_ECDETAILINFO_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_ECDETAILINFO_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<subsid#servtype, servinfo>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_ECDETAILINFO_LEN) {
				continue;
			}
			
			//0--subsid, 1--servtype, 2--servinfo
			String subsid = strArr[0];
			String servtype = strArr[1];
			String servinfo = strArr[2];
			
			map.put(subsid + SPLITCHAR + servtype, servinfo);
		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_special_product
	 */
	public static void LOAD_SPECIAL_PRODUCT_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_SPECIAL_PRODUCT_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();

		String sql = "select bus_subus_type, pro_name from cbep0_special_product";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}

		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_SPECIAL_PRODUCT_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
	
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_SPECIAL_PRODUCT_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<bus_subus_type, pro_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_SPECIAL_PRODUCT_LEN) {
				continue;
			}
			
			//0--bus_subus_type, 1--pro_name
			String bus_subus_type = strArr[0];
			String pro_name = strArr[1];
			
			map.put(bus_subus_type, pro_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_product_translate
	 * 此表数据量大
	 */
	public static void LOAD_PRODUCT_TRANSLATE_FILE() throws IOException, SQLException {
		logger.info("huge table cbep0_product_translate begin");
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_PRODUCT_TRANSLATE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		int count = 0;
		
		String sql = "select vc_product_id, vc_product_name from cbep0_product_translate";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		logger.info("huge table cbep0_product_translate begin");
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
			
			count++;
			if(count % 100000 == 0){
				bw.flush();
				logger.info("huge table cbep0_product_translate have load:" + count);
			}
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
		logger.info("huge table cbep0_product_translate end");
	}
	
	public static HashMap<String, String> GET_PRODUCT_TRANSLATE_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_PRODUCT_TRANSLATE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_product_id, vc_product_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_PRODUCT_TRANSLATE_LEN) {
				continue;
			}
			
			//0--vc_product_id, 1--vc_product_name
			String vc_product_id = strArr[0];
			String vc_product_name = strArr[1];
			
			map.put(vc_product_id, vc_product_name);
		}
		
		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_subject_translate
	 * 此表数据量大
	 */
	public static void LOAD_SUBJECT_TRANSLATE_FILE() throws IOException, SQLException {
		logger.info("huge table cbep0_subject_translate begin");
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_SUBJECT_TRANSLATE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		int count = 0;

		String sql = "select vc_subject_id, vc_subject_name from cbep0_subject_translate";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		logger.info("huge table cbep0_subject_translate start");
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
			
			count++;
			if(count % 100000 == 0){
				bw.flush();
				logger.info("huge table cbep0_subject_translate have load:" + count);
			}
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
		logger.info("huge table cbep0_subject_translate end");
	}
	
	public static HashMap<String, String> GET_SUBJECT_TRANSLATE_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_SUBJECT_TRANSLATE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_subject_id, vc_subject_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_SUBJECT_TRANSLATE_LEN) {
				continue;
			}
			
			//0--vc_subject_id, 1--vc_subject_name
			String vc_subject_id = strArr[0];
			String vc_subject_name = strArr[1];
			
			map.put(vc_subject_id, vc_subject_name);

		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * sysp0_Inf_switch
	 */
	public static void LOAD_INF_SWITCH_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + SYSP0_INF_SWITCH_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_switch_flag, vc_switch_name from sysp0_Inf_switch";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_INF_SWITCH_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + SYSP0_INF_SWITCH_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_switch_flag, vc_switch_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != SYSP0_INF_SWITCH_LEN) {
				continue;
			}
			
			//0--vc_switch_flag, 1--vc_switch_name
			String vc_switch_flag = strArr[0];
			String vc_switch_name = strArr[1];
			
			map.put(vc_switch_flag, vc_switch_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_pf_acct
	 */
	public static void LOAD_PF_ACCT_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_PF_ACCT_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select acctid, printname from cbep0_pf_acct";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_PF_ACCT_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_PF_ACCT_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<acctid, printname>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_PF_ACCT_LEN) {
				continue;
			}
			
			//0--acctid, 1--printname
			String acctid = strArr[0];
			String printname = strArr[1];
			
			map.put(acctid, printname);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_prov_translate
	 */
	public static void LOAD_PROV_TRANSLATE_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_PROV_TRANSLATE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_switch_flag, vc_prov_name from cbep0_prov_translate";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}

		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_PROV_TRANSLATE_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_PROV_TRANSLATE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_switch_flag, vc_prov_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_PROV_TRANSLATE_LEN) {
				continue;
			}
			
			//0--vc_switch_flag, 1--vc_prov_name
			String vc_switch_flag = strArr[0];
			String vc_prov_name = strArr[1];
			
			map.put(vc_switch_flag, vc_prov_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_inf_feetype_translate
	 */
	public static void LOAD_INF_FEETYPE_TRANSLATE_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_FEETYPE_TRANSLATE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();

		String sql = "select vc_feetype_code, vc_feetype_name from cbep0_inf_feetype_translate";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);
		
		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}

		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_INF_FEETYPE_TRANSLATE_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_FEETYPE_TRANSLATE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_feetype_code, vc_feetype_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_FEETYPE_TRANSLATE_LEN) {
				continue;
			}
			
			//0--vc_feetype_code, 1--vc_feetype_name
			String vc_feetype_code = strArr[0];
			String vc_feetype_name = strArr[1];
			
			map.put(vc_feetype_code, vc_feetype_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_hsc_ssid_trans
	 */
	public static void LOAD_HSC_SSID_TRANS_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_HSC_SSID_TRANS_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select ssidnum, ssidname from cbep0_hsc_ssid_trans where valid = 1";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);

		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_HSC_SSID_TRANS_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_HSC_SSID_TRANS_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<ssidnum, ssidname>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_HSC_SSID_TRANS_LEN) {
				continue;
			}
			
			//0--ssidnum, 1--ssidname
			String ssidnum = strArr[0];
			String ssidname = strArr[1];
			
			map.put(ssidnum, ssidname);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * datp0_group_translate
	 */
	public static void LOAD_GROUP_TRANSLATE_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + DATP0_GROUP_TRANSLATE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_bill_type, vc_trans_name from datp0_group_translate";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);

		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_GROUP_TRANSLATE_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + DATP0_GROUP_TRANSLATE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<vc_bill_type, vc_trans_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != DATP0_GROUP_TRANSLATE_LEN) {
				continue;
			}
			
			//0--vc_bill_type, 1--vc_trans_name
			String vc_bill_type = strArr[0];
			String vc_trans_name = strArr[1];
			
			map.put(vc_bill_type, vc_trans_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_net_address
	 */
	public static void LOAD_NET_ADDRESS_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_NET_ADDRESS_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select service_code, net_address from cbep0_net_address";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);

		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_NET_ADDRESS_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_NET_ADDRESS_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<service_code, net_address>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_NET_ADDRESS_LEN) {
				continue;
			}
			
			//0--service_code, 1--net_address
			String service_code = strArr[0];
			String net_address = strArr[1];
			
			map.put(service_code, net_address);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * cbep0_inf_price
	 */
	public static void LOAD_INF_PRICE_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_PRICE_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select unitcode, unit_name from cbep0_inf_price";
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		ResultSet result = stmt.executeQuery();
		OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
		BufferedWriter bw = new BufferedWriter(osw);

		while(result.next()) {
			sb.setLength(0);
			sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
			sb.append(cleanCRLF(result.getString(2)));
			bw.write(sb.toString());
			bw.newLine();
		}
		
		bw.flush();
		close(bw, osw, null, null, result, stmt);
	}
	
	public static HashMap<String, String> GET_INF_PRICE_MAP() throws IOException {
		HashMap<String, String> map = new HashMap<String, String>();
		
		String filePath = getDBFilePath(CURR_DIR) + CBEP0_INF_PRICE_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
		BufferedReader br = new BufferedReader(isr);
		
		// map<unitcode, unit_name>
		String line = null;
		while((line = br.readLine()) != null) {
			String[] strArr = line.split(SPLITCHAR, -1);
			//不符合条件跳过
			if(strArr.length != CBEP0_INF_PRICE_LEN) {
				continue;
			}
			
			//0--unitcode, 1--unit_name
			String unitcode = strArr[0];
			String unit_name = strArr[1];
			
			map.put(unitcode, unit_name);
		}

		close(null, null, br, isr, null, null);
		return map;
	}
	
	/*
	 * sysp1_inf_msi
	 */
	public static void LOAD_INF_MSI_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + SYSP1_INF_MSI_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_intftg, vc_msisdn_low, vc_msisdn_high, vc_area, vc_district, vc_switch_flag, vc_operator, vc_brand, vc_user_type, vc_start_period, vc_stop_period, dt_start_time, dt_stop_time, vc_type from sysp1_inf_msi";
		PreparedStatement stmt = null;
		ResultSet result = null;
		OutputStreamWriter osw = null;
		BufferedWriter bw = null;
		
		try {
			stmt = getConnection().prepareStatement(sql);
			result = stmt.executeQuery();
			osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
			bw = new BufferedWriter(osw);

			while(result.next()) {
				sb.setLength(0);
				sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(3)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(4)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(5)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(6)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(7)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(8)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(9)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(10)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(11)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(12)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(13)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(14)));
				bw.write(sb.toString());
				bw.newLine();
			}
			bw.flush();
		} catch (MySQLSyntaxErrorException e) {
			logger.warn("Table 'paas.sysp1_inf_msi' doesn't exist");
		}
		close(bw, osw, null, null, result, stmt);
	}
	
	public static Vector<Sysp1InfMsi> GET_INF_MSI_VEC() throws IOException {
		Vector<Sysp1InfMsi> vecMSI = new Vector<Sysp1InfMsi>();
		
		String filePath = getDBFilePath(CURR_DIR) + SYSP1_INF_MSI_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		try {
			isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
			br = new BufferedReader(isr);
			
			String line = null;
			while((line = br.readLine()) != null) {
				String[] strArr = line.split(SPLITCHAR, -1);
				//不符合条件跳过
				if(strArr.length != SYSP1_INF_MSI_LEN) {
					continue;
				}
				
				//0--vc_intftg, 1--vc_msisdn_low, 2--vc_msisdn_high, 3--vc_area, 4--vc_district, 5--vc_switch_flag, 6--vc_operator,
				//7--vc_brand, 8--vc_user_type, 9--vc_start_period, 10--vc_stop_period, 11--dt_start_time, 12--dt_stop_time, 13--vc_type
				Sysp1InfMsi sysp1InfMsi = new Sysp1InfMsi();
				sysp1InfMsi.vc_intftg = strArr[0];
				sysp1InfMsi.vc_msisdn_low = strArr[1];
				sysp1InfMsi.vc_msisdn_high = strArr[2];
				sysp1InfMsi.vc_area = strArr[3];
				sysp1InfMsi.vc_district = strArr[4];
				sysp1InfMsi.vc_switch_flag = strArr[5];
				sysp1InfMsi.vc_operator = strArr[6];
				sysp1InfMsi.vc_brand = strArr[7];
				sysp1InfMsi.vc_user_type = strArr[8];
				sysp1InfMsi.vc_start_period = strArr[9];
				sysp1InfMsi.vc_stop_period = strArr[10];
				sysp1InfMsi.dt_start_time = strArr[11];
				sysp1InfMsi.dt_stop_time = strArr[12];
				sysp1InfMsi.vc_type = strArr[13];

				if (!sysp1InfMsi.vc_msisdn_low.contains(sysp1InfMsi.vc_msisdn_high.substring(0, Constant.MIN_SAME_MSI_LEN))) {
					for (int i = 0; i < Constant.MIN_SAME_MSI_LEN; i++) {
						if (sysp1InfMsi.vc_msisdn_low.charAt(i) != sysp1InfMsi.vc_msisdn_high.charAt(i)) {
							Constant.MIN_SAME_MSI_LEN = i;
							break;
						}
					}
				}
				vecMSI.add(sysp1InfMsi);
			}
			Collections.sort(vecMSI);
		} catch (FileNotFoundException e) {
			logger.warn("Can not find the sysp1_inf_msi.curr");
		} 
		
		close(null, null, br, isr, null, null);
		return vecMSI;
	}
	
	/*
	 * sysp1_inf_pbs
	 */
	public static void LOAD_INF_PBS_FILE() throws IOException, SQLException {
		String filePath = getDBFilePath(CURR_DIR) + SYSP1_INF_PBS_FILENAME + CURR_SUFFIX;
		StringBuffer sb = new StringBuffer();
		
		String sql = "select vc_intftg, vc_subno, vc_area, vc_district, vc_switch_flag, vc_operator, vc_brand, vc_user_type, vc_boce_type, vc_start_period, vc_stop_period, dt_start_time, dt_stop_time from sysp1_inf_pbs";
		PreparedStatement stmt = null;
		ResultSet result = null;
		OutputStreamWriter osw = null;
		BufferedWriter bw = null;
		
		try {
			stmt = getConnection().prepareStatement(sql);
			result = stmt.executeQuery();
			osw = new OutputStreamWriter(new FileOutputStream(new File(filePath)), "GBK");
			bw = new BufferedWriter(osw);
			
			while(result.next()) {
				sb.setLength(0);
				sb.append(cleanCRLF(result.getString(1)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(2)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(3)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(4)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(5)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(6)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(7)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(8)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(9)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(10)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(11)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(12)) + SPLITCHAR);
				sb.append(cleanCRLF(result.getString(13)));
				bw.write(sb.toString());
				bw.newLine();
			}
			bw.flush();
		} catch (MySQLSyntaxErrorException e) {
			logger.warn("Table 'paas.sysp1_inf_pbs' doesn't exist");
		}
		close(bw, osw, null, null, result, stmt);
	}
	
	public static Vector<Sysp1InfPbs> GET_INF_PBS_VEC() throws IOException {
		Vector<Sysp1InfPbs> vecPBS = new Vector<Sysp1InfPbs>();
		
		String filePath = getDBFilePath(CURR_DIR) + SYSP1_INF_PBS_FILENAME + CURR_SUFFIX;//输入文件的全路径
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		try {
			isr = new InputStreamReader(new FileInputStream(new File(filePath)), "GBK");
			br = new BufferedReader(isr);

			String line = null;
			while((line = br.readLine()) != null) {
				String[] strArr = line.split(SPLITCHAR, -1);
				//不符合条件跳过
				if(strArr.length != SYSP1_INF_PBS_LEN) {
					continue;
				}
				
				//0--vc_intftg, 1--vc_subno, 2--vc_area, 3--vc_district, 4--vc_switch_flag, 5--vc_operator, 6--vc_brand, 
				//7--vc_user_type, 8--vc_boce_type, 9--vc_start_period, 10--vc_stop_period, 11--dt_start_time, 12--dt_stop_time
				Sysp1InfPbs sysp1InfPbs = new Sysp1InfPbs();
				sysp1InfPbs.vc_intftg = strArr[0];
				sysp1InfPbs.vc_subno = strArr[1];
				sysp1InfPbs.vc_area = strArr[2];
				sysp1InfPbs.vc_district = strArr[3];
				sysp1InfPbs.vc_switch_flag = strArr[4];
				sysp1InfPbs.vc_operator = strArr[5];
				sysp1InfPbs.vc_brand = strArr[6];
				sysp1InfPbs.vc_user_type = strArr[7];
				sysp1InfPbs.vc_boce_type = strArr[8];
				sysp1InfPbs.vc_start_period = strArr[9];
				sysp1InfPbs.vc_stop_period = strArr[10];
				sysp1InfPbs.dt_start_time = strArr[11];
				sysp1InfPbs.dt_stop_time = strArr[12];
				
				vecPBS.add(sysp1InfPbs);
			}
	        Collections.sort(vecPBS); 
		} catch (FileNotFoundException e) {
			logger.warn("Can not find the sysp1_inf_pbs.curr");
		}
		
		close(null, null, br, isr, null, null);
		return vecPBS;
	}
	
	/**
	 * 获取连接数据库Connection对象
	 * @return
	 */
	public static Connection getConnection() {
		if (conn == null) {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(DBURL, DBNAME, DBPWD);
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			} catch (ClassNotFoundException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return conn;
	}
	
	/**
	 * 关闭连接数据库Connection对象
	 */
	public static void closeConnection() {
		if(conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			conn = null;
		}
	}
	
	/**
	 * 把src目录中的文件移动到dst目录，并且修改后缀名
	 * @param src 源文件夹
	 * @param dst 目标文件夹
	 * @param suffix 移动文件后的后缀名
	 */
	public static void moveFile(String src, String dst, String suffix) throws IOException{
		logger.info("* move file begin");
		//原文件夹处理
		File srcFile = new File(src);
		if(!srcFile.exists()) {
			srcFile.mkdirs();
		}
		if(!srcFile.isDirectory()) {
			srcFile.delete();
			srcFile.mkdirs();
		}
		//目标文件夹处理
		File dstFile = new File(dst);
		if(!dstFile.exists()) {
			dstFile.mkdirs();
		}
		if(!dstFile.isDirectory()) {
			dstFile.delete();
			dstFile.mkdirs();
		}
		//目标文件夹清空文件
		File[] tempList = dstFile.listFiles();
		File temp = null;
		for (int i = 0; i < tempList.length; i++) {
			temp = tempList[i];
			if(temp.isFile()){
				temp.delete();
			}
		}
		//移动文件
		File[] srcFiles = srcFile.listFiles();
		File newFile = null;
		String prefix;
		for (int i = 0; i < srcFiles.length; i++) {
			temp = srcFiles[i];
			prefix = temp.getName().substring(0, temp.getName().lastIndexOf("."));
			if(!suffix.startsWith(".")){
				suffix = "." + suffix;
			}
			newFile = new File(dstFile.getCanonicalPath(), prefix + suffix);
			temp.renameTo(newFile);
		}
		logger.info("* move file end");
	}
	
	/**
	 * 获取DBFILE_DIR目录下的子目录路径
	 * @param str 子目录
	 * @return path
	 */
	public static String getDBFilePath(String str) {
		File file = new File(DBFILE_DIR);
		if(!file.exists()){
			file.mkdirs();
		}
		if(!file.isDirectory()){
			file.delete();
			file.mkdirs();
		}
		return new File(file.getAbsolutePath(), str).getAbsolutePath() + File.separator;
	}

	/**
	 * 关闭流
	 * @param bw BufferedWriter
	 * @param osw OutputStreamWriter
	 * @param br BufferedReader
	 * @param isr InputStreamReader
	 * @param result ResultSet
	 * @param stmt PreparedStatement
	 */
	public static void close(BufferedWriter bw, OutputStreamWriter osw, BufferedReader br, InputStreamReader isr, ResultSet result, PreparedStatement stmt) {
		if(bw != null){try {bw.close();} catch (IOException e) {logger.error(e.getMessage(), e);}}
		if(osw != null){try {osw.close();} catch (IOException e) {logger.error(e.getMessage(), e);}}
		if(br != null){try {br.close();} catch (IOException e) {logger.error(e.getMessage(), e);}}
		if(isr != null){try {isr.close();} catch (IOException e) {logger.error(e.getMessage(), e);}}
		if(result != null){try {result.close();} catch (SQLException e) {logger.error(e.getMessage(), e);}}
		if(stmt != null){try {stmt.close();} catch (SQLException e) {logger.error(e.getMessage(), e);}}
	}
	
	/**
	 * 清除字符串中的'\r','\n'
	 */
	public static String cleanCRLF(String str) {
		return str.replace("\r", "").replace("\n", "");
	}
	
}