package com.asiainfo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.util.HbaseLoginUtil;
import com.asiainfo.util.MysqlJdbcProcess;
import com.asiainfo.util.PWDHandle;
import com.asiainfo.util.StringUtil;

public class Constant {

	public static int OVERTIME=600;
	public static int HBASE_TIMEOUT=20;
	public static int SUBBAG_OVERTIME=120;
	public static int BLANKTIME=30;
	public static int PORT=8080;
	public static String CHARSET="gbk";
	public static int THREADNUM=4;
	public static int THREADPOOLNUM=16;
	public static int CONNECTPOOL=1024;
	public static int READBUFFERSIZE=1024;
	public static int WRITEBUFFERSIZE=8192;
	public static int BAGSIZE=8192;
	public static boolean DEBUGMODE = false;
	public static int MAX_CDR_COUNT=50000;
	public static int DBRELOAD_TIME=2;
	public static String IP = "";
	public static String USERNAME = "HBaseDeveloper";
	public static String USERKEYTABFILE = "./user.keytab";
	public static String KRB5FILE = "./krb5.conf";
	public static Configuration configuration = HBaseConfiguration.create();
	public static Connection connection = null;
	private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
	private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
	private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";
	public static TreeSet<String> PrefixSet = new TreeSet<String>();
	
	public static HashMap<String,String>  BILL_USER_MAP=null;
	// map<int_querytype, map<int_querytype#int_cdrtype, int_modelid>>
	public static HashMap<Integer, HashMap<String, Integer>>  INF_QUERY_MAP = null; 
	// map<int_mod_id, vc_delimiter#vc_head>
	public static HashMap<Integer, String>  INF_MOD_MAP = null;
	// map<int_mod_id, List<int_seq_id#int_column_id#vc_tip>> 导入时注意顺序
	public static HashMap<Integer, ArrayList<String>>  INF_MOD_COLUMN_MAP = null;
	// map<int_column_id, map<int_column_id#int_cbe_type, int_get_flag#int_offset#int_len>>
	public static HashMap<Integer, HashMap<String, String>>  INF_COLUMN_MAP = null;
	// map<Col_id, map<Col_id#Col_val, Col_tran>>
	public static HashMap<Integer, HashMap<String, String>>  INF_HENMS_TRAN_MAP = null;
	// map<vc_area, vc_area_name>
	public static HashMap<String, String>  INF_NAT_MAP = null;
	// map<vc_acctid, item_flag>
	public static HashMap<String, String>  CBP_BILLACCTNEW_MAP = null;
	// map<vc_buscode, List<vc_sp_code#vc_bus_name#vc_sp_name#vc_use_type#vc_fee_type#vc_flag>> 此表数据量大，先排序
	public static HashMap<String, ArrayList<String>> BUS_TRANSLATE_MAP = null;
	// map<vc_content_id, vc_content_name#vc_provider_id#vc_provider_name#vc_valid_date#vc_expire_date> 
	public static HashMap<String, String>  IB_MM_CONTENT_INFO_MAP = null;
	// map<vc_content_id, vc_content_name#vc_provider_id#vc_provider_name#vc_valid_date#vc_expire_date> 
	public static HashMap<String, String>  IB_12582_CONTENTINFO_MAP = null;
	// map<sp_code, buscname#sp_type> 
	public static HashMap<String, String>  INF_BUS_MAP = null;
	// map<content_code, list<content_name#start_date#end_date>>
	public static HashMap<String, ArrayList<String>>  IB_GAME_CONTENT_INFO_MAP = null;
	// map<subsid#servtype, servinfo>
	public static HashMap<String, String>  INF_ECDETAILINFO_MAP = null;
	// map<bus_subus_type, pro_name>
	public static HashMap<String, String>  SPECIAL_PRODUCT_MAP = null;
	// map<vc_product_id, vc_product_name> 此表数据量大
	public static HashMap<String, String>  PRODUCT_TRANSLATE_MAP = null;
	// map<vc_subject_id, vc_subject_name> 此表数据量大
	public static HashMap<String, String>  SUBJECT_TRANSLATE_MAP = null;
	// map<vc_switch_flag, vc_switch_name>
	public static HashMap<String, String>  INF_SWITCH_MAP = null;
	// map<acctid, printname>
	public static HashMap<String, String>  PF_ACCT_MAP = null;
	// map<vc_switch_flag, vc_prov_name>
	public static HashMap<String, String>  PROV_TRANSLATE_MAP = null;
	// map<vc_feetype_code, vc_feetype_name>
	public static HashMap<String, String>  INF_FEETYPE_TRANSLATE_MAP = null;
	// map<ssidnum, ssidname>
	public static HashMap<String, String>  HSC_SSID_TRANS_MAP = null;
	// map<vc_bill_type, vc_trans_name>
	public static HashMap<String, String>  GROUP_TRANSLATE_MAP = null;
	// map<service_code, net_address>
	public static HashMap<String, String>  NET_ADDRESS_MAP = null;
	// map<unitcode, unit_name>
	public static HashMap<String, String>  INF_PRICE_MAP = null;
	// 号段表
	public static Vector<Sysp1InfMsi> INF_MSI_VEC = null;
	public static Vector<Sysp1InfPbs> INF_PBS_VEC = null;
	    
	// logback 配置文件常量
	public static String LOGBACK_CONF_FILE = "./logback.xml";
	public static String DEBUG_CDR_FILE = "./debugcdr.xml";
	public static String LOGIN_USERS_PWD = null;

	public final static String CMD_BIND_REQ_RSP = "000";
	public final static String CMD_UNBIND_REQ_RSP = "001";
	public final static String CMD_PWDCHG_REQ_RSP = "002";
	public final static String CMD_HEARTBAG_REQ_RSP = "999";
	// 清单
	public final static String CMD_CDR_REQ = "100";
	public final static String CMD_CDR_RSP = "200";
	public final static String CMD_CLIENT_CDR_RSP = "101";
	// 账单
	public final static String CMD_ACCT_REQ = "300";
	public final static String CMD_ACCT_RSP = "400";
	public final static String CMD_CLIENT_ACCT_RSP = "301";
	// 和账单
	public final static String CMD_ACCT_SUM_REQ = "302";
	public final static String CMD_CLIENT_ACCT_SUM_RSP = "303";
	// 流量账单
	public final static String CMD_ACCT_NET_USAGE_REQ = "304";
	public final static String CMD_CLIENT_ACCT_NET_USAGE_RSP = "305";
	// 和账单、流量账单服务端返回码
	public final static String CMD_ACCT_SUM_OR_NET_USAGE_RSP = "401";
	// 和飞信
	public final static String CMD_HEFETION_REQ = "500";
	public final static String CMD_HEFETION_RSP = "502";
	public final static String CMD_CLIENT_HEFETION_RSP = "501";
	// 集团账单
	public final static String CMD_GROUP_REQ = "600";
	public final static String CMD_GROUP_MX_REQ = "603";
	public final static String CMD_GROUP_DF_REQ = "604";
	public final static String CMD_GROUP_RSP = "602";
	public final static String CMD_CLIENT_GROUP_RSP = "601";
	
	// 清单长度 包括\r\n;
	public final static int CDR_LEN_VOICE = 1018;
	public final static int CDR_LEN_SMS = 965;
	public final static int CDR_LEN_GPRS = 1039;
	public final static int CDR_LEN_MOBILESELF = 946;
	public final static int CDR_LEN_SP = 1023;
	public final static int CDR_LEN_FIX = 300;
	public final static int CDR_LEN_OTHER = 305;
	public final static int CDR_LEN_GROVOICE = 1090;
	public final static int CDR_LEN_GROSMS = 989;
	public final static int CDR_LEN_GROGPRS = 1076;
	public final static int CDR_LEN_GROTHER = 1047;
	public final static int CDR_LEN_FETION = 1500;

	// 号段信息
	public static int MIN_SAME_MSI_LEN = 11;
	public static class Sysp1InfMsi implements Comparable<Object> {
		public String vc_intftg;		// 7
		public String vc_msisdn_low;	// 15
		public String vc_msisdn_high;	// 15
		public String vc_area;			// 8
		public String vc_district;		// 2
		public String vc_switch_flag;	// 2
		public String vc_operator;		// 2
		public String vc_brand;			// 1
		public String vc_user_type;		// 2
		public String vc_start_period;	// 6
		public String vc_stop_period;	// 6
		public String dt_start_time;	// 14
		public String dt_stop_time;		// 14
		public String vc_type;			// 3
		
		@Override
		public int compareTo(Object o) {
			Sysp1InfMsi sysp1InfMsiR = (Sysp1InfMsi) o;
			int ret=this.vc_msisdn_low.compareTo(sysp1InfMsiR.vc_msisdn_low);
			
            if (ret == 0) {
            	ret=this.vc_start_period.compareTo(sysp1InfMsiR.vc_start_period);
            	
            	if (ret == 0){
            		ret=this.dt_start_time.compareTo(sysp1InfMsiR.dt_start_time);
            	}
            }
            return ret;
		}
	}

	public static class Sysp1InfPbs implements Comparable<Object> {
		public String vc_intftg;		// 7
		public String vc_subno;			// 11
		public String vc_area;			// 8
		public String vc_district;		// 2
		public String vc_switch_flag;	// 2
		public String vc_operator;		// 2
		public String vc_brand;			// 1
		public String vc_user_type;		// 2
		public String vc_boce_type;		// 3
		public String vc_start_period;	// 6
		public String vc_stop_period;	// 6
		public String dt_start_time;	// 14
		public String dt_stop_time;		// 14
		
		@Override
		public int compareTo(Object o) {
			Sysp1InfPbs sysp1InfPbsR = (Sysp1InfPbs) o;
			int ret=this.vc_subno.compareTo(sysp1InfPbsR.vc_subno);
            if (ret == 0) {
            	ret=this.vc_start_period.compareTo(sysp1InfPbsR.vc_start_period);
            	if (ret == 0){
            		ret=this.dt_start_time.compareTo(sysp1InfPbsR.dt_start_time);
            	}
            }
            return ret;
		}
	}
	
	
	// 初始化配置
	public static void initData(Properties properties) throws IOException {
		OVERTIME = StringUtil.ParseInt(properties.getProperty("OVERTIME","600"));
		HBASE_TIMEOUT = StringUtil.ParseInt(properties.getProperty("HBASE_TIMEOUT", "10"));
		SUBBAG_OVERTIME = StringUtil.ParseInt(properties.getProperty("SUBBAG_OVERTIME", "120"));
		PORT = StringUtil.ParseInt(properties.getProperty("PORT", "8080"));
		CHARSET=properties.getProperty("CHARSET", "gbk");
		THREADNUM = StringUtil.ParseInt(properties.getProperty("THREADNUM", "4"));
		THREADPOOLNUM = StringUtil.ParseInt(properties.getProperty("THREADPOOLNUM", "16"));
		CONNECTPOOL = StringUtil.ParseInt(properties.getProperty("CONNECTPOOL", "1024"));
		READBUFFERSIZE = StringUtil.ParseInt(properties.getProperty("READBUFFERSIZE", "1024"));
		WRITEBUFFERSIZE = StringUtil.ParseInt(properties.getProperty("WRITEBUFFERSIZE", "8192"));
		BAGSIZE = StringUtil.ParseInt(properties.getProperty("BAGSIZE", "8192"));
		LOGBACK_CONF_FILE=properties.getProperty("LOGBACK_CONF_FILE", "./logback.xml");
		DEBUG_CDR_FILE=properties.getProperty("DEBUG_CDR_FILE", "./debugcdr.xml");
		USERNAME=properties.getProperty("USERNAME", "HBaseDeveloper");
		USERKEYTABFILE=properties.getProperty("USERKEYTABFILE", "./user.keytab");
		KRB5FILE=properties.getProperty("KRB5FILE", "./krb5.conf");
		DEBUGMODE = "true".equalsIgnoreCase(properties.getProperty("DEBUG_MODE"));
		MAX_CDR_COUNT = StringUtil.ParseInt(properties.getProperty("MAX_CDR_COUNT", "50000"));
		LOGIN_USERS_PWD = properties.getProperty("LOGIN_USERS_PWD", "");
		
		BILL_USER_MAP = StringUtil.toMAp(LOGIN_USERS_PWD);
		if (BILL_USER_MAP.size() == 0) {
			throw new IllegalStateException();
		} else {
			for (Entry<String, String> entry : BILL_USER_MAP.entrySet()) {
				entry.setValue(PWDHandle.decrypt(entry.getValue()));
			}
		}
		
		String[] tmp = properties.getProperty("DBRELOAD_TIME","02:00").split(":");
		DBRELOAD_TIME=StringUtil.ParseInt(tmp[0]);
		IP = InetAddress.getLocalHost().getHostAddress();
		
		tmp = properties.getProperty("PREFIX_FILTER","020,0668").split(",");
		for (String pre : tmp) {
			PrefixSet.add(pre);
		}
		
	}

	// 初始化日志
	public static void initLog(Properties properties) throws IOException {
		String logpath = properties.getProperty("LOG_PATH");
		System.setProperty("LOG_HOME", logpath);
		
		File logbackConfFile = new File(Constant.LOGBACK_CONF_FILE);
		if (!logbackConfFile.canRead()) {
			System.out.println("日志配置文件logback.xml不存在:"+ logbackConfFile.getAbsolutePath());
			throw new IllegalStateException();
		}
		if (System.getProperty("logback.configurationFile") == null)
			System.setProperty("logback.configurationFile",logbackConfFile.getAbsolutePath());
	}
	
	// 初始化数据库，初始化接口
	public static void initDB(Properties properties) {
		Logger logger = LoggerFactory.getLogger(Constant.class);
		
		MysqlJdbcProcess.DBURL=properties.getProperty("DBURL");
		MysqlJdbcProcess.DBNAME=properties.getProperty("DBNAME");
		MysqlJdbcProcess.DBPWD=PWDHandle.decrypt(properties.getProperty("DBPWD"));
		MysqlJdbcProcess.DBFILE_DIR=properties.getProperty("DBFILE_DIR");
		
		int mode = StringUtil.ParseInt(properties.getProperty("DBLOAD_MODE", "0"));
		String src = MysqlJdbcProcess.getDBFilePath(MysqlJdbcProcess.CURR_DIR);
		String dst = MysqlJdbcProcess.getDBFilePath(MysqlJdbcProcess.BAK_DIR);

		//启动模式：0-->mysql启动，1-->文件启动
		if(mode == 0){
			
			//1、备份文件，改后缀
			try {
				MysqlJdbcProcess.moveFile(src, dst, MysqlJdbcProcess.BAK_SUFFIX);
			} catch (IOException e) {
				logger.error("文件备份失败", e);
				throw new IllegalStateException();
			}
			
			//2、mysql数据导出到curr目录
			try {
				//2.1 写入成功
				MysqlJdbcProcess.loadFile();
			} catch (Exception e) {
				//2.2 写入失败
				logger.warn("Mysql数据导出到curr目录失败，启动备用方案");
				
				File file = new File(dst);
				if(file.list().length <= 0){
					//2.2.1 backup目录没文件，退出
					logger.error("backup目录没文件，退出");
					throw new IllegalStateException();
				} else {
					//2.2.2 backup目录有文件
					try {
						//2.2.2.1 backup目录有文件，恢复到curr目录
						MysqlJdbcProcess.moveFile(dst, src, MysqlJdbcProcess.CURR_SUFFIX);
					} catch (IOException ee) {
						logger.error("文件恢复失败", ee);
						throw new IllegalStateException();
					}
				}
			}
		}
		
		//3、文件load到内存
		try {
			MysqlJdbcProcess.loadMemory();
		} catch (IOException e) {
			if(mode != 0){
				logger.error("文件模式启动失败，请修改配置项：DBLOAD_MODE=0", e);
			} else {
				logger.error("文件读取失败", e);
			}
			throw new IllegalStateException();
		}
		
	}
	
	// 初始化数据库，定时任务接口
	public static void initDB() {
		Logger logger = LoggerFactory.getLogger(Constant.class);
		
		String src = MysqlJdbcProcess.getDBFilePath(MysqlJdbcProcess.CURR_DIR);
		String dst = MysqlJdbcProcess.getDBFilePath(MysqlJdbcProcess.BAK_DIR);
		
		//1、备份文件，改后缀
		try {
			MysqlJdbcProcess.moveFile(src, dst, MysqlJdbcProcess.BAK_SUFFIX);
		} catch (IOException e) {
			logger.error("文件备份失败", e);
			throw new IllegalStateException();
		}
		
		//2、mysql数据导出到curr目录
		try {
			//2.1 写入成功
			MysqlJdbcProcess.loadFile();
		} catch (Exception e) {
			//2.2 写入失败
			try {
				//2.2.2.1 backup目录有文件，恢复到curr目录
				MysqlJdbcProcess.moveFile(dst, src, MysqlJdbcProcess.CURR_SUFFIX);
			} catch (IOException ee) {
				logger.error("文件恢复失败", ee);
				throw new IllegalStateException();
			}
		}
		
		//3、文件load到内存
		try {
			MysqlJdbcProcess.loadMemory();
		} catch (IOException e) {
			logger.error("定时任务启动失败", e);
			throw new IllegalStateException();
		}
		
	}
	
	// 初始化数据库
	public static void initHBase(Properties properties) {

		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// 如果是安全集群，则在登录之前需要验证
		if (User.isHBaseSecurityEnabled(configuration)) {

			try {
				// 设置登录的上下文名字为client
				HbaseLoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, Constant.USERNAME, Constant.USERKEYTABFILE);
				
				// 设置登录的登录SERVER_PRINCIPAL_KEY和DEFAULT_SERVER_PRINCIPAL
				HbaseLoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);

				// 设置user.keytab和krb5.conf
				HbaseLoginUtil.login(Constant.USERNAME, Constant.USERKEYTABFILE, Constant.KRB5FILE, configuration);
				
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
}
