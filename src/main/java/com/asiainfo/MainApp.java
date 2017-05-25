package com.asiainfo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.util.LoadTimeTask;
import com.asiainfo.util.MysqlJdbcProcess;
import com.asiainfo.util.mina.MinaCodecFactory;

public class MainApp {

	public static void main(String[] args) {

		System.out.println("��ʼ��ʼ������ȴ�...");
		
		// ��ȡ�����ļ���Ĭ��Ϊ��ǰĿ¼�£�Ҳ����ʹ�����в�������
		String propertiesPath = "./QuerySrv.properties";
		if (args.length > 0) {
			propertiesPath = args[0];
		}
		Properties properties = new Properties();
		FileInputStream inputFile;
		try {	
			inputFile = new FileInputStream(propertiesPath);
			properties.load(inputFile);
			inputFile.close();
		} catch (FileNotFoundException e) {
			System.out.println("\n����:�Ҳ��������ļ�\n Exception:" + e.getMessage());
			return;
		} catch (IOException e) {
			System.out.println("\n����:�����ļ���ȡʧ��\n Exception:" + e.getMessage());
			return;
		}

		// Constant �̶����������б��롢���ݿ����úͽӿ��߼��������Լ����������߼�
		try {
			Constant.initData(properties);
			Constant.initLog(properties);
			Constant.initDB(properties);
			Constant.initHBase(properties);
		} catch (Exception e) {
			System.out.println("����:��ʼ���̶�����ʧ��");
			e.printStackTrace();
			return;
		} finally {
			MysqlJdbcProcess.closeConnection();
		}

		LoadTimeTask.LoadTask();

		System.out.println("��ʼ�������������ѯ");
		Logger logger = LoggerFactory.getLogger(MainApp.class);
		logger.info("Start Complete");
		
		// ����һ����������server�˵�Socket����Ϊ�����Ƿ����������IoAcceptor
		NioSocketAcceptor acceptor = new NioSocketAcceptor(Constant.THREADNUM);   // Constant.THREADNUM
		// ���һ����־������
		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		// ���һ�����������
		acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new MinaCodecFactory()));
		// ���һ���̳߳�
		acceptor.getFilterChain().addLast("threadPool",new ExecutorFilter(Constant.THREADPOOLNUM));

		// ��ҵ������,��δ���Ҫ��acceptor.bind()����֮ǰִ�У���Ϊ���׽���֮��Ͳ���������Щ׼��
		acceptor.setHandler(new ServerHanlder());
		// ���ö�ȡ���ݵĻ�������С
		acceptor.getSessionConfig().setReadBufferSize(Constant.READBUFFERSIZE);
		// ����������ݵĻ�������С
		acceptor.getSessionConfig().setSendBufferSize(Constant.WRITEBUFFERSIZE);
		// ÿ��ִsessionIdle������ʱ����
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE,Constant.BLANKTIME);
		// ������Ӷ�����
		acceptor.setBacklog(Constant.CONNECTPOOL);  
		// ��һ�������˿�
		try {
			acceptor.bind(new InetSocketAddress(Constant.PORT));
		} catch (IOException e) {
			logger.error("", e);
			e.printStackTrace();
			System.exit(1);
		}
		
	}
}
