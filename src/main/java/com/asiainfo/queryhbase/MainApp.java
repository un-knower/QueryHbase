package com.asiainfo.queryhbase;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.queryhbase.util.LoadTimeTask;
import com.asiainfo.queryhbase.util.MysqlJdbcProcess;
import com.asiainfo.queryhbase.util.mina.MinaCodecFactory;

public class MainApp {

	public static void main(String[] args) {

		System.out.println("��ʼ��ʼ������ȴ�...");

		// ��ȡ�����ļ���Ĭ��Ϊ��ǰĿ¼�£�Ҳ����ʹ�����в�������
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// ��ʼ��
		FileSystemXmlApplicationContext appContext = new FileSystemXmlApplicationContext(appContextPath);
		Constant con = (Constant) appContext.getBean("property");
		try {
			con.initData();
			con.initLog();
			con.initDB();
			con.initHBase();
		} catch (IOException e) {
			System.out.println("����:��ʼ���̶�����ʧ��");
			e.printStackTrace();
			appContext.close();
			return ;
		} finally {
			MysqlJdbcProcess.closeConnection();
		}
		ServerHanlder.setAppContext(appContext);
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
