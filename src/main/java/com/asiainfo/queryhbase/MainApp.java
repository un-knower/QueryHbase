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

		System.out.println("开始初始化，请等待...");

		// 读取配置文件，默认为当前目录下，也可以使用运行参数配置
		String appContextPath = "./applicationContext.xml";
		if (args.length > 0) {
			appContextPath = args[0];
		}

		// 初始化
		FileSystemXmlApplicationContext appContext = new FileSystemXmlApplicationContext(appContextPath);
		Constant con = (Constant) appContext.getBean("property");
		try {
			con.initData();
			con.initLog();
			con.initDB();
			con.initHBase();
		} catch (IOException e) {
			System.out.println("错误:初始化固定参数失败");
			e.printStackTrace();
			appContext.close();
			return ;
		} finally {
			MysqlJdbcProcess.closeConnection();
		}
		ServerHanlder.setAppContext(appContext);
		LoadTimeTask.LoadTask();

		System.out.println("初始化结束，允许查询");
		Logger logger = LoggerFactory.getLogger(MainApp.class);
		logger.info("Start Complete");
		
		// 创建一个非阻塞的server端的Socket，因为这里是服务端所以用IoAcceptor
		NioSocketAcceptor acceptor = new NioSocketAcceptor(Constant.THREADNUM);   // Constant.THREADNUM
		// 添加一个日志过滤器
		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		// 添加一个编码过滤器
		acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new MinaCodecFactory()));
		// 添加一个线程池
		acceptor.getFilterChain().addLast("threadPool",new ExecutorFilter(Constant.THREADPOOLNUM));

		// 绑定业务处理器,这段代码要在acceptor.bind()方法之前执行，因为绑定套接字之后就不能再做这些准备
		acceptor.setHandler(new ServerHanlder());
		// 设置读取数据的缓冲区大小
		acceptor.getSessionConfig().setReadBufferSize(Constant.READBUFFERSIZE);
		// 设置输出数据的缓冲区大小
		acceptor.getSessionConfig().setSendBufferSize(Constant.WRITEBUFFERSIZE);
		// 每次执sessionIdle方法的时间间隔
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE,Constant.BLANKTIME);
		// 最大连接队列数
		acceptor.setBacklog(Constant.CONNECTPOOL);  
		// 绑定一个监听端口
		try {
			acceptor.bind(new InetSocketAddress(Constant.PORT));
		} catch (IOException e) {
			logger.error("", e);
			e.printStackTrace();
			System.exit(1);
		}
		
	}
}
