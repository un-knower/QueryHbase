package com.asiainfo.queryhbase;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.asiainfo.queryhbase.service.ServiceBridge;
import com.asiainfo.queryhbase.util.HeadMessage;
import com.asiainfo.queryhbase.util.StringUtil;

public class ServerHanlder extends IoHandlerAdapter{
	
	protected static final Logger logger = LoggerFactory.getLogger(ServerHanlder.class); 
	protected static FileSystemXmlApplicationContext appContext;
	
    @Override  
    public void messageReceived(IoSession session, Object message) throws Exception {
    	String m=message.toString();
        HeadMessage head=new HeadMessage();
        head.setCommand(m.substring(0,10));
        head.setSequence(m.substring(10,18));
        head.setLength(m.substring(18,26));
        head.setSystem(m.substring(26,46));
        head.setEncrypt_flag(m.substring(46,47));
        head.setErrcode(m.substring(47,48));
        head.setMorepkt(m.substring(48,49));
        head.setDecompresslen(m.substring(49,60));
    	String bodyUndecrypt = m.substring(60); 
    	
    	if (session.getAttribute("result") == null) session.setAttribute("result", "".getBytes());	// 结果数据
    	if (session.getAttribute("position") == null) session.setAttribute("position", "");			// 当前发送数据的起点
    	if (session.getAttribute("preposition") == null) session.setAttribute("preposition", "");	// 上一次发送数据的起点，用于超时重发
    	if (session.getAttribute("code") == null) session.setAttribute("code", "");					// 登陆后续密码 
    	if (session.getAttribute("reSent") == null) session.setAttribute("reSent", "");				// 重连次数
    	if (session.getAttribute("QueryInfo") == null) session.setAttribute("QueryInfo", "");		// 统计基本信息
    	if (session.getAttribute("hbaseTime") == null) session.setAttribute("hbaseTime", "");		// hbase查询时间
    	if (session.getAttribute("transTime") == null) session.setAttribute("transTime", "");		// 翻译时间
    	if (session.getAttribute("hbaseRow") == null) session.setAttribute("hbaseRow", "");			// hbase查询后记录数
    	if (session.getAttribute("sendStart") == null) session.setAttribute("sendStart", "");		// 结果数据发送开始时间
    	if (session.getAttribute("queryStart") == null) session.setAttribute("queryStart", "");		// 结果数据发送开始时间
    	if (session.getAttribute("sequence") == null) session.setAttribute("sequence", "");			// 当前sequence
        session.setAttribute("sumBlankTime", "0");

    	try {
    		ServiceBridge servBridge = (ServiceBridge) appContext.getBean(head.getCommand());
			logger.info(servBridge.getName());
	        servBridge.handler(session, head, bodyUndecrypt);
    	} catch (BeansException e) {
        	logger.warn("无法识别的请求，报文头：" + message.toString().substring(0, 60)); 
        	return ;
    	}
    }
  
    @Override  
    public void sessionIdle(IoSession session, IdleStatus status)throws Exception { 
    	logger.info("server-服务端进入空闲状态..");
    	
        session.setAttribute("sumSubBagBlankTime", StringUtil.ParseInt((String) session.getAttribute("sumSubBagBlankTime"))+Constant.BLANKTIME+"");
    	if((((byte [])session.getAttribute("result")).length != 0) && (StringUtil.ParseInt((String) session.getAttribute("sumSubBagBlankTime")) > Constant.SUBBAG_OVERTIME)){
    		logger.warn("server-服务端将关闭连接，接受客户端确认超时："+Constant.SUBBAG_OVERTIME+"s");
            session.close(Boolean.TRUE);
    	}
    	
    	session.setAttribute("sumBlankTime", StringUtil.ParseInt((String)session.getAttribute("sumBlankTime"))+Constant.BLANKTIME+"");
    	if(StringUtil.ParseInt((String)session.getAttribute("sumBlankTime")) >= Constant.OVERTIME) {
    		logger.warn("server-服务端将关闭连接，客户端空闲超时："+Constant.OVERTIME+"s");
            session.close(Boolean.TRUE);
    	} 
    } 
  
    @Override  
    public void sessionOpened(IoSession session) throws Exception {  
        logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx"); 
        logger.info("server-服务端与客户端连接打开...");  
        session.setAttribute("sumBlankTime", "0");
        session.setAttribute("sumSubBagBlankTime", "0");
    } 
    
    @Override  
    public void sessionClosed(IoSession session) throws Exception {  
        logger.info("server-session关闭连接断开"); 
        logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx");
    }

	public static FileSystemXmlApplicationContext getAppContext() {
		return appContext;
	}

	public static void setAppContext(FileSystemXmlApplicationContext appContext) {
		ServerHanlder.appContext = appContext;
	}  

}
