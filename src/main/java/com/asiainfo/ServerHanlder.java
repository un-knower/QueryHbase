package com.asiainfo;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.resource.*;
import com.asiainfo.service.*;
import com.asiainfo.util.HeadMessage;
import com.asiainfo.util.StringUtil;

public class ServerHanlder extends IoHandlerAdapter{
	
	protected static final Logger logger = LoggerFactory.getLogger(ServerHanlder.class); 
	ServiceBridge servBridge = null;
	
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

        if(head.getCommand().startsWith(Constant.CMD_BIND_REQ_RSP)){
    		logger.info("请求登陆");
        	servBridge = new BindReqBridge(null, null);
        } else if(head.getCommand().startsWith(Constant.CMD_UNBIND_REQ_RSP)){
    		logger.info("请求解绑");
        	servBridge = new BindReqBridge(null, null);
        } else if(head.getCommand().startsWith(Constant.CMD_CDR_REQ)){
    		logger.info("清单查询");
    		servBridge = new ServiceReqBridge(new BOSSRecord(), Constant.CMD_CDR_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_CDR_RSP)){
        	logger.info("清单客户端确认");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_CDR_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_ACCT_REQ)){
        	logger.info("账单查询");
    		servBridge = new ServiceReqBridge(new CXBill(), Constant.CMD_ACCT_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_ACCT_RSP)){
        	logger.info("账单客户端确认");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_ACCT_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_ACCT_SUM_REQ)){
        	logger.info("和账单查询");
    		servBridge = new ServiceReqBridge(new HEBill(), Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_ACCT_SUM_RSP)){
        	logger.info("和账单客户端确认");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_ACCT_NET_USAGE_REQ)){
        	logger.info("流量账单查询");
    		servBridge = new ServiceReqBridge(new LLBill(), Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_ACCT_NET_USAGE_RSP)){
        	logger.info("流量账单客户端确认");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_HEFETION_REQ)){
    		logger.info("和飞信查询");
    		servBridge = new ServiceReqBridge(new FETIONBill(), Constant.CMD_HEFETION_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_HEFETION_RSP)){
        	logger.info("和飞信客户端确认");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_HEFETION_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_GROUP_REQ)){
        	logger.info("集团总账单查询");
    		servBridge = new ServiceReqBridge(new GROUPBill(), Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_GROUP_MX_REQ)){
        	logger.info("集团明细账单查询");
    		servBridge = new ServiceReqBridge(new GROUPMXBill(), Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_GROUP_DF_REQ)){
        	logger.info("集团代付账单查询");
    		servBridge = new ServiceReqBridge(new GROUPDFBill(), Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_GROUP_RSP)){
        	logger.info("集团账单客户端确认");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_HEARTBAG_REQ_RSP)){
    		logger.info("接收心跳");
        	servBridge = new HeartBeatBridge(null, null);
        } else {
        	logger.warn("无法识别的请求，报文头：" + message.toString().substring(0, 60)); 
        	return ;
        }
		
        servBridge.handler(session, head, bodyUndecrypt);
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

}
