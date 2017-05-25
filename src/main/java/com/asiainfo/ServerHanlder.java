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
    	
    	if (session.getAttribute("result") == null) session.setAttribute("result", "".getBytes());	// �������
    	if (session.getAttribute("position") == null) session.setAttribute("position", "");			// ��ǰ�������ݵ����
    	if (session.getAttribute("preposition") == null) session.setAttribute("preposition", "");	// ��һ�η������ݵ���㣬���ڳ�ʱ�ط�
    	if (session.getAttribute("code") == null) session.setAttribute("code", "");					// ��½�������� 
    	if (session.getAttribute("reSent") == null) session.setAttribute("reSent", "");				// ��������
    	if (session.getAttribute("QueryInfo") == null) session.setAttribute("QueryInfo", "");		// ͳ�ƻ�����Ϣ
    	if (session.getAttribute("hbaseTime") == null) session.setAttribute("hbaseTime", "");		// hbase��ѯʱ��
    	if (session.getAttribute("transTime") == null) session.setAttribute("transTime", "");		// ����ʱ��
    	if (session.getAttribute("hbaseRow") == null) session.setAttribute("hbaseRow", "");			// hbase��ѯ���¼��
    	if (session.getAttribute("sendStart") == null) session.setAttribute("sendStart", "");		// ������ݷ��Ϳ�ʼʱ��
    	if (session.getAttribute("queryStart") == null) session.setAttribute("queryStart", "");		// ������ݷ��Ϳ�ʼʱ��
    	if (session.getAttribute("sequence") == null) session.setAttribute("sequence", "");			// ��ǰsequence
        session.setAttribute("sumBlankTime", "0");

        if(head.getCommand().startsWith(Constant.CMD_BIND_REQ_RSP)){
    		logger.info("�����½");
        	servBridge = new BindReqBridge(null, null);
        } else if(head.getCommand().startsWith(Constant.CMD_UNBIND_REQ_RSP)){
    		logger.info("������");
        	servBridge = new BindReqBridge(null, null);
        } else if(head.getCommand().startsWith(Constant.CMD_CDR_REQ)){
    		logger.info("�嵥��ѯ");
    		servBridge = new ServiceReqBridge(new BOSSRecord(), Constant.CMD_CDR_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_CDR_RSP)){
        	logger.info("�嵥�ͻ���ȷ��");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_CDR_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_ACCT_REQ)){
        	logger.info("�˵���ѯ");
    		servBridge = new ServiceReqBridge(new CXBill(), Constant.CMD_ACCT_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_ACCT_RSP)){
        	logger.info("�˵��ͻ���ȷ��");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_ACCT_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_ACCT_SUM_REQ)){
        	logger.info("���˵���ѯ");
    		servBridge = new ServiceReqBridge(new HEBill(), Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_ACCT_SUM_RSP)){
        	logger.info("���˵��ͻ���ȷ��");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_ACCT_NET_USAGE_REQ)){
        	logger.info("�����˵���ѯ");
    		servBridge = new ServiceReqBridge(new LLBill(), Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_ACCT_NET_USAGE_RSP)){
        	logger.info("�����˵��ͻ���ȷ��");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_ACCT_SUM_OR_NET_USAGE_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_HEFETION_REQ)){
    		logger.info("�ͷ��Ų�ѯ");
    		servBridge = new ServiceReqBridge(new FETIONBill(), Constant.CMD_HEFETION_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_HEFETION_RSP)){
        	logger.info("�ͷ��ſͻ���ȷ��");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_HEFETION_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_GROUP_REQ)){
        	logger.info("�������˵���ѯ");
    		servBridge = new ServiceReqBridge(new GROUPBill(), Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_GROUP_MX_REQ)){
        	logger.info("������ϸ�˵���ѯ");
    		servBridge = new ServiceReqBridge(new GROUPMXBill(), Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_GROUP_DF_REQ)){
        	logger.info("���Ŵ����˵���ѯ");
    		servBridge = new ServiceReqBridge(new GROUPDFBill(), Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_CLIENT_GROUP_RSP)){
        	logger.info("�����˵��ͻ���ȷ��");
    		servBridge = new ServiceRspBridge(null, Constant.CMD_GROUP_RSP);
        } else if(head.getCommand().startsWith(Constant.CMD_HEARTBAG_REQ_RSP)){
    		logger.info("��������");
        	servBridge = new HeartBeatBridge(null, null);
        } else {
        	logger.warn("�޷�ʶ������󣬱���ͷ��" + message.toString().substring(0, 60)); 
        	return ;
        }
		
        servBridge.handler(session, head, bodyUndecrypt);
    }
  
    @Override  
    public void sessionIdle(IoSession session, IdleStatus status)throws Exception { 
    	logger.info("server-����˽������״̬..");
    	
        session.setAttribute("sumSubBagBlankTime", StringUtil.ParseInt((String) session.getAttribute("sumSubBagBlankTime"))+Constant.BLANKTIME+"");
    	if((((byte [])session.getAttribute("result")).length != 0) && (StringUtil.ParseInt((String) session.getAttribute("sumSubBagBlankTime")) > Constant.SUBBAG_OVERTIME)){
    		logger.warn("server-����˽��ر����ӣ����ܿͻ���ȷ�ϳ�ʱ��"+Constant.SUBBAG_OVERTIME+"s");
            session.close(Boolean.TRUE);
    	}
    	
    	session.setAttribute("sumBlankTime", StringUtil.ParseInt((String)session.getAttribute("sumBlankTime"))+Constant.BLANKTIME+"");
    	if(StringUtil.ParseInt((String)session.getAttribute("sumBlankTime")) >= Constant.OVERTIME) {
    		logger.warn("server-����˽��ر����ӣ��ͻ��˿��г�ʱ��"+Constant.OVERTIME+"s");
            session.close(Boolean.TRUE);
    	} 
    } 
  
    @Override  
    public void sessionOpened(IoSession session) throws Exception {  
        logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx"); 
        logger.info("server-�������ͻ������Ӵ�...");  
        session.setAttribute("sumBlankTime", "0");
        session.setAttribute("sumSubBagBlankTime", "0");
    } 
    
    @Override  
    public void sessionClosed(IoSession session) throws Exception {  
        logger.info("server-session�ر����ӶϿ�"); 
        logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx");
    }  

}
