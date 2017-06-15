package com.asiainfo.queryhbase.service;

import org.apache.mina.core.session.IoSession;

import com.asiainfo.queryhbase.resource.Record;
import com.asiainfo.queryhbase.util.HeadMessage;
import com.asiainfo.queryhbase.util.StringUtil;

public class ServiceRspBridge extends ServiceBridge {

	public ServiceRspBridge(Record rec, String rspCommand) {
		super(rec, rspCommand);
	}

	@Override
	public void handler(IoSession session, HeadMessage head, String bodyUndecrypt) throws Exception {
    	int reSent = StringUtil.ParseInt((String)session.getAttribute("reSent"));
		
    	if(head.getErrcode().startsWith("1")){
    		if (++reSent > 3){
        		logger.info("�ط���������3�Σ����Ͽ�");
        		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
            	head.setErrcode("5");
                session.write(toIoBuffer(head.toString(), "", session));
                session.close(Boolean.TRUE);
    		} else {
    			logger.info("�ط���"+reSent+"��");
            	head.setErrcode("0");
    			session.setAttribute("reSent", reSent+"");
    			session.setAttribute("position", (String)session.getAttribute("preposition"));
    		}
    	} else {
    		session.setAttribute("reSent", "0");
    	}
    	
    	/* ����ͻ����Ƿ����������Ĳ��� ��һ���ͻ��˲���������ӣ���session.sequence�Ṳ�ã�
    	if (StringUtil.ParseInt(head.getSequence()) != StringUtil.ParseInt((String)session.getAttribute("sequence"))) {
			logger.info("���յ�sequence"+StringUtil.ParseInt(head.getSequence())
					+"������Ԥ�ڵ�sequence"+StringUtil.ParseInt((String)session.getAttribute("sequence")));
    		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
        	head.setErrcode("6");
            session.write(toIoBuffer(head.toString(), "", session));
            session.close(Boolean.TRUE);
    	} else {
    		session.setAttribute("sequence", StringUtil.ParseInt((String)session.getAttribute("sequence")) + 1 + "");
    	} */

    	sendMultipulBag(session, head);

	}

	@Override
	protected void fillHead(HeadMessage head) {
		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
		String[] tmp = head.getSequence().split("[^\\d*]");
		head.setSequence(StringUtil.addSpaces(8, String.format("%07d",StringUtil.ParseInt(tmp[0]) + 1)));
	}

}
