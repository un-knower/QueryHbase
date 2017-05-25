package com.asiainfo.service;

import java.util.HashMap;

import org.apache.mina.core.session.IoSession;

import com.asiainfo.Constant;
import com.asiainfo.resource.Record;
import com.asiainfo.util.Encrypt;
import com.asiainfo.util.HeadMessage;
import com.asiainfo.util.StringUtil;

public class BindReqBridge extends ServiceBridge {

	public BindReqBridge(Record rec, String rspCommand) {
		super(rec, rspCommand);
	}

	@Override
	public void handler(IoSession session, HeadMessage head,
			String bodyUndecrypt) throws Exception {
    	String req=null;
    	HashMap<String,String> map=null;
    	
		if (Constant.BILL_USER_MAP == null || Constant.BILL_USER_MAP.get(head.getSystem().trim()) == null) {
			head.setErrcode("1");
            session.write(toIoBuffer(head.toString(), "LoginResult=1", session));
    		logger.error("��½ʧ��");
            session.close(Boolean.TRUE);  
		} else {
        	req=new String(Encrypt.CXEncrypt(bodyUndecrypt.getBytes("ISO-8859-1"), 
        			Constant.BILL_USER_MAP.get(head.getSystem().trim()).getBytes("ISO-8859-1")),"ISO-8859-1");
		}
    	
		logger.info("���󴮣�"+req);
    	map=StringUtil.toMAp(req);
    	
    	if (map.size() != 2 || map.get("RandomCode") == null) {
			head.setErrcode("1");
            session.write(toIoBuffer(head.toString(), "LoginResult=1", session));
    		logger.error("��½ʧ��");
    		return ;
    	} else {
    		session.setAttribute("code",(String)map.get("RandomCode"));
        	session.write(toIoBuffer(head.toString(), "LoginResult=0", session));
    		logger.info("��½�ɹ�");
    	}
	}

	@Override
	protected void fillHead(HeadMessage head) {
	}

}
