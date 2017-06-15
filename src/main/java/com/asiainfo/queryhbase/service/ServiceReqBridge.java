package com.asiainfo.queryhbase.service;

import java.util.HashMap;

import org.apache.mina.core.session.IoSession;

import com.asiainfo.queryhbase.resource.Record;
import com.asiainfo.queryhbase.util.HeadMessage;
import com.asiainfo.queryhbase.util.StringUtil;
import com.asiainfo.queryhbase.util.Encrypt;

public class ServiceReqBridge extends ServiceBridge {

	public ServiceReqBridge(Record record, String rspCommand) {
		super(record, rspCommand);
	}

	@Override
	public void handler(IoSession session, HeadMessage head, String bodyUndecrypt) throws Exception {
    	String req=new String(Encrypt.CXEncrypt(bodyUndecrypt.getBytes("ISO-8859-1"),((String)session.getAttribute("code")).getBytes("ISO-8859-1")),"ISO-8859-1");
		logger.info("���󴮣�"+req);
        StringBuffer body = null;
        HashMap<String,String> map=StringUtil.toMAp(req);
        byte[] res=(byte[])session.getAttribute("result");
        session.setAttribute("queryStart", String.valueOf(System.currentTimeMillis()));
        
        // ������ݷ��ͺ�ʱ��ӡ
        if (map.get("SubNo") != null) { // �嵥���ͷ���
	    	session.setAttribute("QueryInfo", map.get("SubNo")+"|"+map.get("BillPeriod")+"|"+map.get("BeginDate")
	    			+"|"+map.get("EndDate")+"|"+map.get("QueryTime")+"|"+map.get("CDRType")+"|"+map.get("AcctID"));
        } else { // �����˵�
	    	session.setAttribute("QueryInfo", map.get("MobileNo")+"|"+map.get("BillPeriod")+"|"+map.get("StartCycle")
	    			+"|"+map.get("EndCycle")+"|"+map.get("QueryTime")+"|"+map.get("CDRType")+"|"+map.get("AcctID"));    	
        }

        if (map.size() == 0) { // ���󴮸�ʽ����
    		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
        	head.setErrcode("3");
    		head.setLength(StringUtil.addSpaces(8, 60+""));
            session.write(toIoBuffer(head.toString(), "", session));
            return ;
        } else if (res.length != StringUtil.ParseInt((String)session.getAttribute("position"))) { // �������ݣ��ͻ��˻�û������
    		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
        	head.setErrcode("4");
    		head.setLength(StringUtil.addSpaces(8, 60+""));
            session.write(toIoBuffer(head.toString(), "", session));
            return ;
        }

    	body = record.GetResult(head, map);    
    	if ("9".equals(head.getErrcode())) { // ���������ش���
            session.write(toIoBuffer(head.toString(), body.toString(), session));
			System.exit(1);
    	}
    	    
        res = Encrypt.CXEncrypt(body.toString().getBytes("GBK"), ((String)session.getAttribute("code")).getBytes("ISO-8859-1"));
		session.setAttribute("result", res);
        session.setAttribute("position", "0");
        session.setAttribute("hbaseTime", String.valueOf(record.getHbaseTime()));
        session.setAttribute("transTime", String.valueOf(record.getTransTime()));
        session.setAttribute("hbaseRow", String.valueOf(record.getRowCnt()));
    	session.setAttribute("sendStart", String.valueOf(System.currentTimeMillis()));				// ������ݷ��Ϳ�ʼʱ��
    	sendMultipulBag(session, head);

	}

	@Override
	protected void fillHead(HeadMessage head) {
		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
	}

}
