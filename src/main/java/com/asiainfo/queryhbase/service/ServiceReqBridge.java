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
		logger.info("请求串："+req);
        StringBuffer body = null;
        HashMap<String,String> map=StringUtil.toMAp(req);
        byte[] res=(byte[])session.getAttribute("result");
        session.setAttribute("queryStart", String.valueOf(System.currentTimeMillis()));
        
        // 结果数据发送耗时打印
        if (map.get("SubNo") != null) { // 清单、和飞信
	    	session.setAttribute("QueryInfo", map.get("SubNo")+"|"+map.get("BillPeriod")+"|"+map.get("BeginDate")
	    			+"|"+map.get("EndDate")+"|"+map.get("QueryTime")+"|"+map.get("CDRType")+"|"+map.get("AcctID"));
        } else { // 各类账单
	    	session.setAttribute("QueryInfo", map.get("MobileNo")+"|"+map.get("BillPeriod")+"|"+map.get("StartCycle")
	    			+"|"+map.get("EndCycle")+"|"+map.get("QueryTime")+"|"+map.get("CDRType")+"|"+map.get("AcctID"));    	
        }

        if (map.size() == 0) { // 请求串格式错误
    		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
        	head.setErrcode("3");
    		head.setLength(StringUtil.addSpaces(8, 60+""));
            session.write(toIoBuffer(head.toString(), "", session));
            return ;
        } else if (res.length != StringUtil.ParseInt((String)session.getAttribute("position"))) { // 缓存数据，客户端还没接收完
    		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
        	head.setErrcode("4");
    		head.setLength(StringUtil.addSpaces(8, 60+""));
            session.write(toIoBuffer(head.toString(), "", session));
            return ;
        }

    	body = record.GetResult(head, map);    
    	if ("9".equals(head.getErrcode())) { // 服务器严重错误
            session.write(toIoBuffer(head.toString(), body.toString(), session));
			System.exit(1);
    	}
    	    
        res = Encrypt.CXEncrypt(body.toString().getBytes("GBK"), ((String)session.getAttribute("code")).getBytes("ISO-8859-1"));
		session.setAttribute("result", res);
        session.setAttribute("position", "0");
        session.setAttribute("hbaseTime", String.valueOf(record.getHbaseTime()));
        session.setAttribute("transTime", String.valueOf(record.getTransTime()));
        session.setAttribute("hbaseRow", String.valueOf(record.getRowCnt()));
    	session.setAttribute("sendStart", String.valueOf(System.currentTimeMillis()));				// 结果数据发送开始时间
    	sendMultipulBag(session, head);

	}

	@Override
	protected void fillHead(HeadMessage head) {
		head.setCommand(StringUtil.addSpaces(10, super.rspCommand));
	}

}
