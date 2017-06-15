package com.asiainfo.queryhbase.service;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.queryhbase.Constant;
import com.asiainfo.queryhbase.resource.Record;
import com.asiainfo.queryhbase.util.HeadMessage;
import com.asiainfo.queryhbase.util.StringUtil;
import com.asiainfo.queryhbase.util.Encrypt;

public abstract class ServiceBridge  {
	
	protected static final Logger logger = LoggerFactory.getLogger(ServiceBridge.class); 
	protected static final Logger logger_stati = LoggerFactory.getLogger("statistics"); 
	protected String name = null;
	protected Record record = null;
	protected String rspCommand = null;
	
	public abstract void handler(IoSession session, HeadMessage head, String bodyUndecrypt) throws Exception;
	protected abstract void fillHead(HeadMessage head) ;

	public ServiceBridge(Record rec, String rspCommand){
		this.record = rec;
		this.rspCommand = rspCommand;
	}
	
	// 详单外其他内容的发送接口
	protected IoBuffer toIoBuffer(String head,String body,IoSession session) throws UnsupportedEncodingException {
    	byte[] b=Encrypt.CXEncrypt(body.getBytes("GBK"),((String)session.getAttribute("code")).getBytes("ISO-8859-1"));
 		IoBuffer buf = IoBuffer.allocate(60+b.length);
 		
 		buf.setAutoExpand(true);
        buf.put((head.substring(0,18)+StringUtil.addSpaces(8,60+b.length+"")+head.substring(26,49)+StringUtil.addSpaces(11, "0000000000")).getBytes());
        buf.put(b);
        buf.flip();
        
        String errcode = head.substring(47,48);
        if (!errcode.equals("0")) {
        	/*
        	 * errcode 说明
        	 * 1  登录失败
        	 * 2  查询超过条数(暂时不设置限制)
        	 * 3  请求串格式错误
        	 * 4  客户端还没接收完缓存数据
        	 * 5  重发次数超过3次
        	 * 6 sequence错误
        	 * 7 (预留)
        	 * 8 清单内容可能遇到问题，请检查
        	 * 9  服务器严重错误
        	 */
        	
        	String queryInfo = (String)session.getAttribute("QueryInfo");

        	if (errcode.equals("1")) {
        		queryInfo = "null|null|null|null|null|null|null";
        	}
        	
    		long now = System.currentTimeMillis();
    		long totalTime = 0;
    		if (!session.getAttribute("queryStart").equals("")) { // errcode=2/3/4/5/6
    			totalTime = now-StringUtil.ParseLong((String)session.getAttribute("queryStart"));
    		}
    		long sentTime = 0;
    		if (!session.getAttribute("sendStart").equals("")) { // errcode=4/5/6
    			sentTime = now-StringUtil.ParseLong((String)session.getAttribute("sendStart"));
    		}
    		long hbaseTime = 0;
    		if (!session.getAttribute("hbaseTime").equals("")) { // errcode=2/4/5/6
    			hbaseTime = StringUtil.ParseLong((String)session.getAttribute("hbaseTime"));
    		}
    		long transTime = 0;
    		if (!session.getAttribute("transTime").equals("")) { // errcode=2/4/5/6
    			transTime = StringUtil.ParseLong((String)session.getAttribute("transTime"));
    		}
    		int hbaseRow = 0;
    		if (!session.getAttribute("hbaseRow").equals("")) { // errcode=2/4/5/6
    			hbaseRow = StringUtil.ParseInt((String)session.getAttribute("hbaseRow"));
    		}
        	logger.info("# |"+queryInfo+"|"+hbaseRow+"|"+hbaseTime+"|"+transTime+"|"+sentTime+"|"+totalTime
        			+"|"+errcode+"|,fail");
        	logger_stati.info("# |"+Constant.IP+"|"+Constant.PORT+"|"+queryInfo+"|"+hbaseRow+"|"+hbaseTime+"|"+transTime+"|"+sentTime+"|"+totalTime
        			+"|"+errcode+"|,fail");
    		session.setAttribute("result", "".getBytes());
        	session.setAttribute("QueryInfo", "");		// 结果数据发送耗时打印
        	session.setAttribute("sendStart", "");		// 结果数据发送开始时间
            session.setAttribute("hbaseTime", "");
            session.setAttribute("transTime", "");
            session.setAttribute("hbaseRow", "");
            session.setAttribute("queryStart", "");
    		session.setAttribute("sequence", "");
        }
        
    	return buf;
    }

	// 详单发送接口
	protected void sendMultipulBag(IoSession session, HeadMessage head) throws UnsupportedEncodingException{
    	IoBuffer buf = IoBuffer.allocate(Constant.BAGSIZE);
		byte[] res = (byte [])session.getAttribute("result");
    	int position = StringUtil.ParseInt((String)session.getAttribute("position"));
		session.setAttribute("preposition", position+"");
    	int bodylen = 0;

    	if ((res == null) || (res.length <= position)){ 
    		return ;
    	} else if ((res.length - position) > Constant.BAGSIZE-60) {
    		bodylen = Constant.BAGSIZE - 60;
    		head.setMorepkt("1");
    	} else {
    		bodylen = res.length - position;
    		head.setMorepkt("0");
    	}
    	
    	fillHead(head);
		head.setDecompresslen(StringUtil.addSpaces(11, "0000000000"));
		head.setLength(StringUtil.addSpaces(8, bodylen+60+""));

 		buf.setAutoExpand(true);
        buf.put(head.toString().getBytes());
        buf.put(Arrays.copyOfRange(res, position, position+bodylen));
        buf.flip();
    	session.write(buf);
    	buf.free(); // 占用空间大，手动释放
    	position += bodylen;
		logger.info("发送分包 ["+head.getSequence().trim()+"|"+(bodylen+60)+"|"+head.getMorepkt()+"]");
		
    	if (head.getMorepkt().startsWith("0")){
    		long now = System.currentTimeMillis();
    		long totalTime = now-StringUtil.ParseLong((String)session.getAttribute("queryStart"));
    		long sentTime = now-StringUtil.ParseLong((String)session.getAttribute("sendStart"));
    		long hbaseTime = StringUtil.ParseLong((String)session.getAttribute("hbaseTime"));
    		long transTime = StringUtil.ParseLong((String)session.getAttribute("transTime"));
    		int hbaseRow = StringUtil.ParseInt((String)session.getAttribute("hbaseRow"));
        	logger.info("# |"+(String)session.getAttribute("QueryInfo")+"|"+hbaseRow+"|"+hbaseTime
        			+"|"+transTime+"|"+sentTime+"|"+totalTime+ "|0|,success");
        	logger_stati.info("# |"+Constant.IP+"|"+Constant.PORT+"|"+(String)session.getAttribute("QueryInfo")+"|"+hbaseRow+"|"+hbaseTime
        			+"|"+transTime+"|"+sentTime+"|"+totalTime+ "|0|,success");
    		res = "".getBytes();
    		session.setAttribute("result", res);
        	session.setAttribute("QueryInfo", "");		// 结果数据发送耗时打印
        	session.setAttribute("sendStart", "");		// 结果数据发送开始时间
            session.setAttribute("hbaseTime", "");
            session.setAttribute("transTime", "");
            session.setAttribute("hbaseRow", "");
            session.setAttribute("queryStart", "");
    		session.setAttribute("sequence", "");
    		position = 0;
    	}
    	session.setAttribute("position", position+"");
        session.setAttribute("sumSubBagBlankTime", "0");
    }
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Record getRecord() {
		return record;
	}
	public void setRecord(Record record) {
		this.record = record;
	}
	public String getRspCommand() {
		return rspCommand;
	}
	public void setRspCommand(String rspCommand) {
		this.rspCommand = rspCommand;
	}

}
