package com.asiainfo.queryhbase.util;

public class HeadMessage {
    @Override
	public String toString() {
		return  command + sequence
		        + length + system
				+ encrypt_flag + errcode
				+ morepkt + decompresslen
				;
	}
	public String command;		// 10位，命令字，全字母和数字，最大长度为9，以填充NULL结束
    public String sequence;		// 8位，包的sequence号，全数字，最大长度为7，以填充NULL结束，包序号由服务端生成，对于每个请求，相应的一个或多个应答包都从0开始编号，逐次连续加一。如果有多个应答包，则只有最后一个应答包的morePkt 为0,其它包的morePkt 都为1。右对齐。
    public String length;		// 8位，包的总长度，全数字，为包控制信息和实际传送内容长度之和。最大长度为7，以填充NULL结束。如果传送内容加密，则是加密后的长度
    public String system;		// 20位，客户端名称，不能存在空格和CRLF，如果是应答包，则为发起请求的客户端名称，以填充NULL结束
    public String encrypt_flag;	// 1位，传送内容是否加密。’1’：已加密
    public String errcode;		// 1位，出错代码，响应连接使用
    public String morepkt ;		// 1位，是否还有后续包，1有，0无
    public String decompresslen;// 11位，保留字段
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public String getSequence() {
		return sequence;
	}
	public void setSequence(String sequence) {
		this.sequence = sequence;
	}
	public String getLength() {
		return length;
	}
	public void setLength(String length) {
		this.length = length;
	}
	public String getSystem() {
		return system;
	}
	public void setSystem(String system) {
		this.system = system;
	}
	public String getEncrypt_flag() {
		return encrypt_flag;
	}
	public void setEncrypt_flag(String encrypt_flag) {
		this.encrypt_flag = encrypt_flag;
	}
	public String getErrcode() {
		return errcode;
	}
	public void setErrcode(String errcode) {
		this.errcode = errcode;
	}
	public String getMorepkt() {
		return morepkt;
	}
	public void setMorepkt(String morepkt) {
		this.morepkt = morepkt;
	}
	public String getDecompresslen() {
		return decompresslen;
	}
	public void setDecompresslen(String decompresslen) {
		this.decompresslen = decompresslen;
	}


}
