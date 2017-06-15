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
	public String command;		// 10λ�������֣�ȫ��ĸ�����֣���󳤶�Ϊ9�������NULL����
    public String sequence;		// 8λ������sequence�ţ�ȫ���֣���󳤶�Ϊ7�������NULL������������ɷ�������ɣ�����ÿ��������Ӧ��һ������Ӧ�������0��ʼ��ţ����������һ������ж��Ӧ�������ֻ�����һ��Ӧ�����morePkt Ϊ0,��������morePkt ��Ϊ1���Ҷ��롣
    public String length;		// 8λ�������ܳ��ȣ�ȫ���֣�Ϊ��������Ϣ��ʵ�ʴ������ݳ���֮�͡���󳤶�Ϊ7�������NULL����������������ݼ��ܣ����Ǽ��ܺ�ĳ���
    public String system;		// 20λ���ͻ������ƣ����ܴ��ڿո��CRLF�������Ӧ�������Ϊ��������Ŀͻ������ƣ������NULL����
    public String encrypt_flag;	// 1λ�����������Ƿ���ܡ���1�����Ѽ���
    public String errcode;		// 1λ��������룬��Ӧ����ʹ��
    public String morepkt ;		// 1λ���Ƿ��к�������1�У�0��
    public String decompresslen;// 11λ�������ֶ�
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
