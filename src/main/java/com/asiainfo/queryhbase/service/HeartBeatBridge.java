package com.asiainfo.queryhbase.service;

import org.apache.mina.core.session.IoSession;

import com.asiainfo.queryhbase.resource.Record;
import com.asiainfo.queryhbase.util.HeadMessage;

public class HeartBeatBridge extends ServiceBridge {

	public HeartBeatBridge(Record rec, String rspCommand) {
		super(rec, rspCommand);
	}

	@Override
	public void handler(IoSession session, HeadMessage head,
			String bodyUndecrypt) throws Exception {
        session.write(toIoBuffer(head.toString(), "", session));
	}

	@Override
	protected void fillHead(HeadMessage head) {
	}

}
