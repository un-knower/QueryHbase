package com.asiainfo.queryhbase.service;

import org.apache.mina.core.session.IoSession;

import com.asiainfo.queryhbase.resource.Record;
import com.asiainfo.queryhbase.util.HeadMessage;

public class BindRelieaseBridge extends ServiceBridge {

	public BindRelieaseBridge(Record rec, String rspCommand) {
		super(rec, rspCommand);
	}

	@Override
	public void handler(IoSession session, HeadMessage head,
			String bodyUndecrypt) throws Exception {
        session.write(toIoBuffer(head.toString(), "", session));
        session.close(Boolean.TRUE);
	}

	@Override
	protected void fillHead(HeadMessage head) {
	}

}
