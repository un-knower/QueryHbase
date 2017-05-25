package com.asiainfo.util.mina;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

public class MinaCodecFactory implements ProtocolCodecFactory {
    
    private MinaDecoder decoder;
    private MinaEncoder encoder;
    
    public MinaCodecFactory() {
        encoder = new MinaEncoder();
        decoder = new MinaDecoder();
    }

 
    public ProtocolDecoder getDecoder(IoSession session) throws Exception {
        return decoder;
    }

 
    public ProtocolEncoder getEncoder(IoSession session) throws Exception {
        return encoder;
    }

}

