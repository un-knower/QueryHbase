package com.asiainfo.util.mina;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import com.asiainfo.util.StringUtil;

public class MinaDecoder extends CumulativeProtocolDecoder  {

	 
    @Override
    public boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
            throws Exception {    	
        if(in.remaining() >= 60){//前60字节是包头
            //标记当前position的快照标记mark，以便后继的reset操作能恢复position位置
            in.mark(); 
            byte[] l = new byte[60];
            in.get(l);        
            String head=new String(l,"ISO-8859-1");
            //包的数据长度
            int len = StringUtil.ParseInt(head.toString().substring(18,26).trim());
            //注意上面的get操作会导致下面的remaining()值发生变化
            if(in.remaining() < len-60){
                //如果消息内容不够，则重置恢复position位置到操作前,进入下一轮, 接收新数据，以拼凑成完整数据
                in.reset();   
                return false;
            }else{
                //消息内容足够
                in.reset();//重置恢复position位置到操作前
                int sumlen = len;//总长 = 包头+包体
                byte[] packArr = new byte[sumlen];
                in.get(packArr, 0 , sumlen);
           
                out.write(new String(packArr,"ISO-8859-1"));             
                if(in.remaining() > 0){//如果读取一个完整包内容后还粘了包，就让父类再调用一次，进行下一次解析
                    return true;
                }
            }
        }
        return false;//处理成功，让父类进行接收下个包
    }
    
}
