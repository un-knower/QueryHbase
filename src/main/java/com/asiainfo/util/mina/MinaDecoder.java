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
        if(in.remaining() >= 60){//ǰ60�ֽ��ǰ�ͷ
            //��ǵ�ǰposition�Ŀ��ձ��mark���Ա��̵�reset�����ָܻ�positionλ��
            in.mark(); 
            byte[] l = new byte[60];
            in.get(l);        
            String head=new String(l,"ISO-8859-1");
            //�������ݳ���
            int len = StringUtil.ParseInt(head.toString().substring(18,26).trim());
            //ע�������get�����ᵼ�������remaining()ֵ�����仯
            if(in.remaining() < len-60){
                //�����Ϣ���ݲ����������ûָ�positionλ�õ�����ǰ,������һ��, ���������ݣ���ƴ�ճ���������
                in.reset();   
                return false;
            }else{
                //��Ϣ�����㹻
                in.reset();//���ûָ�positionλ�õ�����ǰ
                int sumlen = len;//�ܳ� = ��ͷ+����
                byte[] packArr = new byte[sumlen];
                in.get(packArr, 0 , sumlen);
           
                out.write(new String(packArr,"ISO-8859-1"));             
                if(in.remaining() > 0){//�����ȡһ�����������ݺ�ճ�˰������ø����ٵ���һ�Σ�������һ�ν���
                    return true;
                }
            }
        }
        return false;//����ɹ����ø�����н����¸���
    }
    
}
