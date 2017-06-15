package com.asiainfo.queryhbase.util;

import java.io.UnsupportedEncodingException;

public class Encrypt {
	private static byte[] getCArr(byte[] ctx){
		byte[] result=new byte[ctx.length+1];
		for(int i=0;i<ctx.length;i++)result[i]=ctx[i];
		result[ctx.length]=0x00;
		return result;
	}
	
	public static byte[] CXEncrypt(byte[] ctx, byte[] password ) throws UnsupportedEncodingException
	{
	    // we will consider size of sbox 256 bytes
	    // (extra byte are only to prevent any mishep just in case)
		byte[] inp=getCArr(ctx);
		byte[] key=getCArr(password);
		byte[] Sbox=new byte[257];
		byte[] Sbox2=new byte[257];
	    int i, j, t, x;
	    int inplen=inp.length-1;
	    int keylen=key.length-1;
	    // this unsecured key is to be used only when there is no input key from user
	    byte[] OurUnSecuredKey = "www.systweak.com".getBytes("ISO-8859-1");
	    OurUnSecuredKey=getCArr(OurUnSecuredKey);
	    int OurKeyLen = OurUnSecuredKey.length;
	    byte temp, k;
	    i = j = t = x = 0;
	    k = temp = 0;
	    
	    
	    // initialize sbox i
	    for (i = 0; i < 256; i++) 
	    {
	        Sbox[i] = (byte)i;
	    }
	    
	    j = 0;
	    // whether user has sent any inpur key
	    if (keylen!=0) 
	    {
	        // initialize the sbox2 with user key
	        for (i = 0; i < 256; i++) 
	        {
	            if (j == keylen) 
	            {
	                j = 0;
	            }

	            Sbox2[i] = key[j++];
	        }
	    } 
	    else 
	    {
	        // initialize the sbox2 with our key
	        for (i = 0; i < 256; i++) 
	        {
	            if ((int)j == OurKeyLen) 
	            {
	                j = 0;
	            }

	            Sbox2[i] = OurUnSecuredKey[j++];
	        }
	    }
//	    for(i = 0; i < 256; i++)System.out.print(Sbox[i]+",");
	    j = 0 ; // Initialize j
	    // scramble sbox1 with sbox2
	    for (i = 0; i < 256; i++) 
	    {
	        j = (j + Sbox[i] + Sbox2[i]) % 256;
//	        
	        if(j<0)j=j+256;
//	        System.out.print(j+",");
	        temp =  Sbox[i];
	        Sbox[i] = Sbox[j];
	        Sbox[j] =  temp;
	        
	    }
	    i = j = 0;
	    for (x = 0; (int)x < inplen; x++) 
	    {
	        // increment i
	        i = (i + 1) % 256;
	        // increment j
	        j = (j + (Sbox[i])) % 256;
	        
	        // Scramble SBox #1 further so encryption routine will
	        // will repeat itself at great interval
	        if(j<0)j+=256;
	        temp = Sbox[i];
	        Sbox[i] = Sbox[j];
	        Sbox[j] = temp;
	        
	        // Get ready to create pseudo random  byte for encryption key
	        t = (Sbox[i]) + (Sbox[j]) % 256;
	        if(t<0)t+=256;
	        // get the random byte
	        k = Sbox[t];
//	        System.out.println(x+":"+k);
	        // xor with the data and done
	        inp[x] = (byte)(inp[x] ^ k);
	    }
	    byte res[]=new byte[inp.length-1];
	    for(i=0;i<inp.length-1;i++)res[i]=inp[i];
	    return res;
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		byte[] str=CXEncrypt("LoginResult=0".getBytes("GBK"),"1234567890".getBytes("ISO-8859-1"));
		System.out.println(new String(str));  
		System.out.println(new String(CXEncrypt(str,"1234567890".getBytes("ISO-8859-1")))); 
	}
}
