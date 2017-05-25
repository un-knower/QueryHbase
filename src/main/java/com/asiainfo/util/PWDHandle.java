package com.asiainfo.util;

import java.util.Scanner;


/**
 * 对密码串进行处理
 * 
 * @author 
 * 
 */
public class PWDHandle {
	/* 密码种子 */
	private final static int[] seed = { 26, 115, 193, 27, 82, 162, 42, 85, 226,
			138, 56, 61, 49, 23, 174, 137 };

	/**
	 * 对字符（整数码）进行加密
	 * 
	 * @param c
	 * @param idx
	 * @return
	 */
	public static int encode(int c, int idx) {
		int i = (idx % 8) * 2;
		int itmp = c ^ seed[i];
		itmp = (itmp + seed[i + 1]) % 256;
		return itmp;
	}

	/**
	 * 对字符串（密码串）进行加密
	 * 
	 * @param pwdIn
	 * @return
	 */
	public static String encrypt(String pwdIn) {
		int ior = 0;
		int k = 'A';
		int length = pwdIn.length();
		// char[] pwdOut = new char[pwdIn.length() * 2];
		String pwdOut = "";
		for (int i = 0; i < length; i++) {
			int c = pwdIn.charAt(i) ^ ior;
			ior = c;
			c = encode(c, i);
			// pwdOut[i * 2] = (char) (c >> 4 + k);
			// pwdOut[i * 2 + 1] = (char) (c & 15 + k);
			pwdOut += (char) ((c >> 4) + k);
			pwdOut += (char) ((c & 15) + k);
		}

		// return String.copyValueOf(pwdOut);
		return pwdOut;
	}

	/**
	 * 对字符（整数码）进行解密
	 * 
	 * @param c
	 * @param idx
	 * @return
	 */
	public static int decode(int c, int idx) {
		int i = (idx % 8) * 2;
		int itmp = (c + 256 - seed[i + 1]) % 256;
		itmp ^= seed[i];
		return itmp;
	}

	/**
	 * 对字符串（密码串）进行解密
	 * 
	 * @param pwdOut
	 * @return
	 */
	public static String decrypt(String pwdOut) {
		int ior = 0;
		int k = 'A';
		int length = pwdOut.length() / 2;
		String pwdIn = "";

		for (int i = 0; i < length; i++) {
			int c = (pwdOut.charAt(2 * i) - k) << 4;
			c += (pwdOut.charAt(i * 2 + 1) - k);
			c = decode(c, i);
			c ^= ior;
			ior ^= c;
			pwdIn += (char) c;
		}

		return pwdIn;
	}

	public static void main(String[] args) {
		String in = null;
		
		@SuppressWarnings("resource")
		Scanner sc = new Scanner(System.in);
		while (true) {
			System.out.println("---begin---");
			System.out.print("输入选项e或者d选择加密或者解密：");
			in = sc.nextLine();
			
			if (in.startsWith("e")){
				System.out.print("输入明文：");
				in = sc.nextLine();
				System.out.println("密文为：" + encrypt(in));
				
			} else if (in.startsWith("d")){
				System.out.print("输入密文：");
				in = sc.nextLine();
				System.out.println("密文为：" + decrypt(in));
				
			} else {
				System.out.println("选择只能为e或d");
			}
			System.out.println("---end---");
		}
	}

}
