package com.asiainfo.util;

import java.util.Scanner;


/**
 * �����봮���д���
 * 
 * @author 
 * 
 */
public class PWDHandle {
	/* �������� */
	private final static int[] seed = { 26, 115, 193, 27, 82, 162, 42, 85, 226,
			138, 56, 61, 49, 23, 174, 137 };

	/**
	 * ���ַ��������룩���м���
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
	 * ���ַ��������봮�����м���
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
	 * ���ַ��������룩���н���
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
	 * ���ַ��������봮�����н���
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
			System.out.print("����ѡ��e����dѡ����ܻ��߽��ܣ�");
			in = sc.nextLine();
			
			if (in.startsWith("e")){
				System.out.print("�������ģ�");
				in = sc.nextLine();
				System.out.println("����Ϊ��" + encrypt(in));
				
			} else if (in.startsWith("d")){
				System.out.print("�������ģ�");
				in = sc.nextLine();
				System.out.println("����Ϊ��" + decrypt(in));
				
			} else {
				System.out.println("ѡ��ֻ��Ϊe��d");
			}
			System.out.println("---end---");
		}
	}

}
