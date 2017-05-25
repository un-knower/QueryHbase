package com.asiainfo.util;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.Constant;

public class LoadTimeTask {

	public static final Logger logger = LoggerFactory.getLogger(LoadTimeTask.class);  
	private static final long PERIOD_DAY = 24 * 60 * 60 * 1000;

	// ���ӻ��������
	private static Date addDay(Date date, int num) {
		Calendar startDT = Calendar.getInstance();
		startDT.setTime(date);
		startDT.add(Calendar.DAY_OF_MONTH, num);
		return startDT.getTime();
	}

	public static boolean LoadTask() {

		Calendar calendar = Calendar.getInstance();
		/*** ����ÿ��2:00ִ�з��� ***/
		calendar.set(Calendar.HOUR_OF_DAY, Constant.DBRELOAD_TIME);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);

		Date date = calendar.getTime(); 
		date = addDay(date, 1); // ��һ��ִ�ж�ʱ�����ʱ�䣬�ڶ��쿪ʼ��һ��

		Timer t = new Timer();
		t.scheduleAtFixedRate(new TimerTask() {

			public void run() {
				Constant.initDB();
			}
		}, date, PERIOD_DAY);

		return true;

	}

}
