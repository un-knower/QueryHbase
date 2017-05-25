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

	// 增加或减少天数
	private static Date addDay(Date date, int num) {
		Calendar startDT = Calendar.getInstance();
		startDT.setTime(date);
		startDT.add(Calendar.DAY_OF_MONTH, num);
		return startDT.getTime();
	}

	public static boolean LoadTask() {

		Calendar calendar = Calendar.getInstance();
		/*** 定制每日2:00执行方法 ***/
		calendar.set(Calendar.HOUR_OF_DAY, Constant.DBRELOAD_TIME);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);

		Date date = calendar.getTime(); 
		date = addDay(date, 1); // 第一次执行定时任务的时间，第二天开始第一次

		Timer t = new Timer();
		t.scheduleAtFixedRate(new TimerTask() {

			public void run() {
				Constant.initDB();
			}
		}, date, PERIOD_DAY);

		return true;

	}

}
