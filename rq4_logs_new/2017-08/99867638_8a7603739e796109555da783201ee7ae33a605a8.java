package com.saramdl.chinese_calendar;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class LunarConjunction extends GregorianCalendar {
	int year, month, day, hour, minute; // ���� ���Ϸ�
	
	public LunarConjunction() {
		// TODO Auto-generated constructor stub
	}

	public LunarConjunction(TimeZone arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	public LunarConjunction(Locale arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	public LunarConjunction(TimeZone arg0, Locale arg1) {
		super(arg0, arg1);
		// TODO Auto-generated constructor stub
	}

	public LunarConjunction(int arg0, int arg1, int arg2) {
		super(arg0, arg1, arg2);
		// TODO Auto-generated constructor stub
	}

	public LunarConjunction(int arg0, int arg1, int arg2, int arg3, int arg4) {
		super(arg0, arg1, arg2, arg3, arg4);
		// TODO Auto-generated constructor stub
	}

	public LunarConjunction(int arg0, int arg1, int arg2, int arg3, int arg4, int arg5) {
		super(arg0, arg1, arg2, arg3, arg4, arg5);
		// TODO Auto-generated constructor stub
	}
	
	
	// Ư������(udate)���κ��� tmin�� ������ ��¥�� ���ϱ�
	public Calendar getDateByMin(long tmin, int year, int month, int day, int hour, int minute) {
		Calendar cal = new GregorianCalendar(year, month-1, day, hour, minute);
		
		long millis = cal.getTimeInMillis() + tmin;
		cal.setTimeInMillis(millis);
		
		return cal;
	}

	/**
	 * ���� �ջ� (�̹��� ���Ϸ�) �ð� ���ϱ�
	 * @param syear
	 * @param smonth
	 * @param sday
	 * @return
	 */
	public Calendar getPreviousConjunction(int syear, int smonth, int sday) {
		long dm = disp2days(syear, smonth, sday, 1995, 12, 31);
		double dem = moonsundegree(dm);
		double d = dm;
		double de = dem;
		
		while (de < 13.5) {
		    d = d - 1;
			de = moonsundegree(d);
		}
		
		while (de > 1) {
		    d = d - 0.04166666666;
			de = moonsundegree(d);
		}
		
		while (de < 359.99) {
		    d = d - 0.000694444;
			de = moonsundegree(d);
		}
		
		d = d + 0.375;
		d = d * 1440 ;
		long i = -1 * (int) d;
		Calendar pc = getDateByMin(i, 1995, 12, 31, 0, 0);
		
		year = pc.get(Calendar.YEAR);
	  	month = pc.get(Calendar.MONTH) + 1;
	    day = pc.get(Calendar.DAY_OF_MONTH);
	    hour = pc.get(Calendar.HOUR);
	    minute = pc.get(Calendar.MINUTE);

		return pc;
	}
	
	/**
	 * ���� �ջ�(������ ���Ϸ�) �ð� ���ϱ�
	 * @param syear
	 * @param smonth
	 * @param sday
	 * @return
	 */
	public Calendar getNextConjunction(int syear, int smonth, int sday) {
		long dm = disp2days(syear, smonth, sday, 1995, 12, 31);
		double dem = moonsundegree(dm);
		double d = dm;
		double de = dem;
		
		while (de < 346.5) {
		  d = d + 1;
		  de = moonsundegree(d);
		}
		
		while (de < 359) {
		  d = d + 0.04166666666;
		  de = moonsundegree(d);
		}
		
		while (de>0.01) {
		  d = d + 0.000694444;
		  de = moonsundegree(d);
		}
		
		double pd = d;
		
		d = d + 0.375;
		d = d * 1440;
		long  i = -1 * (int) d;
		Calendar pc = getDateByMin(i, 1995, 12, 31, 0, 0);
		
		// �Է³�¥�� ������ ���Ϸ��̸� 
		if ( (smonth == pc.get(Calendar.MONTH) + 1) && (sday == pc.get(Calendar.DAY_OF_MONTH)) ) {
			year = pc.get(Calendar.YEAR);
		  	month = pc.get(Calendar.MONTH) + 1;
		    day = pc.get(Calendar.DAY_OF_MONTH);
		    hour = pc.get(Calendar.HOUR);
		    minute = pc.get(Calendar.MINUTE);
		
		    d = pd;
		
		    while (de < 347) {
		    	d = d + 1;
		      	de = moonsundegree(d);
		    }
		    
		    while (de < 359) {
		    	d = d + 0.04166666666;
		    	de = moonsundegree(d);
		    }
		    
		    while (de > 0.01) {
		    	d = d + 0.000694444;
		    	de = moonsundegree(d);
		    }
		
		    d = d + 0.375;
		    d = d * 1440 ;
		    i = -1 * (int) (d);
			pc = getDateByMin(i, 1995, 12, 31, 0, 0);
		}
		
		return pc;	
	}

	public Calendar getFullMoon(int syear, int smonth, int sday) {
		double d = disp2days(year, month, day, 1995, 12, 31); // ���� ���Ϸ�
	  	d = d + 12; // ���� 12��
		double de = moonsundegree(d);

		while (de < 166.5) {
		    d = d + 1;
		    de = moonsundegree(d);
		}
	
		while (de < 179) {
			d = d + 0.04166666666;
			de = moonsundegree(d);
		}
	
		while (de < 179.99) {
			d = d + 0.000694444;
			de = moonsundegree(d);
		}
	
		d = d + 0.375;
		d = d * 1440;
		int i = -1 * (int) d;
		Calendar pc = getDateByMin(i, 1995, 12, 31, 0, 0);
		
		return pc;	
	}
	
	
	/**
	 * �¾�Ȳ��� ��Ȳ���� ����
	 * @param day
	 * @return 0 => �ջ�, 180 => ��
	 */
	public static double moonsundegree(double day) {
		// 1996�� ����
		double sl = day * 0.98564736 + 278.956807;		// ��� Ȳ��
		double smin = 282.869498 + 0.00004708 * day;	//������ Ȳ��
		double sminangle = Math.PI * (sl - smin) / 180 ;		// �����̰�
		double sd = 1.919 * Math.sin(sminangle) + 0.02 * Math.sin(2*sminangle); // Ȳ����
		double sreal = degreelow(sl + sd) ; 			// ��Ȳ�� 
		
		double ml = 27.836584 + 13.17639648 * day; 		// ���Ȳ��
		double mmin = 280.425774 + 0.11140356 * day; 	// ������ Ȳ��
		double mminangle = Math.PI * (ml-mmin) / 180; 	// �����̰�
		double msangle = 202.489407 - 0.05295377 * day; // ����Ȳ��
		double msdangle = Math.PI*(ml-msangle) / 180; 	// �����̰�
		double md = 5.068889 * Math.sin(mminangle) + 0.146111 * Math.sin(2 * mminangle) + 0.01 * Math.sin(3 * mminangle)
		       - 0.238056 * Math.sin(sminangle) - 0.087778 * Math.sin(mminangle + sminangle)  // Ȳ����
		       + 0.048889 * Math.sin(mminangle - sminangle) - 0.129722 * Math.sin(2 * msdangle)
		       - 0.011111 * Math.sin(2*msdangle - mminangle) - 0.012778 * Math.sin(2 * msdangle + mminangle);
		double mreal = degreelow(ml + md) ; // ��Ȳ�� 

		return degreelow(mreal - sreal);
	}
	
	/**
	 * ������ 0~360�� �̳��� ����
	 * @param d
	 * @return
	 */
	public static double degreelow(double d) {
		long i;
		double di;
		
		di = d;
		i = (int) di;
		i = i / 360;
		di = di - (360 * i);
		
		while (di >= 360 || di < 0) {
			if (di > 0) 
				di = di - 360;
			else 
				di = di + 360;
		}
		
		return di;
	}
	
	// y1,m1,d1�Ϻ��� y2,m2,d2������ �ϼ� ���
	public static int disp2days(int year1, int month1, int day1, int year2, int month2, int day2) {
		Calendar date1 = new GregorianCalendar(year1, month1 - 1, day1);
		Calendar date2 = new GregorianCalendar(year2, month2 - 1, day2);
		
	    return (int) (date2.getTimeInMillis() - date1.getTimeInMillis()) / (24 * 60 * 60 * 1000);
	}

}