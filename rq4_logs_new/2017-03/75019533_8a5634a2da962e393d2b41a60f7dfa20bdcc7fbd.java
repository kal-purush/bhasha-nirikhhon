package Classes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.swing.JOptionPane;

public class DefaultStateLawCalculation implements StateLawCalculable {
	private static final String stateName = "Default";
	private static final String stateAbbrv = "USA";
	private static final BigDecimal stateWPP = new BigDecimal("13.0"); 
	private static final int stateDaysToLate = 30; 
	private static final SimpleTimeZone timeZone = new SimpleTimeZone(0, "Standard");

	public DefaultStateLawCalculation() {
		// TODO Auto-generated constructor stub
	}
	@Override
	public ArrayList<Paycheck> addAndTrimToPriorWages(Paycheck pc, ArrayList<Paycheck> pchecks, CompClaim cHist) {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		//long mWeek = mDay * 7;
		String message = "";
		String eol = System.getProperty("line.separator");
		boolean unaddable = priorWagesIsComplete(pchecks);
		if(unaddable){
			message ="Paycheck cannot be added. Total time period of prior wages entered meets Missouri Law criteria.";
			JOptionPane.showMessageDialog(null, message);
			return pchecks;
		}
		Paycheck newPC = null;
		//Calendar pcPD = pc.getPaymentDate();
		//long pcPDate = pcPD.getTimeInMillis();
		Calendar priorWeekStart = new GregorianCalendar(timeZone);
		priorWeekStart.setTime(cHist.getPriorWeekStart().getTime());
		Calendar earliestPriorWageDate =  new GregorianCalendar(timeZone);
		earliestPriorWageDate.setTime(cHist.getEarliestPriorWageDate().getTime());
		Calendar pcPPS = new GregorianCalendar(timeZone);
		pcPPS.setTime(pc.getPayPeriodStart().getTime());
		long mPCPPS = pcPPS.getTimeInMillis();
		SimpleDateFormat formatter = new SimpleDateFormat("MMM-dd-yyyy");
		formatter.setLenient(false);
		formatter.setTimeZone(timeZone);
		java.util.Date ePWD = earliestPriorWageDate.getTime();
		//System.out.println("EPW Date: "+formatter.format(ePWD));
		//System.out.println("PWS Date: "+formatter.format(priorWeekStart.getTime()));
		//Date dPPS = (Date) pcPPS.getTime();
		 
		long mPWeekEnd =  priorWeekStart.getTimeInMillis() + (mDay * 6);
		GregorianCalendar pWeekEnd = new GregorianCalendar(timeZone);
		pWeekEnd.setTimeInMillis(mPWeekEnd);
		Calendar priorWeekEnd = this.normalizeCalendarTime(pWeekEnd);
		Calendar pcPPE = new GregorianCalendar(timeZone);
		pcPPE.setTime(pc.getPayPeriodEnd().getTime());
		//Date dPPE = (Date) pcPPE.getTime();
		java.util.Date dPWE = priorWeekEnd.getTime();
		

		if(pcPPS.compareTo(priorWeekEnd) > 0){
			message = "Invalid paycheck start date. Pay Period Start Date must be before the end of the week immediately prior to week of injury."+eol+
					"Date Entered: "+formatter.format(pcPPS.getTime());
			JOptionPane.showMessageDialog(null, message);
			return pchecks;
		}
		else if(pcPPS.compareTo(earliestPriorWageDate) < 0){
			

			//if period overlaps ePWD, alter the paycheck to represent only applicable portion (days after ePWD)
			if (pcPPE.compareTo(earliestPriorWageDate) > 0){
				long pcE = pcPPE.getTimeInMillis();
				long mEPW = earliestPriorWageDate.getTimeInMillis();
				long mNewPP = (pcE - mEPW)+mDay;
				int PPDays = (int) Math.ceil(mNewPP / mDay);

				pc.setPayPeriodStart(earliestPriorWageDate);
				//String pG = String.valueOf(mNewPP / (pcE - mPCPPS));
				BigDecimal pcPeriod = new BigDecimal(String.valueOf(pcE)).subtract(new BigDecimal(String.valueOf(mPCPPS)));
				BigDecimal percentGross = new BigDecimal(String.valueOf(mNewPP)).divide(pcPeriod, 4, RoundingMode.HALF_EVEN);
				BigDecimal gA = pc.getGrossAmount();
				BigDecimal pGAMult = gA.multiply(percentGross);
				BigDecimal nG = pGAMult.setScale(2, RoundingMode.HALF_EVEN);
				String newGross = String.valueOf(nG);
				pc.setGrossAmount(newGross);
				message = "Only last " + String.valueOf(PPDays) + " Days of submitted Pay Check entered and calculated for Gross Amount due to earliest accepted date for prior wages relative to date of injury";
				JOptionPane.showMessageDialog(null, message);
				//System.out.print("If work hours used to calculate pay during this period were not evenly distributed for the above number of days,");
				//System.out.print("please manually enter Gross Income ONLY for the hours worked this pay period STARTING on " + formatter.format(ePWD) +".");
				//System.out.println("Gross Amount is currently set at: " + pc.getGrossAmount() + "Enter '1' to set Gross Income manually ONLY if meeting above criteria.");
				//System.out.println("Enter '2' to confirm and finish adding Paycheck.");
				//int selection = s.nextInt();
				/*while(selection < 1 || selection > 2){
					System.out.println("Invalid input.");
					System.out.println("Please Select an Option:");
					System.out.println("1) Enter Manual Gross Income for pay period starting on " + formatter.format(ePWD) +".");
					System.out.println("2) Confirm and finish adding Paycheck.");
					selection = s.nextInt();
				}

				if (selection == 1){
					System.out.println("Enter Manual Gross Income for wages earned by hours worked from " + formatter.format(ePWD) +" through " + formatter.format(dPPE) + ":");
					String gI = s.next();
					pc.setGrossAmount(gI);
				}*/
			}
			else{
				message = "Invalid paycheck end date. Pay Period End Date must be after " + formatter.format(ePWD) + " based on date of injury in accordance with State law.";
				System.out.println("Invalid PPE Date: MOCalc - first condition. Date: "+formatter.format(pcPPE.getTime()));
				JOptionPane.showMessageDialog(null, message);
				return pchecks;
			}	
		}
		else if (pcPPE.compareTo(priorWeekEnd) > 0){
			//if PPE is past PWE, alter the paycheck to represent days up to PWE
			if (pcPPS.compareTo(priorWeekEnd) < 0){
				long pcE = pcPPE.getTimeInMillis();
				
				long mNewPP = (mPWeekEnd - mPCPPS)+mDay;
				int PPDays = (int) Math.ceil(mNewPP / mDay);
				//pc.setPayPeriodEnd(priorWeekEnd);
				//String pG = String.valueOf(mNewPP / (pcE - mPCPPS));
				BigDecimal pcPeriod = new BigDecimal(String.valueOf(pcE)).subtract(new BigDecimal(String.valueOf(mPCPPS)));
				BigDecimal percentGross = new BigDecimal(String.valueOf(mNewPP)).divide(pcPeriod, 4, RoundingMode.HALF_EVEN);
				BigDecimal gA = pc.getGrossAmount();
				BigDecimal pGAMult = gA.multiply(percentGross);
				BigDecimal nG = pGAMult.setScale(2, RoundingMode.HALF_EVEN);
				String newGross = String.valueOf(nG);
				//pc.setGrossAmount(newGross);
				newPC = new Paycheck(newGross, pc.getPaymentDate(), pc.getPayPeriodStart(), priorWeekEnd);
				message = "Only first " + String.valueOf(PPDays) + " Days of entered Pay Check entered and calculated for Gross Amount due to last accepted date for prior wages relative to date of injury (end of prior week)";
				JOptionPane.showMessageDialog(null, message);
				/*System.out.println("If work hours used to calculate pay during this period were not evenly distributed for the above number of days,");
				System.out.println("please manually enter Gross Income ONLY for the hours worked this pay period STARTING on " + formatter.format(dPPS) + " through " + formatter.format(dPWE) + ".");
				System.out.println("Gross Amount is currently set at: " + pc.getGrossAmount() + "Enter '1' to set Gross Income manually ONLY if meeting above criteria.");
				System.out.println("Enter '2' to confirm and finish adding Paycheck.");
				int selection = s.nextInt();
				while(selection < 1 || selection > 2){
					System.out.println("Invalid input.");
					System.out.println("Please Select an Option:");
					System.out.println("1) Enter Manual Gross Income for pay period starting on " + formatter.format(dPPS) + " through " + formatter.format(dPWE) + ".");
					System.out.println("2) Confirm and finish adding Paycheck.");
					selection = s.nextInt();
				}

				if (selection == 1){
					System.out.println("Enter Manual Gross Income for wages earned by hours worked from " + formatter.format(dPPS) +" through " + formatter.format(dPWE) + ":");
					String gI = s.next();
					pc.setGrossAmount(gI);
				}*/
			}
			else{
				message = "Invalid paycheck start date. Pay Period Start Date must be before " + formatter.format(dPWE) + " based on date of injury in accordance with State law.";
				System.out.println("Invalid PPS Date: MOCalc - last condition. Date: "+formatter.format(pcPPS.getTime()));
				JOptionPane.showMessageDialog(null, message);
				return pchecks;
			}	
		}
		if (newPC != null){
			pchecks.add(newPC);
			//System.out.println(newPC.toString());
		}
		else{
			pchecks.add(pc);
			//System.out.println(pc.toString());
		}
		cHist.setPriorWages(pchecks);
		System.out.println("Prior Wages: "+eol+cHist.listPriorWages());
		return pchecks;
	}

	@Override
	public ArrayList<TPDPaycheck> addTPDWorkPaycheck(TPDPaycheck pc, ArrayList<TPDPaycheck> pchecks, Calendar priorWeekStart, Calendar lightDutyStart) throws Exception {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		String message = "";
		Calendar pcPPS = pc.getPayPeriodStart();
		//long pcPPSDate = pcPPS.getTimeInMillis();
		//SimpleDateFormat formatter = new SimpleDateFormat("MMM-dd-yyyy");
		//formatter.setLenient(false);
		//Date dPPS = (Date) pcPPS.getTime();
		 
		long mPWeekEnd =  priorWeekStart.getTimeInMillis() + (mDay * 6);
		GregorianCalendar pWeekEnd = new GregorianCalendar(timeZone);
		pWeekEnd.setTimeInMillis(mPWeekEnd);
		Calendar priorWeekEnd = this.normalizeCalendarTime(pWeekEnd);
		Calendar pcPPE = pc.getPayPeriodEnd();
		//Date dPPE = (Date) pcPPE.getTime();
		//Date dPWE = (Date) priorWeekEnd.getTime();
		if (lightDutyStart != null){
			if(pc.getPayPeriodStart().before(lightDutyStart)){
				pc.setPayPeriodStart(lightDutyStart);
			}
		}

		if(pcPPE.compareTo(priorWeekEnd) <= 0){
			message = "Invalid paycheck end date. Pay Period End Date must be after the end of the week immediately prior to week of injury.";
			JOptionPane.showMessageDialog(null, message);
			return pchecks;
		}
		else if(pcPPS.compareTo(priorWeekEnd) <= 0){
			throw new Exception("Paycheck Start Date must be trimmed using ReimbursementSummary.trimWorkPayment(Paycheck, totalHrsWorked, weekInjHrsWorked)");
		}
		pchecks.add(pc);
		
		
		return pchecks;
	}

	@Override
	public ArrayList<WorkCompPaycheck> addWCPaycheck(WorkCompPaycheck wcPC, ArrayList<WorkCompPaycheck> wcPayments, Calendar priorWeekStart) {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		long mWeek = mDay * 7;
		String message = "";
		
		//Calendar wcPRD = pc.getPaymentDate();
		//long wcPRDate = wcPRD.getTimeInMillis();
		Calendar pcPPS = wcPC.getPayPeriodStart();
		//long pcPPSDate = pcPPS.getTimeInMillis();
		SimpleDateFormat formatter = new SimpleDateFormat("MMM-dd-yyyy");
		formatter.setLenient(false);
		long mEPPS = priorWeekStart.getTimeInMillis() + mWeek;
		GregorianCalendar ePPSt = new GregorianCalendar(timeZone);
		Date epcPPS = new Date(mEPPS);
		ePPSt.setTime(epcPPS);
		Calendar ePPS = this.normalizeCalendarTime(ePPSt);
		//Date dPPS = pcPPS.getTime();

		//Calendar pcPPE = pc.getPayPeriodEnd();
		//Date dPPE = pcPPE.getTime();

		if(pcPPS.compareTo(ePPS) < 0){
			message = "Invalid paycheck start date. Pay Period Start Date must be on or after " + formatter.format(epcPPS) + " based on date of injury in accordance with Missouri law.";
			JOptionPane.showMessageDialog(null, message);
			return wcPayments;
		}
		
		wcPayments.add(wcPC);
		return wcPayments;
	}
	
	@Override
	public ArrayList<WorkCompPaycheck> addWCPaycheckNoKnownPP(WorkCompPaycheck wcPC, ArrayList<WorkCompPaycheck> wcPayments, Calendar priorWeekStart) {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		long mWeek = mDay * 7;
		String message = "";
		
		Calendar wcPRD = wcPC.getPaymentDate();
		SimpleDateFormat formatter = new SimpleDateFormat("MMM-dd-yyyy");
		formatter.setLenient(false);
		long mEPPS = priorWeekStart.getTimeInMillis() + mWeek;
		GregorianCalendar ePPSt = new GregorianCalendar(timeZone);
		Date epcPPS = new Date(mEPPS);
		ePPSt.setTime(epcPPS);
		Calendar ePPS = this.normalizeCalendarTime(ePPSt);

		if(wcPRD.compareTo(ePPS) < 0){
			message = "Invalid paycheck pay date. Pay Date must be on or after " + formatter.format(epcPPS) + " based on date of injury in accordance with Missouri law.";
			JOptionPane.showMessageDialog(null, message);
			return wcPayments;
		}
		
		wcPayments.add(wcPC);
		return wcPayments;
	}
	
	public Paycheck[] splitDateInjuredPayPeriodChecks(Paycheck pc, CompClaim cHist){
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		Paycheck[] splitPCs = {new Paycheck(), new TPDPaycheck()};
		final Paycheck fpc = pc;
		Paycheck pw = this.addAndTrimToPriorWages(pc, new ArrayList<Paycheck>(), cHist).get(0);
		splitPCs[0] = pw;
		long pwPPE = pw.getPayPeriodEnd().getTimeInMillis();
		Calendar tpdPPS = new GregorianCalendar(timeZone);
		tpdPPS.setTimeInMillis(pwPPE + mDay);
		
		TPDPaycheck tpdPC = new TPDPaycheck(fpc.getGrossAmount().subtract(pw.getGrossAmount()).toString(), fpc.getPaymentDate(),
				tpdPPS, fpc.getPayPeriodEnd(), this);
		splitPCs[1] = tpdPC;
		
		return splitPCs;
	}

	@Override
	public BigDecimal computeAnyLatePaymentCompensation(BigDecimal grossAmnt, BigDecimal calculatedWeeklyPayment) {
		BigDecimal tenth = calculatedWeeklyPayment.multiply(new BigDecimal("0.10"));
		BigDecimal tenthAdded = calculatedWeeklyPayment.add(tenth);
		BigDecimal diff = tenthAdded.subtract(grossAmnt);
		return diff.setScale(2, RoundingMode.HALF_EVEN);
	}

	@Override
	public BigDecimal computeAvgPriorGrossWeeklyPayment(ArrayList<Paycheck> priorWages) {
		String pWT = "0.00";
		BigDecimal priorWT = new BigDecimal(pWT);
		BigDecimal priorWageTotal = priorWT.setScale(2, RoundingMode.HALF_EVEN);
		
		for (Paycheck p : priorWages){
			priorWageTotal = priorWageTotal.add(p.getGrossAmount());
		}
		
		BigDecimal avgPriorGrossWeeklyPayment = priorWageTotal.divide(stateWPP, 2, RoundingMode.HALF_EVEN);
		return avgPriorGrossWeeklyPayment;
	}

	@Override
	public BigDecimal computeCalculatedWeeklyPayment(BigDecimal avgPGrossWeekPay) {
		BigDecimal two3 = new BigDecimal("2").divide(new BigDecimal("3"), 20, RoundingMode.HALF_UP);
		BigDecimal cWP = avgPGrossWeekPay.multiply(two3);
		System.out.println("Calculated Weelkly Payment (unrounded): $"+cWP.toPlainString());
		return cWP.setScale(2, RoundingMode.HALF_EVEN);
	}

	/* probably wont need, will delete or implement later 
	 * @Override
	public BigDecimal computeAmountNotPaid(ArrayList<WorkCompPaycheck> wcPayments, BigDecimal calcWP) {
		String aNP = "0.00";
		BigDecimal amountNotPaid = new BigDecimal(aNP);
		
		for (WorkCompPaycheck p : this.wcPayments){
			BigDecimal aSO = this.calculatedWeeklyPayment.subtract(p.getGrossAmount());
			amountNotPaid = amountNotPaid.add(aSO);
			aSO = aSO.setScale(2, RoundingMode.HALF_EVEN);
			p.setAmountStillOwed(aSO);
		}
		amountNotPaid = amountNotPaid.setScale(2, RoundingMode.HALF_EVEN);
		this.amountNotPaid = amountNotPaid;		return null;
	}*/

	@Override
	public Calendar computeEarliestPriorWageDate(Calendar priorWeekStart) {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		long mWeek = mDay * 7;
		long mPWS = priorWeekStart.getTimeInMillis();
		
		//based on Missouri Law (13 weeks of paychecks starting with the week immediately preceding the week injured)
		long mEPW = (mPWS - (mWeek * 12));
		GregorianCalendar ePWDate = new GregorianCalendar(timeZone);
		ePWDate.setTimeInMillis(mEPW);
		return this.normalizeCalendarTime(ePWDate);
	}

	@Override
	//formulas based on law at: http://www.moga.mo.gov/mostatutes/stathtml/28700001801.html 
	//returns Work Comp supplemental payment amount (BigDecimal) for hours actually worked during TPD period. Amount calculated based on Missouri Law.
	public BigDecimal computeWCSupplementalPayment(TPDPaycheck workPayment, BigDecimal avgPriorGrossWeeklyPayment) {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		long mWeek = mDay * 7;
		//long days = workPayment.getDaysInPayPeriod();
		long mPP = (workPayment.getPayPeriodEnd().getTimeInMillis() + mDay) - workPayment.getPayPeriodStart().getTimeInMillis();
		//long mPPDays = (long) Math.ceil(mPP/mDay);
		
		//System.out.println("Days in Pay Period(sLC manual): "+mPPDays);
		//System.out.println("Days in Pay Period(sLC): "+days);
		//long mDR = mPP % mWeek;
		//long daysRemaining = Math.round((mPP % mWeek) / mDay);
		//BigDecimal week = new BigDecimal(String.valueOf(mWeek));
		//BigDecimal weekPercentRemainder =  week.divide(new BigDecimal(String.valueOf(Math.round((mPP % mWeek) / mDay))), RoundingMode.UNNECESSARY);
		BigDecimal payPWeeks = (new BigDecimal(String.valueOf(mPP)).divide(new BigDecimal(String.valueOf(mWeek)), 20, RoundingMode.HALF_UP));
		BigDecimal ppSupplementalPayment = 
				(avgPriorGrossWeeklyPayment.multiply(payPWeeks).subtract(workPayment.getGrossAmount())).multiply(new BigDecimal("2").divide(new BigDecimal("3"), 20, RoundingMode.HALF_UP));

		return ppSupplementalPayment.compareTo(new BigDecimal("0")) > 0 ? ppSupplementalPayment.setScale(2, RoundingMode.HALF_EVEN): new BigDecimal("0");
	}

	@Override
	public boolean determineAndSetIsLate(Calendar payPeriodEnd, Calendar payReceived) {
		boolean isLate = false;
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		//long mWeek = mDay * 7;
		long mPPE = payPeriodEnd.getTimeInMillis();
		long mPRD = payReceived.getTimeInMillis();
		int daysLate = (int) Math.ceil((mPRD - mPPE) / mDay);
		if(daysLate > stateDaysToLate){
			isLate = true;
		}
		else{
			isLate = false;
		}		
		return isLate;
	}
	
	//ensures Calendar time is set to 00:00 on same date
	@Override
	public Calendar normalizeCalendarTime(Calendar calendar) {
		if (calendar.get(Calendar.HOUR) == 0 && calendar.get(Calendar.HOUR_OF_DAY) == 0 && calendar.get(Calendar.MINUTE) == 0 && calendar.get(Calendar.SECOND) == 0 && calendar.get(Calendar.MILLISECOND) == 0){
			return calendar;
		}
		GregorianCalendar newCal = new GregorianCalendar(timeZone);
		//long dstOffSet = calendar.get(Calendar.DST_OFFSET);
		
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int date =  calendar.get(Calendar.DATE);
		long offSet = calendar.getTimeZone().getOffset(calendar.getTimeInMillis());
		calendar.setTimeInMillis(calendar.getTimeInMillis() + offSet);
		if (calendar.get(Calendar.YEAR) != year){
			calendar.set(Calendar.YEAR, year);
		}
		if (calendar.get(Calendar.MONTH) != month){
			calendar.set(Calendar.MONTH, month);
		}
		if (calendar.get(Calendar.DATE) != date){
			calendar.set(Calendar.DATE, date);
		}
		
	    newCal.setLenient(false);
	    newCal.set(Calendar.YEAR, year);
	    newCal.set(Calendar.MONTH, month);
	    newCal.set(Calendar.DATE, date);
	    newCal.set(Calendar.AM_PM, Calendar.AM);
	    newCal.set(Calendar.HOUR, 0);
	    newCal.set(Calendar.HOUR_OF_DAY, 0);
	    newCal.set(Calendar.MINUTE, 0);
	    newCal.set(Calendar.SECOND, 0);
	    newCal.set(Calendar.MILLISECOND, 0);
	    
	    /*
	    long newOffset = newCal.getTimeZone().getOffset(newCal.getTimeInMillis());
	    if (newOffset != 0){
	    	newCal.setTimeInMillis(newCal.getTimeInMillis() + newOffset);
	    	if (newCal.get(Calendar.YEAR) != year){
				newCal.set(Calendar.YEAR, year);
			}
			if (newCal.get(Calendar.MONTH) != month){
				newCal.set(Calendar.MONTH, month);
			}
			if (newCal.get(Calendar.DATE) != date){
				newCal.set(Calendar.DATE, date);
			}
			newCal.set(Calendar.AM_PM, Calendar.AM);
		    newCal.set(Calendar.HOUR, 0);
		    newCal.set(Calendar.HOUR_OF_DAY, 0);
		    newCal.set(Calendar.MINUTE, 0);
		    newCal.set(Calendar.SECOND, 0);
		    newCal.set(Calendar.MILLISECOND, 0);
	    }
	    
	    
	    long dstOffSet = newCal.get(Calendar.DST_OFFSET);
	    if (newCal.get(Calendar.HOUR) != 0 || newCal.get(Calendar.HOUR_OF_DAY) != 0){
	    	newCal.setTimeInMillis(newCal.getTimeInMillis() - dstOffSet);
	    }
	    */
	  
	    return newCal;
	}
	@Override
	public SortedMap<Paycheck, Integer> sortPCHashMapByDate(Map<Paycheck, Integer> pcMap){
		//HashMap<Paycheck, Integer> result = new HashMap<Paycheck, Integer>();
		
		SortedMap<Paycheck, Integer> map =
		        new TreeMap<Paycheck, Integer>(new Comparator<Paycheck>(){
		        		@Override
	        			public int compare(Paycheck p1, Paycheck p2) {
    				
    				return p1.compareTo(p2.getPayPeriodStart());
            	}});
		map.putAll(pcMap);
        //sort by PPS Date of key
        /*pcMap.entrySet().stream().sorted(Map.Entry.<Paycheck, Integer>comparingByKey(new Comparator<Paycheck>(){
                	@Override
        			public int compare(Paycheck p1, Paycheck p2) {
        				
        				return p1.compareTo(p2.getPayPeriodStart());
                	}}))
                .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));
        */
        return map;
	}
	
	@Override
	public SortedMap<WorkCompPaycheck, Integer> sortWCPCHashMapByDate(Map<WorkCompPaycheck, Integer> wcpcMap){
		//HashMap<WorkCompPaycheck, Integer> result = new HashMap<WorkCompPaycheck, Integer>();
		
		SortedMap<WorkCompPaycheck, Integer> map =
		        new TreeMap<WorkCompPaycheck, Integer>(new Comparator<WorkCompPaycheck>(){
		        		@Override
	        			public int compare(WorkCompPaycheck p1, WorkCompPaycheck p2) {
    				
    				return p1.compareTo(p2.getPayPeriodStart());
            	}});
		map.putAll(wcpcMap);
		
		//sort by PPS Date of key
		/*wcpcMap.entrySet().stream().sorted(Map.Entry.<WorkCompPaycheck, Integer>comparingByKey(new Comparator<WorkCompPaycheck>(){
        	@Override
			public int compare(WorkCompPaycheck p1, WorkCompPaycheck p2) {
				
				return p1.compareTo(p2.getPayPeriodStart());
        	}}))
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));
		*/
		
		return map;
	}

	@Override
	public String getStateAbbrv() {
		return DefaultStateLawCalculation.stateAbbrv;
	}
	
	public int getStateDaysToLate(){
		return DefaultStateLawCalculation.stateDaysToLate;
	}
	
	@Override
	public String getStateName(){
		return DefaultStateLawCalculation.stateName;
	}
	public BigDecimal getStateWeeksPriorPeriod(){
		return DefaultStateLawCalculation.stateWPP;
	}
	
	@Override
	public TimeZone getTimeZone(){
		return DefaultStateLawCalculation.timeZone;
	}

	@Override
	public boolean priorWagesIsComplete(ArrayList<Paycheck> priorWages) {
		long mDay = (1000 * 60 * 60 * 24); // 24 hours in milliseconds
		long mWeek = mDay * 7;
		long mPeriod = 0;
		for (Paycheck p1 : priorWages){
			long mP1S = p1.getPayPeriodStart().getTimeInMillis();
			long mP1E = p1.getPayPeriodEnd().getTimeInMillis();
			mPeriod += (mP1E - mP1S)+mDay;
		}
		if (stateWPP.compareTo(BigDecimal.valueOf(Math.ceil(mPeriod / mWeek))) > 0){
			return false;
		}
		else{
			return true;
		}		
	}

	@Override
	public boolean isWithinTPDPeriod(ArrayList<Paycheck> tpdWork, Paycheck pc) {
		for (int i = 0, j=tpdWork.size()-1; i < j; i++, j--){
			if(pc.doPayPeriodsOverlap(tpdWork.get(i)) || pc.doPayPeriodsOverlap(tpdWork.get(j))) return true; 
			if (j - i == 2){
				i++;
				return pc.doPayPeriodsOverlap(tpdWork.get(i));
				
			}
		}
		return false;
	}
	
	
	/* for testing purposes
	public void main(){
		WorkCompPaycheck wc = new WorkCompPaycheck();
		BigDecimal test = computeWCSupplementalPayment(wc, new BigDecimal("500"));
	}*/
}
	
