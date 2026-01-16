package com.tjhx.vo;

import java.math.BigDecimal;

/**
 * 督导统计信息一览
 */
public class MngUserStatisticsDetail {
	/** 督导员 */
	private String mngUserId;
	/** 督导员 */
	private String mngUserName;
	/** 店铺数量 */
	private int orgCnt;
	/** 本月任务 */
	private BigDecimal saleMonthTargetAmt = new BigDecimal(0);
	/** 当前已完成 */
	private BigDecimal amtByNow = new BigDecimal("0");
	/** 当前完成率 */
	private BigDecimal cmpRateByNow = new BigDecimal("0");
	/** 月预计完成 */
	private BigDecimal expMonthAmt = new BigDecimal("0");
	/** 月预计完成率 */
	private BigDecimal expCmpRateByNow = new BigDecimal("0");
	/** 任务增长金额 */
	private BigDecimal taskGrowthamt = new BigDecimal("0");
	/** 任务完成店数（全部） */
	private int taskCmpOrgCnt;
	/** 任务完成店数百分比 */
	private BigDecimal taskCmpOrgRate = new BigDecimal("0");
	/** 当月店任务（预计）贡献率（区域） */
	private BigDecimal expAmtRate = new BigDecimal("0");
	/** 当月店销售贡献率（区域） */
	private BigDecimal amtRate = new BigDecimal("0");
	/** 当月销售贡献指数增幅（区域） */
	private BigDecimal ratescope = new BigDecimal("0");

	/**
	 * 取得督导员
	 * 
	 * @return 督导员
	 */
	public String getMngUserId() {
		return mngUserId;
	}

	/**
	 * 设置督导员
	 * 
	 * @param mngUserId 督导员
	 */
	public void setMngUserId(String mngUserId) {
		this.mngUserId = mngUserId;
	}

	/**
	 * 取得督导员
	 * 
	 * @return 督导员
	 */
	public String getMngUserName() {
		return mngUserName;
	}

	/**
	 * 设置督导员
	 * 
	 * @param mngUserName 督导员
	 */
	public void setMngUserName(String mngUserName) {
		this.mngUserName = mngUserName;
	}

	/**
	 * 取得店铺数量
	 * 
	 * @return 店铺数量
	 */
	public int getOrgCnt() {
		return orgCnt;
	}

	/**
	 * 设置店铺数量
	 * 
	 * @param orgCnt 店铺数量
	 */
	public void setOrgCnt(int orgCnt) {
		this.orgCnt = orgCnt;
	}

	/**
	 * 取得本月任务
	 * 
	 * @return 本月任务
	 */
	public BigDecimal getSaleMonthTargetAmt() {
		return saleMonthTargetAmt;
	}

	/**
	 * 设置本月任务
	 * 
	 * @param saleMonthTargetAmt 本月任务
	 */
	public void setSaleMonthTargetAmt(BigDecimal saleMonthTargetAmt) {
		this.saleMonthTargetAmt = saleMonthTargetAmt;
	}

	/**
	 * 取得当前已完成
	 * 
	 * @return 当前已完成
	 */
	public BigDecimal getAmtByNow() {
		return amtByNow;
	}

	/**
	 * 设置当前已完成
	 * 
	 * @param amtByNow 当前已完成
	 */
	public void setAmtByNow(BigDecimal amtByNow) {
		this.amtByNow = amtByNow;
	}

	/**
	 * 取得当前完成率
	 * 
	 * @return 当前完成率
	 */
	public BigDecimal getCmpRateByNow() {
		/** 当前已完成/本月任务 */
		if (null != saleMonthTargetAmt & null != amtByNow & saleMonthTargetAmt.compareTo(BigDecimal.ZERO) == 1
				& amtByNow.compareTo(BigDecimal.ZERO) == 1) {
			cmpRateByNow = amtByNow.divide(saleMonthTargetAmt, 4, BigDecimal.ROUND_UP);
			cmpRateByNow = cmpRateByNow.multiply(new BigDecimal("100"));
			cmpRateByNow = cmpRateByNow.divide(new BigDecimal("1"), 2, BigDecimal.ROUND_UP);
		}
		return cmpRateByNow;
	}

	/**
	 * 设置当前完成率
	 * 
	 * @param cmpRateByNow 当前完成率
	 */
	public void setCmpRateByNow(BigDecimal cmpRateByNow) {
		this.cmpRateByNow = cmpRateByNow;
	}

	/**
	 * 取得月预计完成
	 * 
	 * @return 月预计完成
	 */
	public BigDecimal getExpMonthAmt() {
		return expMonthAmt;
	}

	/**
	 * 设置月预计完成
	 * 
	 * @param expMonthAmt 月预计完成
	 */
	public void setExpMonthAmt(BigDecimal expMonthAmt) {
		this.expMonthAmt = expMonthAmt;
	}

	/**
	 * 取得月预计完成率
	 * 
	 * @return 月预计完成率
	 */
	public BigDecimal getExpCmpRateByNow() {
		/** 月预计完成/本月任务 */
		if (null != saleMonthTargetAmt & null != expMonthAmt & saleMonthTargetAmt.compareTo(BigDecimal.ZERO) == 1
				& expMonthAmt.compareTo(BigDecimal.ZERO) == 1) {
			expCmpRateByNow = expMonthAmt.divide(saleMonthTargetAmt, 4, BigDecimal.ROUND_UP);
			expCmpRateByNow = expCmpRateByNow.multiply(new BigDecimal("100"));
			expCmpRateByNow = expCmpRateByNow.divide(new BigDecimal("1"), 2, BigDecimal.ROUND_UP);
		}
		return expCmpRateByNow;
	}

	/**
	 * 设置月预计完成率
	 * 
	 * @param expCmpRateByNow 月预计完成率
	 */
	public void setExpCmpRateByNow(BigDecimal expCmpRateByNow) {
		this.expCmpRateByNow = expCmpRateByNow;
	}

	/**
	 * 取得任务增长金额
	 * 
	 * @return 任务增长金额
	 */
	public BigDecimal getTaskGrowthamt() {
		taskGrowthamt = expMonthAmt.subtract(saleMonthTargetAmt);
		return taskGrowthamt;
	}

	/**
	 * 设置任务增长金额
	 * 
	 * @param taskGrowthamt 任务增长金额
	 */
	public void setTaskGrowthamt(BigDecimal taskGrowthamt) {
		this.taskGrowthamt = taskGrowthamt;
	}

	/**
	 * 取得任务完成店数（全部）
	 * 
	 * @return 任务完成店数（全部）
	 */
	public int getTaskCmpOrgCnt() {
		return taskCmpOrgCnt;
	}

	/**
	 * 设置任务完成店数（全部）
	 * 
	 * @param taskCmpOrgCnt 任务完成店数（全部）
	 */
	public void setTaskCmpOrgCnt(int taskCmpOrgCnt) {
		this.taskCmpOrgCnt = taskCmpOrgCnt;
	}

	/**
	 * 取得任务完成店数百分比
	 * 
	 * @return 任务完成店数百分比
	 */
	public BigDecimal getTaskCmpOrgRate() {
		if (0 != taskCmpOrgCnt) {
			taskCmpOrgRate = new BigDecimal(taskCmpOrgCnt).divide(new BigDecimal(orgCnt), 4, BigDecimal.ROUND_UP);
			taskCmpOrgRate = taskCmpOrgRate.multiply(new BigDecimal("100"));
			taskCmpOrgRate = taskCmpOrgRate.divide(new BigDecimal("1"), 2, BigDecimal.ROUND_UP);
		}
		return taskCmpOrgRate;
	}

	/**
	 * 设置任务完成店数百分比
	 * 
	 * @param taskCmpOrgRate 任务完成店数百分比
	 */
	public void setTaskCmpOrgRate(BigDecimal taskCmpOrgRate) {
		this.taskCmpOrgRate = taskCmpOrgRate;
	}

	/**
	 * 取得当月店任务（预计）贡献率（区域）
	 * 
	 * @return 当月店任务（预计）贡献率（区域）
	 */
	public BigDecimal getExpAmtRate() {
		return expAmtRate;
	}

	/**
	 * 设置当月店任务（预计）贡献率（区域）
	 * 
	 * @param expAmtRate 当月店任务（预计）贡献率（区域）
	 */
	public void setExpAmtRate(BigDecimal expAmtRate) {
		this.expAmtRate = expAmtRate;
	}

	/**
	 * 取得当月店销售贡献率（区域）
	 * 
	 * @return 当月店销售贡献率（区域）
	 */
	public BigDecimal getAmtRate() {
		return amtRate;
	}

	/**
	 * 设置当月店销售贡献率（区域）
	 * 
	 * @param amtRate 当月店销售贡献率（区域）
	 */
	public void setAmtRate(BigDecimal amtRate) {
		this.amtRate = amtRate;
	}

	/**
	 * 取得当月销售贡献指数增幅（区域）
	 * 
	 * @return 当月销售贡献指数增幅（区域）
	 */
	public BigDecimal getRatescope() {
		return ratescope;
	}

	/**
	 * 设置当月销售贡献指数增幅（区域）
	 * 
	 * @param ratescope 当月销售贡献指数增幅（区域）
	 */
	public void setRatescope(BigDecimal ratescope) {
		this.ratescope = ratescope;
	}
}