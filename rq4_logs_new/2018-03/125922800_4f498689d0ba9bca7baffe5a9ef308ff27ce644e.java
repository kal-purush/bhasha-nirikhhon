package de.tonyskulk.gtvisual;

import java.math.BigDecimal;

public class GapDetails {

	private BigDecimal gapValue;
	private BigDecimal gapPercent;
	private BigDecimal lowerPrice;
	private BigDecimal higherPrice;
	private BigDecimal volume;

	public GapDetails(BigDecimal gapValue, BigDecimal gapPercent, BigDecimal lowerPrice, BigDecimal higherPrice,
			BigDecimal volume) {
		this.gapValue = gapValue;
		this.gapPercent = gapPercent;
		this.lowerPrice = lowerPrice;
		this.higherPrice = higherPrice;
		this.volume = volume;
	}

	public BigDecimal getGapValue() {
		return gapValue;
	}

	public void setGapValue(BigDecimal gapValue) {
		this.gapValue = gapValue;
	}

	public BigDecimal getGapPercent() {
		return gapPercent;
	}

	public void setGapPercent(BigDecimal gapPercent) {
		this.gapPercent = gapPercent;
	}

	public BigDecimal getLowerPrice() {
		return lowerPrice;
	}

	public void setLowerPrice(BigDecimal lowerPrice) {
		this.lowerPrice = lowerPrice;
	}

	public BigDecimal getHigherPrice() {
		return higherPrice;
	}

	public void setHigherPrice(BigDecimal higherPrice) {
		this.higherPrice = higherPrice;
	}

	public BigDecimal getVolume() {
		return volume;
	}

	public void setVolume(BigDecimal volume) {
		this.volume = volume;
	}

	@Override
	public String toString() {
		return "GapDetails [gapValue=" + gapValue + ", gapPercent=" + gapPercent + ", lowerPrice=" + lowerPrice
				+ ", higherPrice=" + higherPrice + ", volume=" + volume + "]";
	}

}