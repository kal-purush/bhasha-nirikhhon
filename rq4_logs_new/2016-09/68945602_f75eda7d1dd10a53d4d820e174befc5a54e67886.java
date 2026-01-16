package com.vinhuang.beans;

/**
 * POJO for the output
 *
 */
public class AddressExtractOutput {

	private String addressLine1;
	private String addressLine2;
	private String addressLine3;
	private String city;
	private String zip;

	/**
	 * @return the addressLine1
	 */
	public String getAddressLine1() {
		return this.addressLine1;
	}

	/**
	 * @param addressLine1 the addressLine1 to set
	 */
	public void setAddressLine1(String addressLine1) {
		this.addressLine1 = addressLine1;
	}

	/**
	 * @return the addressLine2
	 */
	public String getAddressLine2() {
		return this.addressLine2;
	}

	/**
	 * @param addressLine2 the addressLine2 to set
	 */
	public void setAddressLine2(String addressLine2) {
		this.addressLine2 = addressLine2;
	}

	/**
	 * @return the addressLine3
	 */
	public String getAddressLine3() {
		return this.addressLine3;
	}

	/**
	 * @param addressLine3 the addressLine3 to set
	 */
	public void setAddressLine3(String addressLine3) {
		this.addressLine3 = addressLine3;
	}

	/**
	 * @return the city
	 */
	public String getCity() {
		return this.city;
	}

	/**
	 * @param city the city to set
	 */
	public void setCity(String city) {
		this.city = city;
	}

	/**
	 * @return the zip
	 */
	public String getZip() {
		return this.zip;
	}

	/**
	 * @param zip the zip to set
	 */
	public void setZip(String zip) {
		this.zip = zip;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.addressLine1 == null) ? 0 : this.addressLine1.hashCode());
		result = prime * result + ((this.addressLine2 == null) ? 0 : this.addressLine2.hashCode());
		result = prime * result + ((this.addressLine3 == null) ? 0 : this.addressLine3.hashCode());
		result = prime * result + ((this.city == null) ? 0 : this.city.hashCode());
		result = prime * result + ((this.zip == null) ? 0 : this.zip.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(this == obj)
			return true;
		if(obj == null)
			return false;
		if(getClass() != obj.getClass())
			return false;
		AddressExtractOutput other = (AddressExtractOutput) obj;
		if(this.addressLine1 == null) {
			if(other.addressLine1 != null)
				return false;
		} else if(!this.addressLine1.equals(other.addressLine1))
			return false;
		if(this.addressLine2 == null) {
			if(other.addressLine2 != null)
				return false;
		} else if(!this.addressLine2.equals(other.addressLine2))
			return false;
		if(this.addressLine3 == null) {
			if(other.addressLine3 != null)
				return false;
		} else if(!this.addressLine3.equals(other.addressLine3))
			return false;
		if(this.city == null) {
			if(other.city != null)
				return false;
		} else if(!this.city.equals(other.city))
			return false;
		if(this.zip == null) {
			if(other.zip != null)
				return false;
		} else if(!this.zip.equals(other.zip))
			return false;
		return true;
	}

}