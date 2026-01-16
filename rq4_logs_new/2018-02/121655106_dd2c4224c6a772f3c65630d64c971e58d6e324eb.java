
package com.iiitb.ooadvoid.dao;

import java.util.List;

import com.iiitb.ooadvoid.pojo.FlopkartListing;

public class FlopkartListingDAO extends HibernateDAO<FlopkartListing>
{
	String entity_name = "FlopkartListing";
	
	public FlopkartListing addFlopkartListing(FlopkartListing item)
	{
		super.add(item);
		return item;
	}
	
	
	public List<FlopkartListing> getFlopkartListings()
	{
		return super.list(entity_name);
	}

	public FlopkartListing getFlopkartListingById(int id)
	{
		return super.find(entity_name,id);
	}

  /*  public List<FlopkartListing> getFlopkartListingByCategoty(FlopkartListing item)
	{
		return super.findAll(entity_name,"category", item.getSubcategoryId());
	}
  */
	public int deleteFlopkartListing(int id)
	{
		return super.remove(entity_name,id);
	}

	public int updateFlopkart_Item(int id, FlopkartListing item)
	{
		if (id <= 0)
			return 0;	
		super.update(item,id);
		return 1;
	}
}