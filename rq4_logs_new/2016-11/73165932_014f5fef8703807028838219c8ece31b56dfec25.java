/*
 * This class represent a cashier for a store, the class holds the current purchase list of a customer and the current sum of money stored in the cashier 
 */

public class Cashier 
{
	
	private float m_fCurrentSum = 0;	// represent the sum of money currently stored in the cashier ( without the on going purchase) 
	private PurchaseList m_currentPurchaseList = new PurchaseList();	// represent the list of item for the current customer 
	
	
	//default constructor 
	public Cashier()
	{
		this(0);
	}
	
	// constructor with an initial sum of money in the cashier
	public Cashier(float fInitValue)
	{
		if(fInitValue >= 0 )
			m_fCurrentSum = fInitValue;				
	}
	
	// retrieves the current sum of money in the cashier (without the on going purchase)
	public float getCurrentCashierSum()
	{
		return m_fCurrentSum;
	}
	
	// adds a new item to the current purchase list.
	public void addNewPurchaseItem( PurchaseItem newPurchaseItem)
	{
		m_currentPurchaseList.addPurchaseItem(newPurchaseItem);
	}
	
	// retrieves the current purchase list as an array of PurchaseItem 
	public PurchaseItem[] getPurchasedItemArray()
	{
		return m_currentPurchaseList.getPurchaseListAsArray();		
	}
	
	// retrieves the current purchase sum 
	public float getCurrentPurchaseSum()
	{
		return m_currentPurchaseList.getPurchasedListSum();
	}
	
	// performs a payment process (the customer pays and the cashier sum is updated) if the paid amount is less then the current purchase list sum
	// then a negative change is return otherwise the change from the paid amount is returned
	public float performPayment(float fPaidAmount)
	{
		if(fPaidAmount <= 0)
			return fPaidAmount;
		float fChange = 0;
		float fCurrentPurchaseSum = m_currentPurchaseList.getPurchasedListSum();
		
		fChange = fPaidAmount - fCurrentPurchaseSum;
		
		m_fCurrentSum += fCurrentPurchaseSum;
		m_currentPurchaseList.clearPurchaseList();
		return fChange;		 
	}
}