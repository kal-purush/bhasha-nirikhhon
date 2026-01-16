package com.org.loan;

import junit.framework.TestCase;

import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.org.loan.model.Loan;
import com.org.loan.service.ICustomerService;
@RunWith(SpringJUnit4ClassRunner.class)
public class LoanAppTest extends TestCase{
	@InjectMocks
	private ICustomerService customerService;
	@Mock
	private MongoClient mockClient;
	@Mock
	private MongoCollection mockCollection;
	@Mock
	private MongoDatabase mockDB;

	
	@Test
    public void applyLoan() {
        Loan loan = new Loan("Home",2110000,"20-10-2020",2,3,12345,"MCA","Krishana Rao",120000,"CTS");
        //assertTrue(customerService.applyLoan(loan));
        assertNotNull(loan.getLoanType());  
    }
	
	@Test(expected=IllegalArgumentException.class)
	public void aviodInvaildLoanType() {
		Loan loan = new Loan("Home",2110000,"20-10-2020",2,3,12345,"MCA","Krishana Rao",120000,"CTS");
		customerService.applyLoan(loan);
	}

}