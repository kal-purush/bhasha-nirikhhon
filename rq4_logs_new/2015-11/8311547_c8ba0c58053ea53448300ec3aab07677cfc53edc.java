package de.mq.merchandise.subject.support;




import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;


import de.mq.merchandise.customer.Customer;
import de.mq.merchandise.subject.support.CommercialSubjectModel.EventType;
import de.mq.merchandise.support.Mapper;
import de.mq.util.event.Observer;

public class CommercialSubjectModelTest {
	
	private static final String SEARCH_FIELD = "search";

	private static final String COMMERCIAL_SUBJECT_FIELD = "commercialSubject";

	private static final long ID = 19680528L;

	private final CommercialSubject search = Mockito.mock(CommercialSubject.class);
	
	private final CommercialSubject commercialSubject = Mockito.mock(CommercialSubject.class);
	private final CommercialSubjectEventFascade commercialSubjectEventFascade = Mockito.mock(CommercialSubjectEventFascade.class);
	
	@SuppressWarnings("unchecked")
	private final  Mapper<Customer, CommercialSubject> customerIntoSubjectMapper = Mockito.mock(Mapper.class);
	
	private final CommercialSubjectModel model = new CommercialSubjectModelImpl(search, commercialSubject, commercialSubjectEventFascade, customerIntoSubjectMapper);
	
	private final Customer customer = Mockito.mock(Customer.class);
	
	@SuppressWarnings("unchecked")
	private final Observer<EventType> observer = Mockito.mock(Observer.class);
	
	@Before
	public final void setUp() {
		ReflectionUtils.doWithFields(model.getClass(), field -> {field.setAccessible(true);ReflectionUtils.setField(field, model, customer);} , Field -> Field.getType().equals(Customer.class));
	   Mockito.when(commercialSubject.id()).thenReturn(Optional.of(ID));
	}
	
	@Test
	public final void setSearch() {
		
		
		model.register(observer, EventType.SearchCriteriaChanged);
		CommercialSubject search = Mockito.mock(CommercialSubject.class);
		Assert.assertEquals(this.search,ReflectionTestUtils.getField(model, SEARCH_FIELD));
		model.setSearch(search);
		Assert.assertEquals(search,ReflectionTestUtils.getField(model, SEARCH_FIELD));
		
		
		Mockito.verify(customerIntoSubjectMapper).mapInto(customer, search);
		Mockito.verify(observer).process(EventType.SearchCriteriaChanged);
	}
	
	@Test
	public final void getSearch() {
		Assert.assertEquals(search, model.getSearch());
	}
	
	@Test
	public final void save() {
		
		model.register(observer, EventType.CommericalSubjectChanged);
		CommercialSubject commercialSubject = Mockito.mock(CommercialSubject.class);
		
		Assert.assertEquals(this.commercialSubject, (CommercialSubject) ReflectionTestUtils.getField(model, COMMERCIAL_SUBJECT_FIELD));
		model.save(commercialSubject);
		
		Assert.assertEquals(CommercialSubjectImpl.class,  ReflectionTestUtils.getField(model, COMMERCIAL_SUBJECT_FIELD).getClass());
		
		
		Mockito.verify( customerIntoSubjectMapper).mapInto(customer, commercialSubject);
		Mockito.verify(commercialSubjectEventFascade).save(this.commercialSubject.id().get(), commercialSubject);
		
		Mockito.verify(observer).process(EventType.CommericalSubjectChanged);
	}
	
	
	@Test
	public final void setCustomer() {
		final Customer customer = Mockito.mock(Customer.class);
	
		model.register(observer, EventType.SearchCriteriaChanged);
		model.setCustomer(customer);
		
		ReflectionUtils.doWithFields(model.getClass(), field -> Assert.assertEquals(customer, ReflectionUtils.getField(field, model)), field -> field.getType().equals(Customer.class));
	   
		Mockito.verify(customerIntoSubjectMapper).mapInto(customer, this.search);
		Mockito.verify(observer).process(EventType.SearchCriteriaChanged);
	
	}
	
	
	@Test
	public final void setCommercialSubjectId() {
		
		model.register(observer, EventType.CommericalSubjectChanged);
		CommercialSubject commercialSubject = Mockito.mock(CommercialSubject.class);
		Mockito.when(commercialSubjectEventFascade.commercialSubjectChanged(ID)).thenReturn(commercialSubject);
		model.setCommercialSubjectId(ID);
		
		Assert.assertEquals(commercialSubject, ReflectionTestUtils.getField(model, COMMERCIAL_SUBJECT_FIELD));
		Mockito.verify(observer).process(EventType.CommericalSubjectChanged);
		
	}

}