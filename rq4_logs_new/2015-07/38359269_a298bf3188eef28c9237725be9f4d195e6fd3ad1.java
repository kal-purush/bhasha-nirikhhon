package hwst.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import hwst.domain.cart.CartVo;
import hwst.service.cart.CartService;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.util.Log4jConfigurer;

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseOperation;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

@ContextConfiguration(locations = "classpath:/hwst/service/config/hw_st-servlet.xml")
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class,
	TransactionalTestExecutionListener.class, DbUnitTestExecutionListener.class})
@DatabaseSetup(value = "/hwst/service/config/INIT_CART.xml", type = DatabaseOperation.CLEAN_INSERT)
/*type = InsertIdentityOperation.CLEAN_INSERT.execute(getConnection(), dataSet);)*/
@RunWith(SpringJUnit4ClassRunner.class)
public class CartServiceTest {
	
	@BeforeClass
	 public static void init(){
	  try {
	   Log4jConfigurer.initLogging("file:WebContent/WEB-INF/log4j.properties");
	   
	  } catch (FileNotFoundException e) {
	   // TODO Auto-generated catch block
	   System.err.println("file not found!!");
	  }
	 }
	
	@Resource(name = "cartService")
	private CartService cartService;
	
	/*@Test
	public void save() throws Exception {
		assertTrue((1 + 2 == 3));
	}*/
	
	//해당 userNo에 해당하는 장바구니항목 전체불러오기
	@Test
	@DatabaseSetup(value = "/hwst/service/config/INIT_CART.xml", type = DatabaseOperation.CLEAN_INSERT)
	public void selectCartAll() {
		// Given
		// When
		List<CartVo> carts=null;
		try {
			carts = cartService.selectCartAll(70);//해당 userNo에 해당하는 장바구니목록 select
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Then
		assertThat(carts.size(),is(3));
	}
	
	//장바구니에 해당 상품리스트 추가
	@Test
	@ExpectedDatabase(assertionMode = DatabaseAssertionMode.NON_STRICT, value = "/hwst/service/config/EXPECTED_ADD_CART.xml")
	public void insertCart() {
		boolean stat = false;
		List<Integer> productOptionNo = new ArrayList<Integer>();
		List<Integer> buyAmount = new ArrayList<Integer>();
		
		productOptionNo.add(0,3);
		productOptionNo.add(1,5);
		buyAmount.add(0,3);
		buyAmount.add(1,5);
		
		try {
			stat = cartService.insertCart(productOptionNo,70, buyAmount);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertThat(stat,is(true));
	}

	//장바구니의 수량데이터항목 수정
	@Test
	@ExpectedDatabase(assertionMode = DatabaseAssertionMode.NON_STRICT, value = "/hwst/service/config/EXPECTED_UPDATE_CART_BUYAMOUNT.xml")
	public void updateCartAmount() {
		boolean stat = false;
		CartVo cartVo = new CartVo(3,2);
		
		try {
			stat = cartService.updateCartAmount(cartVo);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertThat(stat,is(true));
	}
	
	//해당 장바구니의 데이터 삭제
	@Test
	@ExpectedDatabase(assertionMode = DatabaseAssertionMode.NON_STRICT, value = "/hwst/service/config/EXPECTED_EMPTY_CART.xml")
	public void deleteCart() {
		boolean stat = false;
		
		try {
			stat = cartService.deleteCart(3);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertThat(stat,is(true));
	}
	
	
	//장바구니에서 상품들 주문 시 해당상품들의 정보 select해오기
	@Test
	public void selectCartInfo() {
		List<CartVo> carts = new ArrayList<CartVo>();
		List<Integer> cartNo = new ArrayList<Integer>();
		cartNo.add(0,1);
		cartNo.add(1,2);
		
		try {
			carts = cartService.selectCartInfo(cartNo);//try catch 제거 // checked예외
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Then
		assertThat((carts.get(0).getBuyAmount()),is(1));
		assertThat((carts.get(0).getCategoryNo()),is(7));
		assertThat((carts.get(0).getName()),is("발랄한 원피스"));
		assertThat((carts.get(0).getEachPrice()),is(58000));
		
		assertThat((carts.get(1).getBuyAmount()),is(3));
		assertThat((carts.get(1).getCategoryNo()),is(7));
		assertThat((carts.get(1).getName()),is("꽃무늬 원피스"));
		assertThat((carts.get(1).getEachPrice()),is(46000));
		
	}
	
}