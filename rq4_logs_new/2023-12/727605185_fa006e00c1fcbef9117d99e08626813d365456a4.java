package com.acorn.baemin.order.domain;

import lombok.Data;


@Data
public class KakaoOrderDTO {
	
	 String  partner_order_id; 
     String  partner_user_id ;
     String  item_name; 
     String quantity ; 
     String total_amount;
     int orderNumber;
 	int menuCode;
 	int userCode;
 	int storeCode;
 	String orderMenuName;
 	int orderMenuNumber;
 	int orderMenuPrice;
 	String orderStoreName;
 	String orderStoreImage;
 	String orderDate;
 	int payType;
 	int orderType;
 	String reqToSeller;
 	String reqToRider;
 	String orderStatus;
 	int deliveryFee;
 	String deliveryAddress;
 	String userPhone;
 	String optionsInfo;
 	int reviewStatus;

}
package com.acorn.baemin.order.kakaotest;
/*
 * package com.acorn.baemin.order.controller;
 * 
 * import java.net.URI; import java.net.URISyntaxException;
 * 
 * import org.springframework.http.HttpEntity; import
 * org.springframework.http.HttpHeaders; import
 * org.springframework.http.MediaType; import
 * org.springframework.stereotype.Service; import
 * org.springframework.util.LinkedMultiValueMap; import
 * org.springframework.util.MultiValueMap; import
 * org.springframework.web.client.RestClientException; import
 * org.springframework.web.client.RestTemplate;
 * 
 * import com.acorn.baemin.domain.OrderDTO; import
 * com.acorn.baemin.order.domain.KakaoPayApprovalVO; import
 * com.acorn.baemin.order.domain.KakaoPayReadyVO;
 * 
 * import lombok.extern.java.Log;
 * 
 * @Service
 * 
 * @Log public class KakaoPay구 {
 * 
 * private static final String HOST = "https://kapi.kakao.com"; private
 * KakaoPayReadyVO kakaoPayReadyVO; private KakaoPayApprovalVO
 * kakaoPayApprovalVO;
 * 
 * public String kakaoPayReady( OrderDTO orderDTO) {
 * 
 * RestTemplate restTemplate = new RestTemplate();
 * 
 * 
 * 
 * // 서버로 요청할 Header HttpHeaders headers = new HttpHeaders();
 * 
 * headers.add("Authorization", "KakaoAK " +
 * "8b44519adc0723877cc1352fbf89cf6f"); headers.add("Accept",
 * MediaType.APPLICATION_JSON_UTF8_VALUE); headers.add("Content-Type",
 * MediaType.APPLICATION_FORM_URLENCODED_VALUE + ";charset=UTF-8");
 * 
 * String quantity = String.valueOf(orderDTO.getOrderMenuNumber()); String
 * total_amount = String.valueOf(orderDTO.getOrderMenuPrice());
 * 
 * // 서버로 요청할 Body MultiValueMap<String, String> params = new
 * LinkedMultiValueMap<String, String>(); params.add("cid", "TC0ONETIME");
 * params.add("partner_order_id", "test"); params.add("partner_user_id",
 * "test"); params.add("item_name", orderDTO.getOrderMenuName());
 * params.add("quantity", quantity); params.add("total_amount", total_amount);
 * params.add("tax_free_amount", "0"); params.add("approval_url",
 * "http://localhost:8080/kakaoPaySuccess");
 * 
 * 
 * //헤더,바디 합치는 방법 . HttpEntity<MultiValueMap<String, String>> body = new
 * HttpEntity<MultiValueMap<String, String>>(params, headers);
 * 
 * try { // RestTemplate= 카카오페이 데이터 보낼때 사용 , kakaoPayReadyVO =
 * restTemplate.postForObject(new URI(HOST + "/v1/payment/ready"), body,
 * KakaoPayReadyVO.class);
 * 
 * log.info("" + kakaoPayReadyVO);
 * 
 * return kakaoPayReadyVO.getNext_redirect_pc_url();
 * 
 * } catch (RestClientException e) { // TODO Auto-generated catch block
 * e.printStackTrace(); } catch (URISyntaxException e) { // TODO Auto-generated
 * catch block e.printStackTrace(); }
 * 
 * return "/pay";
 * 
 * }
 * 
 * public KakaoPayApprovalVO kakaoPayInfo(String pg_token , OrderDTO orderDTO) {
 * 
 * log.info("KakaoPayInfoVO............................................");
 * log.info("-----------------------------");
 * 
 * RestTemplate restTemplate = new RestTemplate();
 * 
 * String total_amount = String.valueOf(orderDTO.getOrderMenuPrice());
 * 
 * // 서버로 요청할 Header HttpHeaders headers = new HttpHeaders();
 * 
 * headers.add("Authorization", "KakaoAK " +
 * "8b44519adc0723877cc1352fbf89cf6f"); headers.add("Accept",
 * MediaType.APPLICATION_JSON_UTF8_VALUE); headers.add("Content-Type",
 * MediaType.APPLICATION_FORM_URLENCODED_VALUE + ";charset=UTF-8");
 * 
 * // 서버로 요청할 Body MultiValueMap<String, String> params = new
 * LinkedMultiValueMap<String, String>(); params.add("cid", "TC0ONETIME");
 * params.add("tid", kakaoPayReadyVO.getTid()); params.add("partner_order_id",
 * "test"); params.add("partner_user_id", "test"); params.add("pg_token",
 * pg_token); params.add("total_amount", total_amount);
 * 
 * HttpEntity<MultiValueMap<String, String>> body = new
 * HttpEntity<MultiValueMap<String, String>>(params, headers);
 * 
 * try { kakaoPayApprovalVO = restTemplate.postForObject(new URI(HOST +
 * "/v1/payment/approve"), body, KakaoPayApprovalVO.class); log.info("" +
 * kakaoPayApprovalVO);
 * 
 * return kakaoPayApprovalVO;
 * 
 * } catch (RestClientException e) { // TODO Auto-generated catch block
 * e.printStackTrace(); } catch (URISyntaxException e) { // TODO Auto-generated
 * catch block e.printStackTrace(); }
 * 
 * return null; }
 * 
 * }
 * 
 */
package com.acorn.baemin.order.kakaotest;
/*
 * package com.acorn.baemin.order.controller;
 * 
 * 
 * import javax.servlet.http.HttpSession;
 * 
 * import org.springframework.beans.factory.annotation.Autowired;
 * 
 * import org.springframework.stereotype.Controller; import
 * org.springframework.ui.Model; import
 * org.springframework.web.bind.annotation.GetMapping; import
 * org.springframework.web.bind.annotation.PostMapping; import
 * org.springframework.web.bind.annotation.RequestParam; import
 * org.springframework.web.servlet.mvc.support.RedirectAttributes;
 * 
 * import com.acorn.baemin.domain.OrderDTO; import
 * com.acorn.baemin.order.domain.KakaoPayApprovalVO; import
 * com.acorn.baemin.order.service.UserOrderServiceImp;
 * 
 * import lombok.Setter; import lombok.extern.java.Log;
 * 
 * @Log
 * 
 * @Controller public class SampleController구 {
 * 
 * @Setter(onMethod_ = @Autowired) private KakaoPay kakaopay;
 * 
 * 
 * @Autowired UserOrderServiceImp userOrderService;
 * 
 * 
 * 
 * 
 * // @GetMapping("/kakaoPay") public void kakaoPayGet() {
 * 
 * }
 * 
 * @PostMapping("/kakaoPay") public String kakaoPay(HttpSession
 * session, @RequestParam String deliveryAddress, @RequestParam int deliveryFee,
 * 
 * @RequestParam int payType,
 * 
 * @RequestParam int orderType,
 * 
 * @RequestParam String reqToSeller,
 * 
 * @RequestParam String reqToRider,
 * 
 * @RequestParam String userPhone) {
 * log.info("kakaoPay post............................................");
 * 
 * OrderDTO orderDTO = (OrderDTO) session.getAttribute("orderDTO");
 * orderDTO.setDeliveryAddress(deliveryAddress);
 * orderDTO.setDeliveryFee(deliveryFee); orderDTO.setReqToRider(reqToRider);
 * orderDTO.setReqToSeller(reqToSeller); orderDTO.setOrderStatus("주문접수");
 * orderDTO.setUserPhone(userPhone); orderDTO.setPayType(payType);
 * orderDTO.setOrderType(orderType); orderDTO.setOrderDate("오늘");
 * userOrderService.insertOrder(orderDTO); OrderDTO lastOrderDTO =
 * userOrderService.getLastOrder(); System.out.println("주문번호 : ");
 * System.out.println(lastOrderDTO.getOrderNumber());
 * orderDTO.setOrderNumber(lastOrderDTO.getOrderNumber());
 * System.out.println(orderDTO); return "redirect:" 
 * kakaopay.kakaoPayReady(orderDTO);
 * 
 * }
 * 
 * @GetMapping("/kakaoPaySuccess") public String
 * kakaoPaySuccess(RedirectAttributes
 * redirectAttributes, @RequestParam("pg_token") String pg_token, Model model,
 * HttpSession session) {
 * log.info("kakaoPaySuccess get............................................");
 * log.info("kakaoPaySuccess pg_token : " + pg_token);
 * System.out.println("카카오페이 테스트1"); OrderDTO orderDTO = (OrderDTO)
 * session.getAttribute("orderDTO"); System.out.println("카카오페이 테스트2");
 * System.out.println(orderDTO); model.addAttribute("info",
 * kakaopay.kakaoPayInfo(pg_token, orderDTO));
 * 
 * OrderDTO lastOrderDTO = userOrderService.getLastOrder();
 * System.out.println(lastOrderDTO.getOrderNumber());
 * redirectAttributes.addAttribute("orderNumber",
 * lastOrderDTO.getOrderNumber()); return "redirect:orderDetail"; }
 * 
 * }
 */