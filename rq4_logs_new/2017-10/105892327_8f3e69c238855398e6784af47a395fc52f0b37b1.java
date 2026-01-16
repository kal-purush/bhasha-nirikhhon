package org.seonbeen.controller;



import java.util.List;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.seonbeen.domain.BoardVO;
import org.seonbeen.domain.MemberVO;
import org.seonbeen.service.BoardService;
import org.seonbeen.service.MemberService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test/*")
public class APIController {
	
	
	private static Logger logger = Logger.getLogger(APIController.class);
	
	public static final String DEFAULT_METHOD_PARAM = "_method";
	
	@Inject
	private BoardService service;
	
	@Inject
	private MemberService mservice;
	
	@RequestMapping(value="/list/{bno}", method=RequestMethod.GET)
	public ResponseEntity<BoardVO> readapi(@PathVariable("bno") int bno) throws Exception{
		
		
		BoardVO vo = service.listOne(bno);
		
		return new ResponseEntity<BoardVO>(vo,HttpStatus.OK);
		
		
	}
	
	@RequestMapping(value="/register", method=RequestMethod.POST)
	public ResponseEntity<BoardVO> postapi(@ModelAttribute BoardVO vo)throws Exception{
		logger.info(vo);
		
		ResponseEntity<BoardVO> entity = null;
		/*HttpHeaders httpheaders = new HttpHeaders();
		httpheaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
*/		try{
		service.registerBoard(vo);
        int bno = vo.getBno(); 	
        logger.info(bno);
		BoardVO vo2 = service.listOne(bno);
		logger.info(vo2);
		entity = new ResponseEntity<BoardVO>(vo2,HttpStatus.OK); 
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		entity = new ResponseEntity<BoardVO>(HttpStatus.BAD_REQUEST);
		}	


		
		return entity;
		
	}
	@RequestMapping(value="/list", method=RequestMethod.GET)
    public ResponseEntity<List<BoardVO>> listGET() throws Exception{
		
		logger.info("info 안찍힘");
		List<BoardVO> list = service.listAll();
		
		return new ResponseEntity<List<BoardVO>>(list,HttpStatus.OK);
	}
	
	@RequestMapping(value="/custom/{bno}", method=RequestMethod.PUT, produces="application/json")
	public ResponseEntity<BoardVO> patchCustom(@PathVariable("bno") int bno,BoardVO vo)throws Exception{
		logger.info(bno);
		logger.info(vo);
		
		vo.setBno(bno);
		service.modifiedBoard(vo);
		
	    BoardVO vo2 = service.listOne(bno);
		
		return new ResponseEntity<BoardVO>(vo2,HttpStatus.OK);
	}
	
	@RequestMapping(value="/delete/{bno}", method=RequestMethod.DELETE, produces={"application/json"})
	public void deleteApi(@PathVariable("bno") int bno) throws Exception{
				
		service.deleteBoard(bno);
		
	}
	@RequestMapping(value="/join", method=RequestMethod.POST)
	public ResponseEntity<MemberVO> joinApi(@ModelAttribute MemberVO vo) throws Exception{
		
		ResponseEntity<MemberVO> entity = null;
		try{
			logger.info("asdasd");
			logger.info(vo);
			mservice.memberRegister(vo);
		
			int mno = vo.getMno();
			logger.info(mno);
			MemberVO vo2 = mservice.memberRegisterShow(mno);
			logger.info(vo2);
			return new ResponseEntity<MemberVO>(vo2,HttpStatus.OK);
		}catch(Exception e){
			return new ResponseEntity<MemberVO>(HttpStatus.BAD_REQUEST);
		}
		
		
	}
	
	/*@RequestMapping(value="/naver", method=RequestMethod.POST)
	public void naverAPI(){
		 String clientId = "seGPkRi4xxX5rP3GB0Aa";//애플리케이션 클라이언트 아이디값";
	        String clientSecret = "9_fww4oyKZ";//애플리케이션 클라이언트 시크릿값";

	        try {
	            String apiURL = "https://openapi.naver.com/v1/datalab/search";
	            String body = "{\"startDate\":\"2017-01-01\",\"endDate\":\"2017-04-30\",\"timeUnit\":\"month\",\"keywordGroups\":[{\"groupName\":\"한글\",\"keywords\":[\"한글\",\"korean\"]},{\"groupName\":\"영어\",\"keywords\":[\"영어\",\"english\"]}],\"device\":\"pc\",\"ages\":[\"1\",\"2\"],\"gender\":\"f\"}";
	            URL url = new URL(apiURL);
	            HttpURLConnection con = (HttpURLConnection)url.openConnection();
	            con.setRequestMethod("POST");
	            con.setRequestProperty("X-Naver-Client-Id", clientId);
	            con.setRequestProperty("X-Naver-Client-Secret", clientSecret);
	            con.setRequestProperty("Content-Type", "application/json");

	            con.setDoOutput(true);
	            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
	            wr.write(body.getBytes());
	            wr.flush();
	            wr.close();

	            int responseCode = con.getResponseCode();
	            BufferedReader br;
	            if(responseCode==200) { // 정상 호출
	                br = new BufferedReader(new InputStreamReader(con.getInputStream()));
	            } else {  // 에러 발생
	                br = new BufferedReader(new InputStreamReader(con.getErrorStream()));
	            }

	            String inputLine;
	            StringBuffer response = new StringBuffer();
	            while ((inputLine = br.readLine()) != null) {
	                response.append(inputLine);
	            }
	            br.close();
	            System.out.println(response.toString());

	        } catch (Exception e) {
	            System.out.println(e);
	        }
	    }
*/	
	
	
	
}