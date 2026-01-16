package com.flab.kinoistkino.controller;

import com.flab.kinoistkino.model.SearchParam;
import org.springframework.web.bind.annotation.*;


@RestController // Spring에게 이곳은 컨트롤러로 사용 할 것이다 라고 알려주는 지시자
@RequestMapping("/api") // 이 곳으로 들어올 API 주소를 맵핑 (/api 경로를 입력하여 받을 path 입력) localhost:8080/api
public class GetController {

    @RequestMapping(method = RequestMethod.GET, path = "getMethod")
    public String getRequest(){

        return "Hi getMethod";
    }

    // Localhost:8080/api/multiParameter?account=abcd&email=study@gmail.com&page=10
    // 위와 같이 파라미터가 늘어날 때마다 작성해 주는것은 비효율적이기 때문에 객체를 만들어서 가져온다.
    @GetMapping("getMultiParameter")
    public SearchParam getMultiParameter(SearchParam searchParam){ // SearchParam 객체에 정의 된 내용을 가져온다.
        System.out.println(searchParam.getAccount()); // 값들이 제대로 들어왔는지 확인
        System.out.println(searchParam.getPassword());
        System.out.println(searchParam.getName());
        System.out.println(searchParam.getEmail());
        System.out.println(searchParam.getPhoneNumber());
        System.out.println(searchParam.getPage());

        // {"account" : "", "email" : "", "page" : 0} 과 같이 json 형태로도 리턴

        return searchParam;
    }
}