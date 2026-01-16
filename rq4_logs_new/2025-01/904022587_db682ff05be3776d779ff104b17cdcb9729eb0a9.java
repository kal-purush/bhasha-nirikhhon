package com.green.common.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.green.common.mapper.DetailSearchMapper;
import com.green.common.service.DetailSearchService;

@Service
public class DetailSearchServiceImpl implements DetailSearchService{

	@Autowired
	private DetailSearchMapper detailSearchMapper;

	@Override
	public List<HashMap<String, Object>> getAttributeListByCategoryIdx(String categoryIdx) {
	    if(categoryIdx.equals("2") || categoryIdx.equals("3")) {
	        categoryIdx = null;
	    }
	        List<HashMap<String, Object>> categoryAttributeList = detailSearchMapper.getAttributeListByCategoryIdx(categoryIdx);
	        Map<Integer, HashMap<HashMap<String, Object>, List<HashMap<String, Object>>>> groupedData = new HashMap<>();
	        
	        for (HashMap<String, Object> row : categoryAttributeList) {
	        	int group = ((Number) row.get("CATEGORY_ATTRIBUTE_IDX")).intValue();
	        	HashMap<String, Object> attribute =  new HashMap<>();
	        	attribute.put("attribute_idx",Integer.parseInt(String.valueOf(row.get("CATEGORY_ATTRIBUTE_IDX"))));
	        	attribute.put("attribute_name",String.valueOf(row.get("CATEGORY_ATTRIBUTE_NAME")));
	        
	        	HashMap<String, Object> value = new HashMap<>();
	        	value.put("attribute_value_idx",Integer.parseInt(String.valueOf(row.get("CATEGORY_ATTRIBUTE_VALUE_IDX"))));
	        	value.put("attribute_value_name",String.valueOf(row.get("CATEGORY_ATTRIBUTE_VALUE_NAME")));
	        	
	        	groupedData.putIfAbsent(group, new HashMap<>());
	        	groupedData.get(group).putIfAbsent(attribute, new ArrayList<>());
	        	groupedData.get(group).get(attribute).add(value);
	        }
	        
	        List<HashMap<String, Object>> result = new ArrayList<>();
	        for (Entry<Integer, HashMap<HashMap<String, Object>, List<HashMap<String, Object>>>> entry : groupedData.entrySet()) {
	            HashMap<String, Object> groupMap = new HashMap<>();
	            groupMap.put("Group", entry.getKey());
	            
	            // entry.getValue()의 각 항목을 개별적으로 처리
	            for (Entry<HashMap<String, Object>, List<HashMap<String, Object>>> innerEntry : entry.getValue().entrySet()) {
	                HashMap<String, Object> attribute = innerEntry.getKey();
	                List<HashMap<String, Object>> values = innerEntry.getValue();
	                
	                // attribute를 키로 사용하고 values를 값으로 사용
	                groupMap.put(attribute.get("attribute_name").toString(), values);
	            }
	            
	            result.add(groupMap);
	        }
	        System.out.println(result);

		    return result;

	}

	

	/*
	@Override
	public HashMap<String, Object> getProductPagingList(@RequestParam HashMap<String, Object> requestBody) {
		
		HashMap<String, Object> res = new HashMap<>();
		
		int  productCount  = quickFinderMapper.getProductCount(requestBody);
		
	    PagingResponse<HashMap<String, Object>> response = null;
	    if( productCount < 1 ) { // 현재 조회한 자료가 없다면
	    	response = new PagingResponse<>(
	    		Collections.emptyList(), null);}
	    
	    int nowpage = 1; // 기본값 설정
	    if (requestBody.get("nowpage") != null) {
	        try {
	            nowpage = Integer.parseInt(String.valueOf(requestBody.get("nowpage")));
	        } catch (NumberFormatException e) {
	            // 로그 기록 또는 에러 처리
	            // 기본값 1을 유지
	        }
	    }
	    

	    // 페이징을 위한 초기 설정
	    SearchVo searchVo = new SearchVo();
	    searchVo.setPage(nowpage);      // 현재 페이지 정보
	    searchVo.setRecordSize(5);      // 페이지당 20개
	    searchVo.setPageSize(5);        // paging.jsp에 출력할 페이지번호수
	    
	    // Pagination 설정
	    Pagination pagination = new Pagination(productCount, searchVo);
	    int 	offset		= searchVo.getOffset();
	    int		recordSize	= searchVo.getRecordSize();
    	List<HashMap<String, Object>> list = quickFinderMapper.getProductPagingList(offset,recordSize,requestBody);
    	
    	String clobKey = "PRODUCT_DESCRIPTION";

    	list = list.stream().map(item -> {
    	    Object value = item.get(clobKey);
    	    if (value instanceof Clob) {
    	        try {
    	            String clobString = ClobUtils.clobToString((Clob) value);
    	            item.put(clobKey, clobString);
    	        } catch (Exception e) {
    	            // 예외 처리
    	            e.printStackTrace(); // 또는 로깅
    	        }
    	    }
    	    return item;
    	}).collect(Collectors.toList());

    	
	    response = new PagingResponse<>(list, pagination);
	
		res.put("response", response);
		res.put("searchedCount", productCount);
		res.put("nowpage", nowpage);
		res.put("searchVo", searchVo);
		return res;
	}


*/

}