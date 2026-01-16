package com.green.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.green.crawling.mapper.CrawlingMapper;

@Component
public class CrwllingProductFilter {
	
	private static CrawlingMapper crawlingMapper;

	@Autowired
	public void setCrawlingMapper(CrawlingMapper mapper) {
	    CrwllingProductFilter.crawlingMapper = mapper;
	}
    
    public static void cpuAttribute(List<HashMap<String, Object>> list) {
        List<HashMap<String, Object>> crollingItemList = ClobUtils.processClobData(list);
        
        HashMap<String, HashSet<String>> attributeSets = new HashMap<>();
        String[] attributes = {"manufacturer", "tdp", "memory", "graphics", "cores", "threads", 
                               "baseClock", "maxClock", "process", "l2Cache", "l3Cache", "socket"};
        
        for (String attr : attributes) {
            attributeSets.put(attr, new HashSet<>());
        }

        // 정규표현식 패턴 미리 컴파일
        Pattern tdpPattern = Pattern.compile("TDP:\\s*(\\d+)|PBP-MTP:\\s*\\d+-(\\d+)");
        Pattern memoryPattern = Pattern.compile("메모리 규격:\\s*([^/\\n]+)");
        Pattern graphicsPattern = Pattern.compile("내장그래픽:\\s*([^/\\n]+)");
        Pattern corePattern = Pattern.compile("P(\\d+)\\+E(\\d+)코어|(\\d+)코어");
        Pattern threadPattern = Pattern.compile("P(\\d+)\\+E(\\d+)스레드|(\\d+)스레드");
        Pattern baseClockPattern = Pattern.compile("기본 클럭:\\s*([^/\\n]+)GHz");
        Pattern maxClockPattern = Pattern.compile("최대 클럭:\\s*([^/\\n]+)GHz");
        Pattern processPattern = Pattern.compile("([\\d]+)nm");
        Pattern l2CachePattern = Pattern.compile("L2 캐시:\\s*([^/\\n]+)MB");
        Pattern l3CachePattern = Pattern.compile("L3 캐시:\\s*([^/\\n]+)MB");

        for (HashMap<String, Object> product : crollingItemList) {
            String categoryIdx = String.valueOf(product.get("CATEGORY_IDX"));
            String option = String.valueOf(product.get("PRODUCT_DESCRIPTION"));
            int productIdx = Integer.parseInt(String.valueOf(product.get("PRODUCT_IDX")));
            int attrType = 0;
            int attrVal = 0;
            if (categoryIdx.equals("5")) {
                HashMap<String, String> cpuAttributes = new HashMap<>();
                
                cpuAttributes.put("manufacturer", option.contains("인텔") ? "인텔" : "AMD");
                attrType = 1;
                attrVal =  option.contains("인텔") ? 1 : 2;
                crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                
                attrType = 13;
                attrVal =  option.contains("미탑재") ? 44 : 45;
                crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                
                // TDP 추출
                Matcher tdpMatcher = tdpPattern.matcher(option);
                if (tdpMatcher.find()) {
                    cpuAttributes.put("tdp", tdpMatcher.group(1) != null ? tdpMatcher.group(1) : tdpMatcher.group(2));
                    
                    int tdp = Integer.parseInt(tdpMatcher.group(1) != null ? tdpMatcher.group(1) : tdpMatcher.group(2));
                    attrType = 10;
                    if(tdp >= 105) {
                    	attrVal = 36;
                    	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                    }else if(105 > tdp && tdp >= 100) {
                    	attrVal = 35;
                    	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                    }else if(99 >= tdp && tdp >= 76) {
                    	attrVal = 34;
                    	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                    }else if(75 >= tdp && tdp >= 50) {
                    	attrVal = 33;
                    	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                    }else if(49 > tdp) {
                    	attrVal = 32;
                    	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                    }
                    
                    
                }

                // 다른 속성들 추출
                extractAttribute(memoryPattern, option, "memory", cpuAttributes);
                extractAttribute(graphicsPattern, option, "graphics", cpuAttributes);
                extractCores(corePattern, option, cpuAttributes);
                extractThreads(threadPattern, option, cpuAttributes);
                extractAttribute(baseClockPattern, option, "baseClock", cpuAttributes);
                extractAttribute(maxClockPattern, option, "maxClock", cpuAttributes);
                extractAttribute(processPattern, option, "process", cpuAttributes);
                extractAttribute(l2CachePattern, option, "l2Cache", cpuAttributes);
                extractAttribute(l3CachePattern, option, "l3Cache", cpuAttributes);

                // 소켓 타입
                cpuAttributes.put("socket", option.split("/")[0]);

                // 속성 셋에 추가
                for (String attr : attributes) {
                    if (cpuAttributes.containsKey(attr)) {
                        attributeSets.get(attr).add(cpuAttributes.get(attr));
                    }
                }
                
                
                double bascClock = Double.parseDouble(cpuAttributes.get("baseClock"));
                attrType = 8;
                if(5.0 > bascClock && bascClock >= 4.5) {
                	attrVal = 22;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(4.5 > bascClock && bascClock >= 4.0) {
                	attrVal = 23;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(4.0> bascClock && bascClock >= 3.5) {
                	attrVal = 24;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(3.5> bascClock && bascClock >= 3.0) {
                	attrVal = 25;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(3.0> bascClock && bascClock >= 2.5) {
                	attrVal = 26;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }
                double maxClock = Double.parseDouble(cpuAttributes.get("maxClock"));
                attrType = 9;
                if(maxClock > 6.0) {
                	attrVal = 27;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(6.0 > maxClock && maxClock >= 5.0) {
                	attrVal = 28;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(5.0> maxClock && maxClock >= 4.5) {
                	attrVal = 29;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(4.5> maxClock && maxClock >= 4.0) {
                	attrVal = 30;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }else if(4.0> maxClock && maxClock >= 3.5) {
                	attrVal = 31;
                	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                }
                
                attrType = 6;
                int core = Integer.parseInt(cpuAttributes.get("cores"));
                	switch (core) {
					case 24: {
						attrVal = 12;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 20: {
						attrVal = 464;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 16: {
						attrVal = 13;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 14: {
						attrVal = 465;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 12: {
						attrVal = 466;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 10: {
						attrVal = 14;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 8: {
						attrVal = 15;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					case 6: {
						attrVal = 16;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
					default:
						attrVal = 467;
                		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                		break;
					}
                		
                	
                	 attrType = 7;
                     int threads = Integer.parseInt(cpuAttributes.get("threads"));
                     	switch (threads) {
     					case 32: {
     						attrVal = 17;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 24: {
     						attrVal = 18;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 20: {
     						attrVal = 19;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 16: {
     						attrVal = 20;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 14: {
     						attrVal = 21;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 12: {
     						attrVal = 461;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 10: {
     						attrVal = 463;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					default:
     						attrVal = 462;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
                     	
                   	 attrType = 11;
                     int process = Integer.parseInt(cpuAttributes.get("process"));
                     	switch (process) {
     					case 10: {
     						attrVal = 41;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 7: {
     						attrVal = 40;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 5: {
     						attrVal = 39;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					case 4: {
     						attrVal = 38;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
     					default:
     						attrVal = 37;
                     		crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     		break;
     					}
                     	
                   	 attrType = 3;
                     String memory = String.valueOf(cpuAttributes.get("memory"));
                     if(memory.contains("DDR5")) {
                     	attrVal = 6;
                     	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     }
                     if(memory.contains("DDR4")) {
                    	attrVal = 5;
                      	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                     }
                     	 attrType = 12;
                         String socket = String.valueOf(cpuAttributes.get("socket"));
                         if(socket.contains("1851")) {
                         	attrVal = 42;
                         	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(socket.contains("1700")) {
                        	attrVal = 43;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(socket.contains("AM5")) {
                        	attrVal = 485;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(socket.contains("AM4")) {
                        	attrVal = 486;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }
                     	
                     	
                     	
                     	 attrType = 14;
                         double l2Cache = Double.parseDouble(cpuAttributes.get("l2Cache"));
                         if(l2Cache == 40) {
                         	attrVal = 46;
                         	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 36) {
                        	attrVal = 47;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 32) {
                        	attrVal = 48;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 28) {
                        	attrVal = 49;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 26) {
                        	attrVal = 50;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 20) {
                        	attrVal = 468;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 16) {
                        	attrVal = 469;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 12) {
                        	attrVal = 470;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 9.5) {
                        	attrVal = 471;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 8) {
                        	attrVal = 472;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 7.5) {
                        	attrVal = 473;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 6) {
                        	attrVal = 474;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 5) {
                        	attrVal = 475;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 4) {
                        	attrVal = 476;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l2Cache == 3) {
                        	attrVal = 477;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }
                         
                     	 attrType = 15;
                         double l3Cache = Double.parseDouble(cpuAttributes.get("l3Cache"));
                         if(l3Cache == 128) {
                         	attrVal = 51;
                         	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 96) {
                        	attrVal = 52;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 64) {
                        	attrVal = 53;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 36) {
                        	attrVal = 54;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 33) {
                        	attrVal = 55;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 32) {
                        	attrVal = 478;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 30) {
                        	attrVal = 479;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 24) {
                        	attrVal = 480;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 20) {
                        	attrVal = 481;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 18) {
                        	attrVal = 482;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 16) {
                        	attrVal = 483;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }else if(l3Cache == 12) {
                        	attrVal = 484;
                          	crawlingMapper.saveAttr(productIdx,attrType,attrVal);
                         }
            }

        }

        for (String attr : attributes) {
            ArrayList<String> sortedList = new ArrayList<>(attributeSets.get(attr));
            Collections.sort(sortedList, Collections.reverseOrder());
            System.out.println("Unique " + attr + ": " + sortedList);
        }
        
    }

    private static void extractAttribute(Pattern pattern, String option, String attributeName, HashMap<String, String> attributes) {
        Matcher matcher = pattern.matcher(option);
        if (matcher.find()) {
            attributes.put(attributeName, matcher.group(1));
        }
    }

    private static void extractCores(Pattern pattern, String option, HashMap<String, String> attributes) {
        Matcher matcher = pattern.matcher(option);
        if (matcher.find()) {
            int cores = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) :
                        Integer.parseInt(matcher.group(1)) + Integer.parseInt(matcher.group(2));
            attributes.put("cores", String.valueOf(cores));
        }
    }

    private static void extractThreads(Pattern pattern, String option, HashMap<String, String> attributes) {
        Matcher matcher = pattern.matcher(option);
        if (matcher.find()) {
            int threads = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) :
                          Integer.parseInt(matcher.group(1)) + Integer.parseInt(matcher.group(2));
            attributes.put("threads", String.valueOf(threads));
        }
    }
}
