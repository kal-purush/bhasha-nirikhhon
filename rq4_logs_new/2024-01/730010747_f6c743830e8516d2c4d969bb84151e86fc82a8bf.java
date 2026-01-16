package com.dockbang.util;

import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import com.dockbang.model.SubwayStationTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class DijkstraAlgo {
	// 노드, 거리를 저장하는 Map 
	Map<DijkstraNode, Integer> distances;
	// distances의 거리를 오름차순으로 정렬해서 가지는 큐
	PriorityQueue<DijkstraNode> priorityQueue;

    // 다익스트라 알고리즘 함수
    public Map<DijkstraNode, Integer> dijkstra(DijkstraNode start) {
    	distances = new HashMap<>();
    	priorityQueue = new PriorityQueue<>(Comparator.comparingInt(distances::get));
    	
        // 시작 노드의 거리 초기화
        distances.put(start, 0);
        priorityQueue.add(start);
        
        DijkstraNode current = null;
        while (!priorityQueue.isEmpty()) {
//        	System.out.println("priorityQ");
//        	for(DijkstraNode node:priorityQueue) {
//        		System.out.print(node.getData() + "=>");
//        	}
//        	System.out.println();
        	
        	current = priorityQueue.poll();
//        	System.out.println("poll: " + current.getData());
//        	System.out.println("-------------------------------------------------------");

            // 현재 노드의 이웃 노드들을 순회
            for (Map.Entry<DijkstraNode, Integer> entry : current.neighbors.entrySet()) {
            	DijkstraNode neighbor = entry.getKey();
            	
            	// 현재거리 + 이웃노드까지의 거리
            	int newDistance = distances.get(current) + entry.getValue() + getAdditionalTime(current, neighbor);;

                // 이웃에 대한 정보가 없다면 or 새로운 거리가 더 짧다면 업데이트
                if (!distances.containsKey(neighbor) || newDistance < distances.get(neighbor)) {
                	// neighbor까지의 새 거리 추가
                    distances.put(neighbor, newDistance);
                    // 큐에 추가
                    priorityQueue.add(neighbor);
                    // 최단거리 경로를 찾기 위한 parent노드 설정
                    neighbor.setParent(current);
                }
            }
            
//            System.out.println("distances");
//            for(Map.Entry<DijkstraNode, Integer> distance: distances.entrySet()) {
//            	System.out.println(distance.getKey().getData() + ": " + distance.getValue());
//            }
        }
        
        
        return distances;
    }
    
    // 특정상황시 시간 추가
    public int getAdditionalTime(DijkstraNode current, DijkstraNode neighbor) {
    	int addTime = 0;
    	
    	// 부모 노드
    	DijkstraNode parentNode = current.getParent();
    	
    	// null처리 (getParent()가 null이면 pass)
    	if(parentNode != null) {
    		
    		// 환승역인 경우 (전역과 다음역에 같은 호선이 없으면) 3분 추가
    		// flag: 같은 호선이 있는가?
    		Boolean flag = false;
    		for(String parentLine:parentNode.getLines()) {
    			if(neighbor.getLines().contains(parentLine)) {
    				flag = true;
    				break;
    			}
    		}
    		// 전역과 다음역에 같은 호선이 없으면
    		if(!flag) {
//    			System.out.println(parentNode.getData() + " - " + current.getData() + " - " + neighbor.getData() + " 환승 +1");
    			addTime += 3;
    		}
    	}
    	
    	// 역 2개당 1분씩 추가
//    	getPath(neighbor, parentNode)
    	// 추가중...
    	
    	
        return addTime;
    }
    
    
    // 최단경로 반환 (다익스트라 알고리즘 끝낸 distances 내부의 노드 사용 필요)
    public List<DijkstraNode> getPath(DijkstraNode start, DijkstraNode goal) {
        List<DijkstraNode> path = new ArrayList<>();
        DijkstraNode current = goal;

        while (current != null) {
            path.add(current);
            current = current.getParent();
        }

        Collections.reverse(path);
        return path;
    }
    
    // input: (전체 역 리스트, 출발역)
    // 5분이내로 도착할 수 있는 모든 역을 반환
    public List<SubwayStationTO> getStationsNearStart(List<SubwayStationTO> stations, String startStation, int limitMinute){
    			// Map< 역이름, List<호선> >
    			Map<String, List<String>> stationLines = new HashMap<>();
    			
    			// 호선 정보를 역 하나에 전부 보관하기 위한 stations 가공
    			for(SubwayStationTO stationTO:stations) {
    				String name = stationTO.getName();
    				String line = stationTO.getSubway_line();
    				
    				List<String> temp;
    				// Map에 이미 있으면 호선정보 추가
    				if(stationLines.containsKey(name)) {
    					temp = stationLines.get(name);
    					temp.add(line);
    					stationLines.put(name, temp);

    				// Map에 없으면 새로 추가 
    				} else {
    					temp = new ArrayList<>();
    					temp.add(line);
    					stationLines.put(name, temp);
    				}
    			}
    			
    			
    			// default 노드 생성을 위한 클래스
    			MakeDefaultNeighbors makeDefault = new MakeDefaultNeighbors();
    			
    			// 다익스트라 알고리즘을 이용한 일정시간 이내 역세권 매물 찾기 - 테스트
    					List<DijkstraNode> dijkstraNodes = new ArrayList<>(); 
    					DijkstraAlgo dijkstra = new DijkstraAlgo();
    					
    					for(Map.Entry<String, List<String>> stationLine:stationLines.entrySet()) {
    						
    						// 역이름과 호선 삽입
    						DijkstraNode dijkstraNode = new DijkstraNode();
    						dijkstraNode.setName(stationLine.getKey());
    						dijkstraNode.setLines(stationLine.getValue());
    						
    						dijkstraNodes.add(dijkstraNode);
    					}
    					
    					dijkstraNodes = makeDefault.makeaDefaults(dijkstraNodes);
    					
//    					String startStation = "강남"; 	// 출발 역

    					// stream 이용해 역이름 찾기
    			        Optional<DijkstraNode> startNodeOpt = dijkstraNodes.stream()
    			                .filter(node -> startStation.equals(node.getData()))
    			                .findFirst();
    			        
    			        // 5분이내 역 리스트
    			        List<SubwayStationTO> stationLists = new ArrayList<>();
    			        
    			        
    			        if (startNodeOpt.isPresent()) {
    			            DijkstraNode startNode = startNodeOpt.get();
    			            
    			            // 역까지의 최단시간경로 Map
    			            Map<DijkstraNode, Integer> result = dijkstra.dijkstra(startNode);
    			            
    			            // 결과 출력
//    			          System.out.println(startStation + " 으로부터 5분이내 역:");
    			            
    			            
    			            for (Map.Entry<DijkstraNode, Integer> entry : result.entrySet()) {
    			            	// 역에 대한 거리가 n분 이내
    				            if(entry.getValue() <= limitMinute){
    				            	String dstStation = (String) entry.getKey().getData(); 	// 도착 역
    				            	SubwayStationTO stationTO = new SubwayStationTO();
    				            	// to에 이름 추가
    				            	stationTO.setName(dstStation);
    				            	// 결과에 to 추가
    				            	stationLists.add(stationTO);
    				            	
//    				            	System.out.println(entry.getKey().getData() + ": " + entry.getValue());
    				            	
//    				            	// 역 노드 리스트에서 도착역노드 찾기
//    				            	Optional<DijkstraNode> dstNodeOpt = dijkstraNodes.stream()
//    				            			.filter(node -> dstStation.equals(node.getData()))
//    				            			.findFirst();
//    				            	
//    				            	// 도착역이 주어졌을때 최단경로 추출
//    				            	if(dstNodeOpt.isPresent()) {
//    				            		System.out.println(startStation + "역부터 " + dstStation + "역 까지의 최단거리 경로");
//    				            		List<DijkstraNode> path = dijkstra.getPath(startNodeOpt.get(), dstNodeOpt.get());
//    				            		
//    				            		for(DijkstraNode node:path) {
//    				            			System.out.print(node.getData() + " => ");
////    					            		System.out.print(node.getData() + "역의 호선: ");
////    					            		for(String line:node.getLines()) {
////    					            			System.out.print(line + ", ");
////    					            		}
////    					            		System.out.println();
//    				            		}
//    				            		System.out.println("\n");
//    				            	}
    				            }
    			            }
    			        
    			        } else {
    			            System.out.println("Node not found");
    			        }
    			        
    			        return stationLists;
    }

}