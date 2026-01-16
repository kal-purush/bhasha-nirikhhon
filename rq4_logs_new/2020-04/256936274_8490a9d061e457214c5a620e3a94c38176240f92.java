import java.util.*;

class normalFormCheck{
	Map<String,List<String>> sepTableAttri;
	normalFormCheck(){
		sepTableAttri = new HashMap<String,List<String>>();
	}
	public boolean isSecondNF(Map<String,Integer> isPrimeAtt,Map<String,String> fdMap,String[] arrOfAttributes,ArrayList<ArrayList<String>> candidate_keys){
		boolean retval = true;
		//System.out.println("Checking 2nd NF...........");
		for(Map.Entry<String,String> entry : fdMap.entrySet()){
			List<String> non_prime_vals = new ArrayList<String>();
			String cur_key = entry.getKey();
			String cur_val = entry.getValue();
			//System.out.println("__key: "+ cur_key + " cur_val: " + cur_val );
			boolean is_val_non_prime = false;

			String[] cur_val_ar = cur_val.split(",",0);
			for(String sub_cur_val:cur_val_ar){
				if(isPrimeAtt.containsKey(sub_cur_val)==false){
					is_val_non_prime = true;
					non_prime_vals.add(sub_cur_val);
				}
			}
			if(is_val_non_prime==false){
				continue;
			}

			boolean is_key_pSubOfCandi = false;

			List<String> cur_key_ar_list = Arrays.asList(cur_key.split(",",0));
			int len_cur_key_ar = cur_key_ar_list.size();
			for(ArrayList<String> candi_key:candidate_keys){
				if(candi_key.size()>len_cur_key_ar && candi_key.containsAll(cur_key_ar_list)){
					is_key_pSubOfCandi = true;

				/*	for(Map.Entry<String,List<String>> ent : sepTableAttri.entrySet()){
						System.out.println(ent.getValue() + "mp details +++ " + ent.getKey());
					}
					*/

					if(sepTableAttri.containsKey(cur_key)){
						List<String> present_list = sepTableAttri.get(cur_key);
					//	System.out.println("hahhah");
						for(String cur_non_prime_vals:non_prime_vals){
							present_list.add(cur_non_prime_vals);
						}
						sepTableAttri.replace(cur_key,present_list);
					}
					else
						sepTableAttri.put(cur_key,non_prime_vals);
					break;
				}
			}
			if(is_key_pSubOfCandi==true){
				retval = false;;
			}
		}
		return retval;
	}

	public boolean isThirdNF(Map<String,String> fdMap,Map<String,Integer> isPrimeAtt,int num_of_attributes){
		module1Util utilObj = new module1Util();
		boolean retval = true;
		for(Map.Entry<String,String> ent:fdMap.entrySet()){
			ArrayList<String> non_prime_vals = new ArrayList<String>();
			String cur_key = ent.getKey();
			utilObj.initSubsetWithString(cur_key);
			if(utilObj.isSuperKey(fdMap,num_of_attributes)){
				continue;
			}
			boolean is_val_non_prime = true;
			String cur_str_val = ent.getValue();
			String[] ar_val = cur_str_val.split(",");
			for(String cur_val_ar: ar_val){
				if(isPrimeAtt.containsKey(cur_val_ar)==false){
					non_prime_vals.add(cur_val_ar);
					is_val_non_prime = false;
				}
			}
			if(is_val_non_prime==false){
				retval = false;
				sepTableAttri.put(cur_key,non_prime_vals);
			}
		}
		return retval;
	}

	public boolean isBCNF(Map<String,String> fdMap,int num_of_attributes){
		module1Util utilObj = new module1Util();
		boolean retval = true;
		for(Map.Entry<String,String> ent:fdMap.entrySet()){
			ArrayList<String> vals = new ArrayList<String>();
			String cur_key = ent.getKey();
			utilObj.initSubsetWithString(cur_key);
			if(utilObj.isSuperKey(fdMap,num_of_attributes)){
				continue;
			}
			//boolean is_val_non_prime = true;
			String cur_str_val = ent.getValue();
			String[] ar_val = cur_str_val.split(",");
			for(String cur_val_ar: ar_val){
				//if(isPrimeAtt.containsKey(cur_val_ar)==false){
					vals.add(cur_val_ar);
					//is_val_non_prime = false;
				//}
			}
			//if(is_val_non_prime==false){
			retval = false;
			sepTableAttri.put(cur_key,vals);
			//}
		}
		return retval;
	}

	public void decomposeUtil(String[] arrOfAttributes,int highestSatisfied,ArrayList<ArrayList<String>> candidate_keys){
		System.out.println("Current relation with attributes ");
		for(String cur_at: arrOfAttributes){
			System.out.print(cur_at + " ");
		}
		int siz = sepTableAttri.size();
		System.out.println("\nMust be decomposed into " + (siz+1) + " tables,to covert it into " + (highestSatisfied+1)+ "NF" );
		Map<String,Integer> mapOfAttributes = new HashMap<>();
		for(String atts:arrOfAttributes){
			mapOfAttributes.put(atts,new Integer(1));
		}
		int table_id =0;
		for(Map.Entry<String,List<String>> ent:sepTableAttri.entrySet()){
			String cur_key = ent.getKey();
			List<String> cur_val = ent.getValue();
			table_id = table_id + 1;
			System.out.print("R"+table_id + " -> " + cur_key + " ");
			//String[] str_ar = cur_val.split(",");
			for(String cur_str_ar: cur_val){
				if(mapOfAttributes.get(cur_str_ar)==1){
					System.out.print(cur_str_ar + " ");
					mapOfAttributes.put(cur_str_ar,new Integer(0));
				}
			}
			System.out.println("With " + cur_key + " as candidate key");
		}
		System.out.print("R" + (table_id+1)+" -> ");
		for(Map.Entry<String,Integer> ent: mapOfAttributes.entrySet()){
			if(ent.getValue()==1){
				System.out.print(ent.getKey() + " ");
			}
		}
		System.out.print("With ");
		for(ArrayList<String> candi_key:candidate_keys){
			int used=0;
			for(String str_candi_key:candi_key){
				if(mapOfAttributes.get(str_candi_key)!=1){
					used=1;
					break;
				}
			}
			if(used==0){
				for(String str_candi_key:candi_key){
				System.out.print(str_candi_key + " ");
			}
			System.out.print(" or ");
			}
		}
		System.out.println("As candidate key(s)");

	}

	public void printHNF(Map<String,Integer> isPrimeAtt,Map<String,String> fdMap,String[] arrOfAttributes,ArrayList<ArrayList<String>> candidate_keys){
		System.out.println("Highest normal form that is satisfied by given set of attributes and FD's is ");
		int highestSatisfied = 1;
		if(isSecondNF(isPrimeAtt,fdMap,arrOfAttributes,candidate_keys)==false){
			System.out.println("1st Normal Form");

			////
			for(Map.Entry<String,List<String>> entry : sepTableAttri.entrySet()){
				System.out.print("Partial dependecy over : "+ entry.getKey());
				System.out.print(" that determine non prime attributes = ");
				List<String> cur_list = entry.getValue();
				for(String non_prime_list_vals:cur_list){
					System.out.print(non_prime_list_vals + ' ');
				}
				System.out.println();
			}
			////
		}

		else if(isThirdNF(fdMap,isPrimeAtt,arrOfAttributes.length)==false){
			System.out.println("Second Normal Form");
			highestSatisfied = 2;
			////
			for(Map.Entry<String,List<String>> entry : sepTableAttri.entrySet()){
				System.out.print("Transitive dependecy over non super key: "+ entry.getKey());
				System.out.print(" that determine non prime attributes = ");
				List<String> cur_list = entry.getValue();
				for(String non_prime_list_vals:cur_list){
					System.out.print(non_prime_list_vals + ' ');
				}
				System.out.println();
			}
			////
		}

		else if(isBCNF(fdMap,arrOfAttributes.length)==false){
			System.out.println("Third Normal Form");
			highestSatisfied = 3;
			////
			for(Map.Entry<String,List<String>> entry : sepTableAttri.entrySet()){
				System.out.print("Dependecy over non super key: "+ entry.getKey());
				System.out.print(" that determine attribute(s) = ");
				List<String> cur_list = entry.getValue();
				for(String non_prime_list_vals:cur_list){
					System.out.print(non_prime_list_vals + ' ');
				}
				System.out.println();
			}
			////
		}

		else{
			System.out.println("BCNF");
			return;
		}
		decomposeUtil(arrOfAttributes,highestSatisfied,candidate_keys);
	}
}



class module1Util{

	public ArrayList<ArrayList<String>> candidate_keys;
	public List<String> subset;

	public module1Util(){

	}

	public module1Util(ArrayList<String> sub_set){
		subset = sub_set;
	}

	public ArrayList<String> findAttributeClosureUtil(ArrayList<String> subset_plus,Map<String,String> fd_map){
		int flag_fd_applied = 0;
		int cnt=0;
		while(true){
			cnt++;
			//System.out.println("Checking once again");
			int not_new=1;
				for(Map.Entry<String,String> entry : fd_map.entrySet()){
				//	System.out.println("MAP key :" + entry.getKey());
				//	System.out.println("MAP value :" + entry.getValue());
					String[] cur_key = (entry.getKey()).split(",",0);
					String cur_val = entry.getValue();
					int found=1;
					
					for(int i=0;i<cur_key.length;i++){
						if(subset_plus.contains(cur_key[i])==false){
							found=0;
							break;
						}
					}
					if(found==1 ){
						String[] val_ar = cur_val.split(",",0);
						for(String cur_val_ar:val_ar){
							if(subset_plus.contains(cur_val_ar)==false){
								//System.out.println("added "+cur_val_ar);
								not_new=0;
								subset_plus.add(cur_val_ar);
							}
						}
					}
				}
				if(not_new==1){
					//System.out.println("HAHAHA");
					break;
				}
			}
			//System.out.println("Subset+ : " + subset_plus);
			return subset_plus;
	}

	public ArrayList<String> findAttributeClosure(Map<String,String> fd_map){
		int len = subset.size();
		ArrayList<String> subset_plus = new ArrayList<String>();
		for(int i=0;i<len;i++){
			subset_plus.add(subset.get(i));
		}
		return findAttributeClosureUtil(subset_plus,fd_map);
	}

	public boolean isSuperKey(Map<String,String> fd_map,int num_of_attributes){
		ArrayList<String> attribute_closure = findAttributeClosure(fd_map);
	//	System.out.println("Attri closure : " + attribute_closure);
		if(attribute_closure.size()==num_of_attributes){
			return true;
		}
		return false;
	}

	public void initSubsetWithString(String newSubsetStr){

		String[] subset_ar = newSubsetStr.split(",");
		subset = Arrays.asList(subset_ar);

	}

	public void eachSubsetUtil(String[] arrOfAttributes,Map<String,String> fd_map,int ind){

		//System.out.println("Checking for subset :" + subset);
		int sup_key_flag=0;
		if(subset.size()>0 && isSuperKey(fd_map,arrOfAttributes.length)){
			sup_key_flag=1;
			ArrayList<String> subset1 = new ArrayList<String>(subset);
			int to_add=1;
			for(int i=0;i<candidate_keys.size();i++){
				int flg_sub=1;
				if(candidate_keys.size()>0 && candidate_keys.get(i).containsAll(subset)){
					candidate_keys.remove(i);
					flg_sub=0;
				}
				else if(candidate_keys.size()>0 && subset.containsAll(candidate_keys.get(i))){
					to_add=0;
				}
				if(flg_sub==0){
					i--;
				}
			}
			if(to_add==1){
				candidate_keys.add(subset1);
			}
			/*//System.out.println("This is a super key");
			if(candidate_keys.size()==0 || candidate_keys.get(0).size()>subset.size()){
				candidate_keys.clear();
				ArrayList<String> subset1 = new ArrayList<String>(subset);
				candidate_keys.add(subset1);
				//System.out.println("Candi now1:" + candidate_keys);
			}
			else if(candidate_keys.get(0).size()==subset.size()){
				//System.out.println("Candi now2:" + candidate_keys);
				ArrayList<String> subset1 = new ArrayList<String>(subset);
				candidate_keys.add(subset1);
				//System.out.println("Candi now3:" + candidate_keys);

			}*/
		}
		for(int i=ind;i<arrOfAttributes.length;i++){
			//if(sup_key_flag==0)
			//{
				subset.add(arrOfAttributes[i]);
			
				eachSubsetUtil(arrOfAttributes,fd_map,i+1);
			//}
			//if(subset.size()>0)
				subset.remove(subset.size()-1);
		}
		return;
	}

	public ArrayList<ArrayList<String> > eachSubset(String[] arrOfAttributes,Map<String,String> fd_map){
		subset = new ArrayList<String>();
		candidate_keys = new ArrayList<ArrayList<String>>();
		int index = 0;
		eachSubsetUtil(arrOfAttributes,fd_map,index);
		return candidate_keys;
	}
}

class module1 {
	public static void main(String[] args) {
		Scanner scanObj = new Scanner(System.in);
		Scanner scanObj1 = new Scanner(System.in);
		System.out.println("Enter list of attributes seperated by space");
		String attributes_str = scanObj.nextLine();
		String[] arrOfAttributes = attributes_str.split(" ",0);

		//System.out.println("Enter number of FD's:");
		//int num_of_fd = scanObj.nextInt();
		System.out.println("Enter FD's(For FD's like 'AB->CD', enter 'A,B C,D' without quotes) :");
		Map<String,String> fd_map = new HashMap<String,String>();
		while(scanObj1.hasNextLine()){
			String fd1 = scanObj1.nextLine();  //For FD's like 'AB->CD', enter 'A,B C,D' without quotes
			//String fd1 = String.join("", fd1_as_whole);
			String[] arOfFd1 = fd1.split(" ",0);
			//System.out.println("Entered string part1: " + arOfFd1[0] + " Entered string part2: " + arOfFd1[1]);

			//String[] arOfMapVal = arOfFd1[1].split(",",0);
			//for(String cur_str_val:arOfMapVal){
				//System.out.println("mapping " + arOfFd1[0] + "to " + arOfFd1[1]);
			//String newline = System.getProperty("line.separator");
			if(fd1.equals("")){
				break;
			}
			if(fd_map.containsKey(arOfFd1[0])){
				String tmp = fd_map.get(arOfFd1[0]);
				tmp = tmp + "," + arOfFd1[1];
				fd_map.put(arOfFd1[0],tmp);
			}
			else 
				fd_map.put(arOfFd1[0],arOfFd1[1]);  
			//}
		}
		module1Util utilObj = new module1Util();
		ArrayList<ArrayList<String> > candidate_keys = utilObj.eachSubset(arrOfAttributes,fd_map);
		System.out.println("Candidate key(s) : " + candidate_keys);

		Map<String,Integer> isPrimeAttribute = new HashMap<String,Integer>() ;//1 for prime
		for(ArrayList<String> ar_list:candidate_keys){
			for(String prime_att:ar_list){
				//System.out.println("** "+prime_att);
				isPrimeAttribute.put(prime_att,new Integer(1));
			}
		}

		normalFormCheck NF_Obj = new normalFormCheck();
		NF_Obj.printHNF(isPrimeAttribute,fd_map,arrOfAttributes,candidate_keys);
	}
}