package Coding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class Member{
	int age;
	String name;
	
	public Member(int age, String name) {
		this.age = age;
		this.name = name;
	}
	
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Member [age=" + age + ", name=" + name + "]";
	}
	
	
	
}

public class MemberSort {

	public static void main(String[] args) {
		
		
		List<Member> memlist = new ArrayList<>();
		
		memlist.add(new Member(12,"park"));
		memlist.add(new Member(15,"park"));
		memlist.add(new Member(8,"park"));
		memlist.add(new Member(40,"park"));
		memlist.add(new Member(60,"park"));
		
		List<Member> result = Solution(memlist);
		
		for(Member user : result) {
			System.out.println(user);
		}

	}
	
	public static List<Member> Solution(List<Member> member){
		
		Collections.sort(member,new CompareAgeAsc());
	
		
		return member;
		
	}
	
	

}
/**
 * String으로 오름차순(Asc) 정렬
 **/
class CompareNameAsc implements Comparator<Member>{

    @Override
    public int compare(Member m1, Member m2) {
          return m1.getName().compareTo(m2.getName());
    }       
    
}

/**
 * int로 오름차순(Asc) 정렬
 **/
class CompareAgeAsc implements Comparator<Member>{

    @Override
    public int compare(Member m1, Member m2) {
          return m1.getAge() < m2.getAge() ? -1 : m1.getAge() > m2.getAge() ? 1:0;
    }   
}
