package me.tWizT3d_dreaMr.colors;

import java.util.List;

import org.bukkit.entity.Player;

public class color {
	public static String add(String s,List<String> list, Player p){
		String result="";
		while(s.startsWith(" ")) {
			s=s.substring(1);
		}
		if(s.contains("&H")) {
			s.replaceAll("&H", "&h");
		}
		while(s.contains("&h")) {
			String[]end=add2(s);
			String setcolor=end[1];
			int i=0;
			int i2=0;
			while(i<setcolor.length()){
				if(i2==list.size()) {
					i2=0;
				}
				String color=list.get(i2);
				if(color.equalsIgnoreCase("&b")&!p.hasPermission("chatcontrol.chat.format.color.b)")) color="&d";
				if(color.equalsIgnoreCase("&a")&!p.hasPermission("chatcontrol.chat.format.color.a)")) color="&d";
				if(color.equalsIgnoreCase("&0")&!p.hasPermission("chatcontrol.chat.format.color.0)")) color="&d";
				
				result=result+color+setcolor.substring(i, i+1);

				
				i++;
				i2++;
			}s=end[0]+result+end[2];
		}
		return s;
	}public static String addc(String s,List<String> list, Player p){
		String result="";
		while(s.startsWith(" ")) {
			s=s.substring(1);
		}
		if(s.contains("&H")) {
			s.replaceAll("&H", "&h");
		}
		while(s.contains("&h")) {
			String[]end=add2(s);
			String setcolor=end[1];
			int i=0;
			int i2=0;
			while(i<setcolor.length()){
				if(i2==list.size()) {
					i2=0;
				}
				String color=list.get(i2);
				result=result+color+setcolor.substring(i, i+1);

				
				i++;
				i2++;
			}s=end[0]+result+end[2];
		}
		return s;
	}
	public static String[] add2(String s){
		if(s.toLowerCase().contains("&h")){
			String colorme=s.substring(s.indexOf("&h"));String c2="";
			if(colorme.contains("&h"))
			c2=colorme.replaceAll("&h","");
			String dont="";
			if(s.indexOf("&h")!=0)dont=s.replace(colorme, "");
			String aft="";
			if(c2.contains("&")){
				aft=colorme;
				if(colorme.contains("&h")) {
					c2=colorme.replace("&h","");colorme=c2;}
				colorme=colorme.substring(0,colorme.indexOf("&"));
				aft=c2.substring(c2.indexOf("&"));
				
			}
			if(colorme.contains("&h")) {
			c2=colorme.replace("&h","");colorme=c2;
			colorme.replace("&h","");}
			String[] end= {dont,colorme,aft};
			return end;
			
		}
		return null;
		
	}public static String addanv(String s,List<String> list){
		String result="";
		while(s.startsWith(" ")) {
			s=s.substring(1);
		}
		if(s.contains("&H")) {
			s.replaceAll("&H", "&h");
		}
		while(s.contains("&h")) {
			String[]end=addanv2(s);
			String setcolor=end[1];
			int i=0;
			int i2=0;
			while(i<setcolor.length()){
				if(i2==list.size()) {
					i2=0;
				}
				String color=list.get(i2);
				result=result+color+setcolor.substring(i, i+1);

				
				i++;
				i2++;
			}s=end[0]+result+end[2];
		}
		return s;
	}
	public static String[] addanv2(String s){
		if(s.toLowerCase().contains("&h")){
			String colorme=s.substring(s.indexOf("&h"));String c2="";
			if(colorme.contains("&h"))
			c2=colorme.replaceAll("&h","");
			String dont="";
			if(s.indexOf("&h")!=0)dont=s.replace(colorme, "");
			String aft="";
			if(c2.contains("&")){
				aft=colorme;
				if(colorme.contains("&h")) {
					c2=colorme.replace("&h","");colorme=c2;}
				colorme=colorme.substring(0,colorme.indexOf("&"));
				aft=c2.substring(c2.indexOf("&"));
				
			}
			if(colorme.contains("&h")) {
			c2=colorme.replace("&h","");colorme=c2;
			colorme.replace("&h","");}
			String[] end= {dont,colorme,aft};
			return end;
			
		}
		return null;
		
	}
}