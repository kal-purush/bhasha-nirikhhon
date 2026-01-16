/*     */ package com.bedrossian.termon;
/*     */ 
/*     */ import com.google.gson.Gson;
/*     */ import java.io.BufferedReader;
          /*     */ import java.io.IOException;
/*     */ import java.io.InputStream;
/*     */ import java.io.InputStreamReader;
/*     */ import java.net.URL;
/*     */ import java.util.Timer;
/*     */ import java.util.TimerTask;
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ 
/*     */ public class App
/*     */ {
/*     */   public static synchronized void main(String[] args) {
/*  25 */     int refresh = Integer.valueOf(args[0]).intValue();
/*  26 */     final String address = args[1];
/*     */     
			  System.out.println(ConsoleColors.GREEN + "Looking for server http://"+ address + "/data.json");
/*     */     
			  
			  /*String o = "";
			  while (true){
				  try {
					  o = getPrintout(address);
					  
					  clearConsole();
				  
					  System.out.print(o);
					  
					  Thread.sleep(refresh/2);
					  
					  
					  
					  
				  } catch (InterruptedException e) {
					// TODO Auto-generated catch block
					  e.printStackTrace();
				  }
				  
			  }*/
			 
			  
/*  31 */     Timer t = new Timer();
			    t.schedule(new TimerTask()
			        {
			         @Override
					public void run() {
			           String o = getPrintout(address);
			           clearConsole();
			           System.out.print(o);
			         }
			     }, 0L, refresh);
				
/*     */   }
/*     */ 
/*     */ 
/*     */ 
/*     */   
/*     */   public static synchronized void clearConsole() {
/*     */     try {
/*  47 */       String os = System.getProperty("os.name");
/*     */       
/*  49 */       if (os.contains("Windows"))
/*     */       {
/*  51 */         (new ProcessBuilder(new String[] { "cmd", "/c", "cls" })).inheritIO().start().waitFor();
/*     */       }
/*     */       else
/*     */       {
/*  55 */         System.out.println("\033[H");
				  //System.out.println("\\033[2J");
/*  56 */         System.out.flush();
/*     */       }
/*     */     
/*  59 */     } catch (Exception e) {
/*     */       
/*  61 */       System.out.println(e.getMessage());
/*     */     } 
/*     */   }
/*     */ 
/*     */   public static boolean found = false;
/*     */   public static synchronized String getPrintout(String address) {
				
/*  67 */     String json = getJSON("http://" + address + "/data.json");
		      if (json==null) {
		    	  found = false;
		    	  return "Searching...";
		      }else{
		    	  if (!found) {
		    		  System.out.print("\033[H\033[2J");
		    	  }
		    	  found = true;
		    
		    	  
		      }
/*  68 */     Sensor map = JSONtoMAP(json);
/*     */     
				
/*  70 */     Sensor computer = map.Children[0];
/*  71 */     Sensor mobo = computer.Children[0];
/*  72 */     //Sensor cpu = computer.Children[1];
/*  73 */     Sensor mem = computer.Children[2];
/*  74 */     //Sensor gpu1 = computer.Children[3];
/*  75 */     //Sensor gpu2 = computer.Children[4];
/*     */ 
/*     */     
/*     */ 
/*     */     
              StringBuilder ALL = new StringBuilder();
/*  80 */     
              ALL.append("Computer: " + computer.Text);
/*  81 */     ALL.append(System.getProperty("line.separator"));
			  ALL.append("Motherboard: " + mobo.Text);
/*     */     ALL.append(System.getProperty("line.separator"));
			  ALL.append(System.getProperty("line.separator"));
/*     */ 
/*     */     

/*     */ 
/*     */     
/*  95 */     

			  int gpuCount = 0;
			  Sensor sens;
			  for (int c=0;c<map.Children[0].Children.length;c++) {
				  sens = map.Children[0].Children[c];
				  if (sens.Text.contains("NVIDIA")) {
					  ALL.append("GPU "+gpuCount+": " + sens.Text);
					  ALL.append(System.getProperty("line.separator")+"|");
					  ALL.append(printGPU(sens));
					  ALL.append(System.getProperty("line.separator"));
					  ALL.append(System.getProperty("line.separator"));
					  gpuCount++;
				  }else {
					  if (sens.Text.contains("Core")) {
						  ALL.append("CPU: " + sens.Text);
						  ALL.append(System.getProperty("line.separator"));
						  ALL.append("Bus:    " + Colorize(fixedLengthString((((sens.Children[0]).Children[0]).Value), 8)) + " | " + Colorize(fixedLengthString(((sens.Children[1]).Children[0]).Value, 8)) + " | " + Colorize(fixedLengthString((((sens.Children[2]).Children[0]).Value), 6))+ " ");
						  ALL.append(System.getProperty("line.separator"));
						  for (int i = 1; i < (sens.Children[0]).Children.length; i++) {
						  		ALL.append("Core " + i + ": " + fixedLengthString(Colorize(((sens.Children[0]).Children[i]).Value), 8) +" | " + Colorize(fixedLengthString(((sens.Children[1]).Children[i]).Value, 8)) + " | " + Colorize(fixedLengthString((((sens.Children[2]).Children[i]).Value), 6)) + " ");
						  		ALL.append(System.getProperty("line.separator"));
						  }
						  ALL.append(System.getProperty("line.separator"));
					  }else {
						  if (sens.Text.contains("Memory")) {
							  ALL.append("Memory: " + sens.Text);
							  ALL.append(System.getProperty("line.separator"));
							  ALL.append("RAM: " + fixedLengthString(Colorize(((sens.Children[0]).Children[0]).Value), 8) + " | " + fixedLengthString(((sens.Children[1]).Children[0]).Value, 7) + " Used | " + fixedLengthString(((sens.Children[1]).Children[1]).Value, 7) + " Free");
							  
							  ALL.append(System.getProperty("line.separator"));
							  ALL.append(System.getProperty("line.separator"));
						  }
					  }
				  }
					  
			  }
/*     */     
/* 101 */     
/* 104 */     
			  /*ALL.append("GPU 2: " + gpu2.Text);
			  ALL.append(System.getProperty("line.separator")+"|");
     		  ALL.append(printGPU(gpu2));
			  ALL.append(System.getProperty("line.separator"));
			  ALL.append(ConsoleColors.GREEN);
			  ALL.append(System.getProperty("line.separator"));
			  */
			  //App.clearConsole();
			  
			 return (ALL.toString());
			  
/*     */   }
/*     */  

	public static String Colorize(String input) {
		//return input;
		
		if(input.contains(" °")) {
			float t = Float.parseFloat(input.substring(0, input.indexOf(" °")));
			if(t>=70)
				return ConsoleColors.YELLOW + input + ConsoleColors.GREEN;// + input.substring(input.indexOf(" °"), input.length());
			if(t>=80)
				return ConsoleColors.RED + input + ConsoleColors.GREEN;// + input.substring(input.indexOf(" °"), input.length());
			return ConsoleColors.GREEN + input + ConsoleColors.GREEN;// + input.substring(input.indexOf(" °"), input.length());
		}else {
			if (input.contains(" MHz")) {
				return ConsoleColors.WHITE + input + ConsoleColors.GREEN;
			}else {
				if (input.contains("%")) {
					float t = Float.parseFloat(input.substring(0, input.indexOf(" %")));
					if (t>95) {
						return ConsoleColors.RED + input + ConsoleColors.GREEN;
					}
					return ConsoleColors.CYAN+ input + ConsoleColors.GREEN;
				}else {
					return ConsoleColors.GREEN + input + ConsoleColors.GREEN;
				}	
			}
		}
		
		
	}

	public static String printGPU(Sensor gpu) {
		byte b;
		int i;
		Sensor[] arrayOfSensor;
		StringBuilder result = new StringBuilder();
		int p = 0;
		String TableItems[] = new String[8];
		for (i = (arrayOfSensor = gpu.Children).length, b = 0; b < i;) {
			Sensor d = arrayOfSensor[b];
			if (!d.Text.equals("Data")) {
				byte b1;
				int j;
				Sensor[] arrayOfSensor1;
				for (j = (arrayOfSensor1 = d.Children).length, b1 = 0; b1 < j;) {
					Sensor x = arrayOfSensor1[b1];
					TableItems[p++] = (String.valueOf(arrayOfSensor1[b1].Text) + " " +  Colorize(fixedLengthString(x.Value,8)) + " | ");
					b1++;
				}
			}
			b++;
		}
		
		for(int id = 0; id < TableItems.length; id++) {
			String a = TableItems[id];
			String h;
			if (id<4)
				h = TableItems[id+4];
			else
				h = TableItems[id-4];
			
			result.append(fixedLengthString(a,((a.length()>h.length()) ? a.length() : h.length())));
			if (id==3) {
				result.append(System.getProperty("line.separator") + "|");
			}
		}
		return result.toString();
	}
/*     */   
/*     */   public static String fixedLengthString(String string, int length) {
/* 126 */     return String.format("%1$" + length + "s", new Object[] { string });
/*     */   }
/*     */ 
/*     */   
/*     */   public static Sensor JSONtoMAP(String json) {
/* 131 */     Gson gson = new Gson();
/*     */ 
/*     */     
/* 134 */     Sensor map = gson.fromJson(json, Sensor.class);
/*     */     
/* 136 */     return map;
/*     */   }
/*     */ 
/*     */   
/*     */   public static String getJSON(String address) {
/* 141 */     InputStream is = null;
/*     */     
/* 143 */     String json = null;
/*     */     
/*     */     try {
/* 146 */       URL url = new URL(address);
/* 147 */       is = url.openStream();
/* 148 */       BufferedReader br = new BufferedReader(new InputStreamReader(is));
/*     */       
/* 150 */       json = br.readLine();
/*     */ 
/*     */     
/*     */     }
/* 154 */     catch (Exception e) {
				System.out.println("Timed out.");
/*     */     } finally {
/*     */       try {
/* 160 */         if (is != null) is.close(); 
/* 161 */       } catch (IOException iOException) {}
/*     */     } 
/*     */ 
/*     */     
/* 165 */     return json;
/*     */   }
/*     */ }


/* Location:              C:\Users\alexb\Desktop\homelab.jar!\com\bedrossian\termon\App.class
 * Java compiler version: 5 (49.0)
 * JD-Core Version:       1.1.3
 */