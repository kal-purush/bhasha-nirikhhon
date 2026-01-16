public static String PostRequest(String URL,Map parameterMap) { 
		
		  
        StringBuffer parameterBuffer = new StringBuffer();
        if (parameterMap != null) {
            Iterator iterator = parameterMap.keySet().iterator();
            String key = null;
            String value = null;
            while (iterator.hasNext()) {
                key = (String)iterator.next();
                if (parameterMap.get(key) != null) {
                    value = (String)parameterMap.get(key);
                } else {
                    value = "";
                }
                
                parameterBuffer.append(key).append("=").append(value);
                if (iterator.hasNext()) {
                    parameterBuffer.append("&");
                }
            }
        }
        
        System.out.println("POST parameter : " + parameterBuffer.toString());
        
        String obj = parameterBuffer.toString();
        String jsonString="";
        try { 
            //创建连接 
            URL url = new URL(URL); 
            HttpURLConnection connection = (HttpURLConnection) url 
                    .openConnection(); 
            connection.setDoOutput(true); 
            connection.setDoInput(true); 
            connection.setRequestMethod("POST"); //设置请求方法
            connection.setRequestProperty("Charsert", "UTF-8"); //设置请求编码
            connection.setUseCaches(false); 
            connection.setInstanceFollowRedirects(true); 
            connection.setRequestProperty("Content-Type", 
                    "application/x-www-form-urlencoded"); 
            connection.connect(); 
//            PrintWriter out = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(),"utf-8"));  
//            out.println(obj);

            //POST请求 
            DataOutputStream out = new DataOutputStream( 
                    connection.getOutputStream()); //关键的一步


            out.writeBytes(obj); 
            out.flush(); 
            out.close(); 

            // 读取响应
            if (connection.getResponseCode()==200) {
            	
         	    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(),"gbk"));
                String lines;
                StringBuffer sb = new StringBuffer("");
                while ((lines = reader.readLine()) != null) {
                    lines = new String(lines.getBytes(), "utf-8");
                    lines +="\n";
                    
                    sb.append(lines);
                }
                jsonString=sb.toString();
                reader.close();
            }//返回值为200输出正确的响应信息

            if (connection.getResponseCode()==400) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                String lines;
                StringBuffer sb = new StringBuffer("");
                while ((lines = reader.readLine()) != null) {
                    lines = new String(lines.getBytes(), "utf-8");
                    sb.append(lines);
                }
                jsonString=sb.toString();
                reader.close();
            }//返回值错误，输出错误的返回信息
            // 断开连接 
            connection.disconnect(); 
        } catch (MalformedURLException e) { 
            // TODO Auto-generated catch block 
            e.printStackTrace(); 
        } catch (UnsupportedEncodingException e) { 
            // TODO Auto-generated catch block 
            e.printStackTrace(); 
        } catch (IOException e) { 
            // TODO Auto-generated catch block 
            e.printStackTrace(); 
        } 
        return jsonString;
    } 