package examples.MiniWAS;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonExam {
    public static void main(String[] args) throws Exception {
        MiniWasConfiguration miniWasConfiguration =
                new MiniWasConfiguration();

        miniWasConfiguration.setStaticDir("/tmp/www");
        miniWasConfiguration.setClassPath(".");

        // 객체를 json메시지로 변환한다.( 직렬화 한다!)
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonConfiguration =
                objectMapper.writeValueAsString(miniWasConfiguration);
        // 위에 메소드에서 new File() 하면서 json값을 파일에 저장할수있따.

        System.out.println(jsonConfiguration);

        // 역직렬화
        String jsonData = "{\"staticDir\":\"/tmp/www\",\"classPath\":\".\"}";
        MiniWasConfiguration readObj =
                objectMapper.readValue(jsonData, MiniWasConfiguration.class);
        // 위에 메소드에서 new File() 하면서 json 파일을 불러올수있다.

        System.out.println(readObj.getClassPath());
        System.out.println(readObj.getStaticDir());

    }


}