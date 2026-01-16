package com.apidocs.test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.StringUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lifeix.football.common.util.FileUtil;
import com.lifeix.football.common.util.HttpUtil;
import com.lifeix.football.common.util.StringUtil;

import jcifs.smb.NtlmPasswordAuthentication;

/**
 * @author xule
 * @version 2017年4月25日 下午1:30:40
 */
public class CreateJsons {
    private static Logger logger = LoggerFactory.getLogger(CreateJsons.class);

    // private static final String API_PREFIX = "http://54.223.127.33:8000/football/";
    private static final String API_PREFIX = "http://127.0.0.1:8080/football/";

    private static final String reg_response = "#/definitions/Response«([^\\s'\"«»]*)»";
    private static final String reg_list = "#/definitions/Response«List«([^\\s'\"«»]*)»»";
    private static final String reg_object = "#/definitions/(?!Response)([^\\s'\"«»]*)";

    private static final Pattern p_response = Pattern.compile(reg_response);
    private static final Pattern p_list = Pattern.compile(reg_list);
    private static final Pattern p_object = Pattern.compile(reg_object);

    private static final String[] SYSTEMS = { "user" };

    private static final String temp_$ref_replacer = "￥ref";

    private static final List<TypeMapper> TypeMappers = Arrays.asList(
            new TypeMapper("Byte", "byte", "string", "byte", "#/definitions/Response«byte»"),
            new TypeMapper("Character", "char", "string", "", "#/definitions/Response«Character»"),
            new TypeMapper("Boolean", "boolean", "boolean", "", "#/definitions/Response«boolean»"),
            new TypeMapper("Integer", "int", "integer", "int32", "#/definitions/Response«int»"),
            new TypeMapper("Short", "short", "integer", "int32", "#/definitions/Response«int»"),
            new TypeMapper("Long", "long", "integer", "int64", "#/definitions/Response«long»"),
            new TypeMapper("Double", "double", "number", "double", "#/definitions/Response«double»"),
            new TypeMapper("Float", "float", "number", "float", "#/definitions/Response«float»"),
            new TypeMapper("", "string", "string", "", "#/definitions/Response«string»"));

    private static TypeMapper findTypeMapper(String key, String value) throws Exception {
        Class<TypeMapper> clazz = TypeMapper.class;
        Field field = clazz.getDeclaredField(key);
        for (TypeMapper typeMapper : TypeMappers) {
            field.setAccessible(true);
            String value2 = (String) (field.get(typeMapper));
            if (value.equals(value2)) {
                return typeMapper;
            }
        }
        return null;
    }

    private static TypeMapper findTypeMapperByTypeAndFormat(String jsontype, String jsonformat)
            throws Exception {
        for (TypeMapper typeMapper : TypeMappers) {
            if (typeMapper.getJsontype().equals(jsontype) && typeMapper.getJsonformat().equals(jsonformat)) {
                return typeMapper;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        /**
         * 获得原始json
         */
        List<String> originalJsons = getOriginalJsons();
        /**
         * 解析json
         */
        List<List<JSONObject>> newJsons = parseOriginalJsons(originalJsons);
        /**
         * 将json生成文件，并写入指定目录的文件夹中
         */
        putApiJsonFiles(newJsons);
    }

    /**
     * 将json生成文件，并写入指定目录的文件夹中
     * @author xule
     * @version 2017年4月27日 下午5:21:04
     * @param 
     * @return void
     * @throws Exception 
     */
    private static void putApiJsonFiles(List<List<JSONObject>> newJsons) throws Exception {
        String SMBDIR = "smb://192.168.1.17/fb/temp/api_docs/";
        String clientFilePath = SMBDIR+"outer/";
        String serviceFilePath = SMBDIR+"inner/";
        String reg_inner_path = "/([^/]*)";
        String reg_outer_path = "/v2/football/([^/]*)";
        Pattern p_inner = Pattern.compile(reg_inner_path);
        Pattern p_outer = Pattern.compile(reg_outer_path);
        for (int i = 0, size = newJsons.size(); i < size; i++) {
            List<JSONObject> list = newJsons.get(i);
            for (JSONObject json : list) {
                String basePath = json.getString("basePath");
                Pattern pattern = i == 0 ? p_inner : p_outer;
                Matcher matcher = pattern.matcher(basePath);
                String jsonName = null;
                if (matcher.find()) {
                    jsonName = matcher.group(1);
                }
                /**
                 * json写入文件
                 */
                String filePath = (i == 0 ? serviceFilePath : clientFilePath) + StringUtil.firstUpcase(jsonName) + ".json";
                String text = json.toJSONString().replaceAll(temp_$ref_replacer, "\\$ref");
                SmbUtil.writeFile(filePath, text);
            }
        }
    }

    /**
     * 解析原始json
     * @author xule
     * @version 2017年4月27日 下午5:18:48
     * @param 
     * @return List<List<JSONObject>>
     */
    private static List<List<JSONObject>> parseOriginalJsons(List<String> originalJsons) throws Exception {
        List<JSONObject> listInner = new ArrayList<>();
        List<JSONObject> listOuter = new ArrayList<>();
        for (String originalJson : originalJsons) {
            JSONObject jsonObject1 = JSONObject.parseObject(originalJson);
            JSONObject jsonObject2 = JSONObject.parseObject(originalJson);
            if (StringUtils.isEmpty(jsonObject1)) {
                throw new Exception(
                        "parseOriginalJsons error, innerJson is empty. originalJson=" + originalJson);
            }
            if (StringUtils.isEmpty(jsonObject2)) {
                throw new Exception(
                        "parseOriginalJsons error, outerJson is empty. originalJson=" + originalJson);
            }
            parseInnerJsons(jsonObject1);
            parseOuterJsons(jsonObject2);
            jsonObject1.put("basePath", jsonObject1.getString("basePath").replaceAll("/football", ""));
            jsonObject2.put("basePath", "/v2" + jsonObject2.getString("basePath"));
            listInner.add(jsonObject1);
            listOuter.add(jsonObject2);
        }
        return Arrays.asList(listInner, listOuter);
    }
    
    /**
     * 处理responses除200以外的其他code
     * @author xule
     * @version 2017年4月27日 下午4:04:59
     * @param 
     * @return void
     */
    private static void handleOtherResponseCode(JSONObject responsesJsonObject) {
        responsesJsonObject.remove("201");
        responsesJsonObject.remove("404");
        responsesJsonObject.put("401", JSONObject.parse("{\"description\":\"未登录\"}"));
        responsesJsonObject.put("403", JSONObject.parse("{\"description\":\"无权访问\"}"));
    }
    
    /**
     * 处理produces
     * @author xule
     * @version 2017年4月27日 下午4:07:25
     * @param 
     * @return void
     */
    private static void handleProduces(JSONObject methodJsonObject, JSONObject responses200SchemaJsonObject) {
        JSONArray producesJsonArray = methodJsonObject.getJSONArray("produces");
        String responseType = responses200SchemaJsonObject.getString("type");
        String responseFormat = responses200SchemaJsonObject.getString("format");
        if (producesJsonArray.size() != 0) {
            producesJsonArray.remove(0);
        }
        if ("string".equals(responseType) && StringUtils.isEmpty(responseFormat)) {// 接口返回类型为String
            producesJsonArray.add(MimeTypeUtils.TEXT_PLAIN_VALUE);// "text/plain"
        } else {
            producesJsonArray.add(MimeTypeUtils.APPLICATION_JSON_VALUE);// "application/json"
        }
    }
    
    /**
     * 将json解析成服务端内部使用的形式
     * 
     * @author xule
     * @version 2017年4月25日 下午2:01:52
     * @param
     * @return String
     * @throws Exception
     */
    private static void parseInnerJsons(JSONObject jsonObject1) throws Exception {
        JSONObject pathsJson = jsonObject1.getJSONObject("paths");
        for (String path : pathsJson.keySet()) {
            JSONObject methodsJson = pathsJson.getJSONObject(path);
            for (String methodKey : methodsJson.keySet()) {
                JSONObject methodJsonObject = methodsJson.getJSONObject(methodKey);

                /**************************************************** 处理responses ********************************************************/
                JSONObject responsesJsonObject = methodJsonObject.getJSONObject("responses");
                /**
                 * 处理responses.200
                 */
                JSONObject responses200JsonObject = responsesJsonObject.getJSONObject("200");
                JSONObject responses200SchemaJsonObject = responses200JsonObject.getJSONObject("schema");
                String $refValue = responses200SchemaJsonObject.getString(temp_$ref_replacer);
                /**
                 * $ref存在，可能使用了Response进行了包装，也可能是其他对象的引用
                 */
                if (!StringUtils.isEmpty($refValue)) {
                    /**
                     * 匹配"#/definitions/Response《xxx》"
                     */
                    Matcher m = p_response.matcher($refValue);
                    boolean flag = false;
                    if (m.find()) {// 匹配成功
                        flag = true;
                        TypeMapper typeMapper = findTypeMapper("response", $refValue);
                        if (typeMapper == null) {// "xxx"为引用类型
                            responses200SchemaJsonObject.put(temp_$ref_replacer,"#/definitions/" + StringUtil.firstUpcase(m.group(1)));// 修改返回类型
                        } else {// "xxx"为基本数据类型或string类型
                            responses200SchemaJsonObject.remove(temp_$ref_replacer);// 移除$ref，改为基本数据类型的书写格式
                            responses200SchemaJsonObject.put("type", typeMapper.getJsontype());// 修改返回类型
                            if (!StringUtils.isEmpty(typeMapper.getJsonformat())) {
                                responses200SchemaJsonObject.put("format", typeMapper.getJsonformat());
                            }
                        }
                    }
                    /**
                     * 匹配"#/definitions/Response《xxx》"失败，尝试匹配"#/definitions/Response《List《xxx》》"
                     */
                    if (!flag) {
                        Matcher m2 = p_list.matcher($refValue);// "#/definitions/Response《xxx》"不会匹配成功，只有"#/definitions/Response《List《xxx》》"才会匹配成功
                        if (m2.find()) {// 匹配成功
                            String dataType = m2.group(1);// 获得数组元素类型
                            TypeMapper typeMapper = findTypeMapper("basetype", dataType);
                            responses200SchemaJsonObject.remove(temp_$ref_replacer);// 移除$ref，改为基本数据类型的书写格式
                            responses200SchemaJsonObject.put("type", "array");// 修改返回类型
                            JSONObject itemsJsonObject = new JSONObject();
                            if (typeMapper != null) {// #/definitions/Response《List《xxx》》中的xxx是基本数据类型或string类型
                                itemsJsonObject.put("type", typeMapper.getJsontype());
                                if (!StringUtils.isEmpty(typeMapper.getJsonformat())) {
                                    itemsJsonObject.put("format", typeMapper.getJsonformat());
                                }
                            } else {// #/definitions/Response《List《xxx》》中的xxx是引用类型
                                itemsJsonObject.put(temp_$ref_replacer,"#/definitions/" + StringUtil.firstUpcase(dataType));
                            }
                            responses200SchemaJsonObject.put("items", itemsJsonObject);
                        }
                    }
                }
                /**
                 * 处理responses其他code
                 */
                handleOtherResponseCode(responsesJsonObject);
                /**
                 * 处理produces
                 */
                handleProduces(methodJsonObject,responses200SchemaJsonObject);
            }
        }
    }

    /**
     * 将json解析成客户端使用的形式
     * 
     * @author xule
     * @version 2017年4月25日 下午2:01:54
     * @param
     * @return String
     * @throws Exception
     */
    private static void parseOuterJsons(JSONObject jsonObject2) throws Exception {
        JSONObject pathsJson = jsonObject2.getJSONObject("paths");
        for (String path : pathsJson.keySet()) {
            JSONObject methodsJson = pathsJson.getJSONObject(path);
            for (String methodKey : methodsJson.keySet()) {
                JSONObject methodJsonObject = methodsJson.getJSONObject(methodKey);

                /**************************************************** 处理responses ********************************************************/
                JSONObject responsesJsonObject = methodJsonObject.getJSONObject("responses");
                /**
                 * 处理responses.200
                 */
                JSONObject responses200JsonObject = responsesJsonObject.getJSONObject("200");
                JSONObject responses200SchemaJsonObject = responses200JsonObject.getJSONObject("schema");
                String $refValue = responses200SchemaJsonObject.getString(temp_$ref_replacer);
                /**
                 * $ref存在，可能使用了Response进行了包装(#/definitions/Response
                 * <Obj>)，也可能是对象的引用(#/definitions/Obj)
                 */
                if (!StringUtils.isEmpty($refValue)) {
                    /**
                     * 匹配#/definitions/Obj
                     */
                    Matcher m = p_object.matcher($refValue);
                    /**
                     * 匹配成功，说明返回类型为$ref="#/definitions/Obj"形式，对其进行包装。如果匹配失败，则返回类型已经使用了Response进行了包装(
                     * #/definitions/Response<Obj>)，不需要做任何处理
                     */
                    while (m.find()) {
                        String dataType = m.group(1);// 获得返回数据类型
                        responses200SchemaJsonObject.put(temp_$ref_replacer,"#/definitions/Response«" + StringUtil.firstUpcase(dataType) + "»");// 修改返回类型
                    }
                }
                /**
                 * $ref不存在，则需要使用Response进行包装
                 */
                else {
                    String responseType = responses200SchemaJsonObject.getString("type");
                    if (!"array".equals(responseType)) {// 返回类型是基本数据类型或string类型
                        String responseFormat = responses200SchemaJsonObject.getString("format");
                        TypeMapper typeMapper = findTypeMapperByTypeAndFormat(responseType == null ? "" : responseType,responseFormat == null ? "" : responseFormat);
                        /**
                         * 返回类型是基本数据类型或string类型，对其进行包装
                         */
                        if (typeMapper != null) {
                            responses200SchemaJsonObject.remove("type");
                            responses200SchemaJsonObject.remove("format");
                            responses200SchemaJsonObject.put(temp_$ref_replacer, typeMapper.getResponse());
                        }
                    } else {// 返回类型是数组
                        JSONObject itemsJsonObject = responses200SchemaJsonObject.getJSONObject("items");
                        String itemsResponseType = itemsJsonObject.getString("type");
                        String itemsResponseFormat = itemsJsonObject.getString("format");
                        TypeMapper typeMapper2 = findTypeMapperByTypeAndFormat(itemsResponseType == null ? "" : itemsResponseType,itemsResponseFormat == null ? "" : itemsResponseFormat);
                        if (typeMapper2 != null) {// 数组元素为基本数据类型或string类型
                            responses200SchemaJsonObject.put(temp_$ref_replacer,"#/definitions/Response«List«" + typeMapper2.getBasetype() + "»»");
                        } else {// 数组元素是引用类型
                            String items$ref = itemsJsonObject.getString(temp_$ref_replacer);
                            if (StringUtils.isEmpty(items$ref)) {
                                throw new Exception();
                            }
                            Matcher m = p_object.matcher(items$ref);
                            boolean flag = false;
                            while (m.find()) {//匹配引用类型的数组元素成功
                                flag = true;
                                responses200SchemaJsonObject.put(temp_$ref_replacer,"#/definitions/Response«List«" + m.group(1) + "»»");
                            }
                            if (!flag) {
                                throw new Exception("p_object匹配失败，path=" + path + ",method=" + methodKey);
                            }
                        }
                        responses200SchemaJsonObject.remove("type");
                        responses200SchemaJsonObject.remove("items");
                    }
                }
                /**
                 * 处理responses其他code
                 */
                handleOtherResponseCode(responsesJsonObject);
                /**
                 * 处理produces
                 */
                handleProduces(methodJsonObject,responses200SchemaJsonObject);
            }
        }
        /**
         * 添加各方法中所引用的不存在的对象，只处理"#/definitions/Response<xxx>"形式的引用
         */
        addInexistentRefObjs(jsonObject2);
    }

    /**
     * 添加各方法中所引用的不存在的对象，只处理"#/definitions/Response<xxx>"形式的引用
     * @author xule
     * @version 2017年4月27日 下午3:09:35
     * @param 
     * @return void
     * @throws Exception 
     */
    private static void addInexistentRefObjs(JSONObject jsonObject) throws Exception {
        String reg_refObj = "#/definitions/Response«([^\\s'\"]*)»";
        Pattern p_refObj = Pattern.compile(reg_refObj);
        String jsonString = jsonObject.toJSONString();
        Matcher matcher = p_refObj.matcher(jsonString);
        Set<String> set=new HashSet<>();
        /**
         * 匹配所有Response对象引用，并获取包装对象
         */
        while(matcher.find()){
            String innerObj = matcher.group(1);
            set.add(innerObj);//"Response«"+innerObj+"»"
        }
        /**
         * 获得已经存在的引用对象
         */
        JSONObject definitionsJsonObject = jsonObject.getJSONObject("definitions");
        Set<String> definitionsKeySet = definitionsJsonObject.keySet();
        /**
         * 获得不存在的引用对象，并将不存在的引用对象添加到jsonObject中
         */
        for (String innerObj : set) {
            String newRefObjName="Response«"+innerObj+"»";
            if (definitionsKeySet.contains(newRefObjName)) {//引用对象已经存在
                continue;
            }
            /**
             * 引用对象不存在，则创建引用对象
             */
            JSONObject refObj=createInexistentRefObj(innerObj);
            /**
             * 将新创建的引用对象添加到jsonObject中
             */
            definitionsJsonObject.put(newRefObjName, refObj);
        }
        
    }

    /**
     * 创建经Response包装的引用对象
     * @author xule
     * @version 2017年4月27日 下午4:12:53
     * @param 
     * @return JSONObject
     * @throws Exception 
     */
    private static JSONObject createInexistentRefObj(String innerObj) throws Exception {
        String objName="Response«"+innerObj+"»";
        String baseResponseObjJsonString="{description: \"通用的接口返回结构\","
                                    + "type: \"object\","
                                    + "properties: {"
                                    + "code: {type: \"string\"},"
                                    + "message: {type: \"string\"}}}";
        /**
         * 获得基本的Response对象
         */
        JSONObject baseResponseJsonObject = JSONObject.parseObject(baseResponseObjJsonString);
        /**
         * 判断innerObj是否为基本数据类型或string类型
         */
        TypeMapper typeMapper = findTypeMapper("response", "#/definitions/"+objName);
        JSONObject dataJsonObject=new JSONObject();
        /**
         * innerObj是基本数据类型或string类型
         */
        if (typeMapper!=null) {
            dataJsonObject.put("type", typeMapper.getJsontype());
            if (!StringUtils.isEmpty(typeMapper.getJsonformat())) {
                dataJsonObject.put("format", typeMapper.getJsonformat());
            }
        }
        /**
         * innerObj是引用类型或数组
         */
        else{
            String reg_list= "List«([^\\s'\"]*)»";
            Pattern p_list = Pattern.compile(reg_list);
            Matcher matcher = p_list.matcher(innerObj);
            boolean flag=false;
            /**
             * innerObj是数组
             */
            while(matcher.find()){
                flag=true;
                dataJsonObject.put("type", "array");
                String listInnerObj = matcher.group(1);
                TypeMapper typeMapper2 = findTypeMapper("basetype", listInnerObj);
                JSONObject itemsJsonObject = new JSONObject();
                if (typeMapper2!=null) {//innerObj数组元素为基本数据类型或string类型
                    itemsJsonObject.put("type", typeMapper2.getJsontype());
                    if (!StringUtils.isEmpty(typeMapper2.getJsonformat())) {
                        itemsJsonObject.put("format", typeMapper2.getJsonformat());
                    }
                }else{//innerObj数组元素为引用类型
                    itemsJsonObject.put("$ref", "#/definitions/"+listInnerObj);
                }
                dataJsonObject.put("items", itemsJsonObject);
            }
            /**
             * innerObj是引用类型
             */
            if (!flag) {
                dataJsonObject.put("$ref", "#/definitions/"+innerObj);
            }
        }
        /**
         * 给Response设置data属性
         */
        JSONObject baseResponsePropertiesJsonObject = baseResponseJsonObject.getJSONObject("properties");
        baseResponsePropertiesJsonObject.put("data", dataJsonObject);
        return baseResponseJsonObject;
    }

    /**
     * 发送http请求，获得原始json数据
     * @author xule
     * @version 2017年4月25日 下午2:02:46
     * @param
     * @return List<String>
     */
    private static List<String> getOriginalJsons() throws Exception {
        List<String> list = new ArrayList<>();
        for (int i = 0, len = SYSTEMS.length; i < len; i++) {
            String system = SYSTEMS[i];
            String url = API_PREFIX + system + "/v2/api-docs?key=visitor";
            try {
                String result = HttpUtil.sendGet(url);
                if (StringUtils.isEmpty(result)) {
                    throw new Exception("HttpUtil.sendGet error, no response. url:" + url);
                }
                /**
                 * 使用￥ref临时替换$ref，防止出现循环引用
                 */
                result = result.replaceAll("\\$ref", temp_$ref_replacer);
                list.add(result);
            } catch (Exception e) {
                throw new Exception("getOriginalJsons error, message:" + e.getMessage());
            }
        }
        return list;
    }
}