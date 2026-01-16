package com.gs.spider.dao;

import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.gs.spider.mapper.SpiderDataMapper;
import com.gs.spider.model.commons.SpiderInfo;
import com.gs.spider.model.commons.Webpage;
import com.gs.spider.model.commons.spiderResultData;
import com.gs.spider.redis.RedisDB3Service;

import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.scheduler.component.DuplicateRemover;

/**
 * 爬虫数据存入数据库
 * 
 * @author edmond
 *
 */
@Component
public class SpdierDBPipeline extends IDAO<Webpage> implements DuplicateRemover, Pipeline {

	private final static int COMPARE_COUNT = 1000;

	private final static String INDEX_NAME = "commons", TYPE_NAME = "webpage";
	private static final String DYNAMIC_FIELD = "dynamic_fields";
	private static final Gson gson = new GsonBuilder()
			.registerTypeAdapter(Date.class,
					(JsonDeserializer<Date>) (json, typeOfT,
							context) -> new Date(json.getAsJsonPrimitive().getAsLong()))
			.registerTypeAdapter(Date.class,
					(JsonSerializer<Date>) (src, typeOfSrc, context) -> new JsonPrimitive(src.getTime()))
			.setDateFormat(DateFormat.LONG).create();
	private static int COUNT = 0;
	private Logger logger = LogManager.getLogger(SpdierDBPipeline.class);
	private Map<String, Set<String>> urls = Maps.newConcurrentMap();

	/**
	 * 已经检查过的存在的type
	 */
	Set<String> checkedIndexTypeSet = ConcurrentHashMap.<String>newKeySet();

	@Autowired
	private SpiderDataMapper spiderDataMapper;

	
	@Autowired
	private RedisDB3Service redisDB3Service;

	@Autowired
	public SpdierDBPipeline(ESClient esClient) {
		super(esClient, INDEX_NAME, TYPE_NAME);
	}

	/**
	 * 将webmagic的resultItems转换成webpage对象
	 *
	 * @param resultItems
	 * @return
	 */
	public static Webpage convertResultItems2Webpage(ResultItems resultItems) {
		Webpage webpage = new Webpage();
		webpage.setContent(resultItems.get("content"));
		webpage.setTitle(resultItems.get("title"));
		webpage.setUrl(resultItems.get("url"));
		webpage.setId(Hashing.md5().hashString(webpage.getUrl(), Charset.forName("utf-8")).toString());
		webpage.setDomain(resultItems.get("domain"));
		webpage.setSpiderInfoId(resultItems.get("spiderInfoId"));
		
		webpage.setImageList(resultItems.get("imageList"));
		if(resultItems.get("gatherTime")!=null){
			webpage.setGathertime(resultItems.get("gatherTime"));
		}else{
			webpage.setGathertime(new Date());
		}
				
		webpage.setSpiderUUID(resultItems.get("spiderUUID"));
		webpage.setKeywords(resultItems.get("keywords"));
		webpage.setSummary(resultItems.get("summary"));
		webpage.setNamedEntity(resultItems.get("namedEntity"));
		webpage.setImageList(resultItems.get("imageList"));
		if(resultItems.get("publishTime")!=null){
			webpage.setPublishTime(resultItems.get("publishTime"));
		}else{
			webpage.setPublishTime(new Date());
		}
		
		webpage.setCategory(resultItems.get("category"));
		webpage.setRawHTML(resultItems.get("rawHTML"));
		webpage.setDynamicFields(resultItems.get(DYNAMIC_FIELD));
		webpage.setStaticFields(resultItems.get("staticField"));
		webpage.setAttachmentList(resultItems.get("attachmentList"));
		webpage.setImageList(resultItems.get("imageList"));
		if(resultItems.get("processTime")!=null){
			webpage.setProcessTime(resultItems.get("processTime"));
		}else{
			webpage.setProcessTime(new Date().getTime());
		}
		
		return webpage;
	}

	@Override
	public String index(Webpage webpage) {
		return null;
	}

	@Override
	protected boolean check() {
		return esClient.checkCommonsIndex() && esClient.checkWebpageType();
	}

	@Override
	public boolean isDuplicate(Request request, Task task) {
		Set<String> tempLists = urls.computeIfAbsent(task.getUUID(), k -> Sets.newConcurrentHashSet());
		// 初始化已采集网站列表缓存
		if (tempLists.add(request.getUrl())) {// 先检查当前生命周期是否抓取过,如果当前生命周期未抓取,则进一步检查ES
			GetResponse response = client.prepareGet(INDEX_NAME, TYPE_NAME,
					Hashing.md5().hashString(request.getUrl(), Charset.forName("utf-8")).toString()).get();
			return response.isExists();
		} else {// 如果当前生命周期已抓取,直接置为重复
			return true;
		}

	}

	@Override
	public void resetDuplicateCheck(Task task) {

	}

	@Override
	public int getTotalRequestsCount(Task task) {
		return COUNT++;
	}

	@Override
	public void process(ResultItems resultItems, Task task) {		
		SpiderInfo spiderInfo = resultItems.get("spiderInfo");
		// ES中的type字段，对应新闻中的频道
		String esTypeName = spiderInfo.getDefaultCategory();
		Webpage webpage = convertResultItems2Webpage(resultItems);
		if (null != webpage) {										
			try {
				String url = webpage.getUrl();
				List<spiderResultData> realdata = new ArrayList<spiderResultData>();
				spiderResultData data = new spiderResultData();			
				data.setArticle_title(webpage.getTitle());
				data.setUrl(url);
				data.setStatus(0);
				data.setArticle_content(webpage.getContent());	
				data.setArticle_categories(esTypeName);
				if(webpage.getPublishTime() == null){
					data.setArticle_publish_time(new Date().getTime() + "");
				}else{
					data.setArticle_publish_time(webpage.getPublishTime().getTime() + "");
				}
				
				if(webpage.getGathertime() == null){
					data.set__time(new Date().getTime() + "");
				}else{
					data.set__time(webpage.getGathertime().getTime() + "");
				}
				
				Map<String, Object> dynamicFieldMap= webpage.getDynamicFields();			

				if(null!=dynamicFieldMap && dynamicFieldMap.containsKey("author")){
					Object obj =dynamicFieldMap.get("author");
					if(obj != null && !"null".equals(obj)){
						data.setArticle_author(obj+"");
					}else{
						data.setArticle_author("网络新闻");
					}
					
				}else{
					data.setArticle_author("网络新闻");
				}
				
							
				realdata.add(data);
				
				// 分频道存储数据
				String tableName = esTypeName + "_2017";
				logger.debug(tableName + " " + url);
				List<spiderResultData> result = spiderDataMapper.selectByUrl(tableName, url);
				if (null == result || 0 == result.size()) {
					try{
						spiderDataMapper.insert(tableName, realdata);						
						redisDB3Service.setValue(webpage.getId(),url);
					}catch(Exception e){
						logger.error("数据库 出错," + e.getLocalizedMessage());
					}
					
					// 存入es TODO
					// saveInES(INDEX_NAME, esTypeName, null, webpage);

				} else {
					logger.info("页面已存在:" + url);
				}

			} catch (Exception e) {
				logger.error("数据解析错误," + e.getLocalizedMessage());
			}
		}

	}

	/**
	 * 清除已停止任务的抓取url列表
	 *
	 * @param taskId
	 *            任务id
	 */
	public void deleteUrls(String taskId) {
		urls.remove(taskId);
		logger.info("任务{}已结束,抓取列表缓存已清除", taskId);
	}

	public boolean isNotEmpty(String title, String publishTime, String content, String res) {
		boolean flag = false;
		if (StringUtils.isNotBlank(title) && StringUtils.isNotBlank(publishTime) && StringUtils.isNotBlank(content)
				&& StringUtils.isNotBlank(res)) {
			flag = true;
		}
		return flag;
	}

	/**
	 * 数据录入ES中
	 * 
	 * @param indexName
	 * @param esTypeName
	 * @param typeJson
	 * @param webpage
	 */
	public void saveInES(String indexName, String esTypeName, String typeJson, Webpage webpage) {

		// 检查类型是否存在，已经检查过不再检查
		String indexType = indexName + "-" + esTypeName;
		if (!checkedIndexTypeSet.contains(indexType)) {
			if (StringUtils.isEmpty(typeJson)) {
				typeJson = "webpage.json";
			}
			esClient.checkType(indexName, esTypeName, typeJson);
		}

		
	}





}