package org.example.generator;

import com.baomidou.mybatisplus.core.toolkit.StringPool;
import com.baomidou.mybatisplus.generator.AutoGenerator;
import com.baomidou.mybatisplus.generator.InjectionConfig;
import com.baomidou.mybatisplus.generator.config.DataSourceConfig;
import com.baomidou.mybatisplus.generator.config.FileOutConfig;
import com.baomidou.mybatisplus.generator.config.GlobalConfig;
import com.baomidou.mybatisplus.generator.config.PackageConfig;
import com.baomidou.mybatisplus.generator.config.StrategyConfig;
import com.baomidou.mybatisplus.generator.config.TemplateConfig;
import com.baomidou.mybatisplus.generator.config.po.TableInfo;
import com.baomidou.mybatisplus.generator.config.rules.NamingStrategy;
import com.baomidou.mybatisplus.generator.engine.FreemarkerTemplateEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.example.common.annotation.Log;
import org.example.common.base.BaseController;
import org.example.common.base.BaseService;
import org.example.common.domain.request.QueryRequest;
import org.example.common.domain.response.PageResult;
import org.example.common.domain.response.Result;
import org.example.common.util.JsonUtil;
import org.example.common.util.MapperUtil;

/**
 * 代码生成器
 */
public class CoverCodeGenerator {

    private static AutoGenerator mpg = new AutoGenerator();

    private static final String AUTHOR = "linzhaoming";

    static {
        GlobalConfig gc = new GlobalConfig();
        gc.setAuthor(AUTHOR);
        gc.setOutputDir(System.getProperty("user.dir") + "/src/main/java");
        gc.setOpen(false);
        gc.setBaseResultMap(Boolean.TRUE);
        gc.setBaseColumnList(Boolean.TRUE);
        mpg.setGlobalConfig(gc);
    }

    private static final String DB_HOST = "47.103.195.154";
    private static final String DB_PORT = "3306";
    private static final String DB = "base";
    private static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "123456";

    static {
        DataSourceConfig dsc = new DataSourceConfig();
        dsc.setUrl(String.format("jdbc:mysql://%s:%s/%s", DB_HOST, DB_PORT, DB));
        dsc.setDriverName(DB_DRIVER);
        dsc.setUsername(DB_USERNAME);
        dsc.setPassword(DB_PASSWORD);
        mpg.setDataSource(dsc);
    }

    private static final String TABLE_PREFIX = "t_";
    private static final String[] TABLES = new String[] { "t_sys_gen" };

    static {
        StrategyConfig sc = new StrategyConfig();
        sc.setTablePrefix(TABLE_PREFIX);
        sc.setInclude(TABLES);
        sc.setNaming(NamingStrategy.underline_to_camel);
        sc.setColumnNaming(NamingStrategy.underline_to_camel);
        sc.setRestControllerStyle(true);
        sc.setSuperControllerClass(BaseController.class.getName());
        sc.setSuperServiceImplClass(BaseService.class.getName());
        mpg.setStrategy(sc);
    }

    private static final String COVER_TEMPLATE_PATH = "/template/cover";
    private static final String MODULE_NAME = "org.example.module.xxx.xxx";

    static {
        PackageConfig pc = new PackageConfig();
        pc.setParent(MODULE_NAME);
        pc.setEntity("domain.entity");
        mpg.setPackageInfo(pc);
    }

    static {
        // 设置template为项目配置的template
        TemplateConfig tc = new TemplateConfig();
        tc.setXml(null);
        tc.setEntity(COVER_TEMPLATE_PATH + "/entity.java");
        tc.setService(COVER_TEMPLATE_PATH + "/service.java");
        tc.setServiceImpl(COVER_TEMPLATE_PATH + "/serviceImpl.java");
        tc.setController(COVER_TEMPLATE_PATH + "/controller.java");

        mpg.setTemplate(tc);
        mpg.setTemplateEngine(new FreemarkerTemplateEngine());
        InjectionConfig cfg = new InjectionConfig() {
            @Override
            public void initMap() {
                // 配置controller通用类map
                Map<String, Object> map = new HashMap<>();
                map.put("logAnnotationClass", Log.class.getName());
                map.put("queryRequestClass", QueryRequest.class.getName());
                map.put("resultClass", Result.class.getName());
                map.put("pageResultClass", PageResult.class.getName());
                map.put("jsonUtilClass", JsonUtil.class.getName());
                map.put("mapperUtilClass", MapperUtil.class.getName());
                map.put("requestPackage", MODULE_NAME + ".domain.request");
                map.put("responsePackage", MODULE_NAME + ".domain.response");
                this.setMap(map);
            }
        };

        String mapperXmlTemplatePath = COVER_TEMPLATE_PATH + "/mapper.xml.ftl";
        List<FileOutConfig> focList = new ArrayList<>();
        focList.add(new FileOutConfig(mapperXmlTemplatePath) {
            @Override
            public String outputFile(TableInfo tableInfo) {
                return System.getProperty("user.dir") + "/src/main/resources/mapper" + "/" + tableInfo.getEntityName()
                    + "Mapper" + StringPool.DOT_XML;
            }
        });

        String requestTemplatePath = COVER_TEMPLATE_PATH + "/request.java.ftl";
        focList.add(new FileOutConfig(requestTemplatePath) {
            @Override
            public String outputFile(TableInfo tableInfo) {
                String packageValue = MODULE_NAME.replace(".", "/");
                return System.getProperty("user.dir") + "/src/main/java/" + packageValue + "/domain/request/"
                    + tableInfo.getEntityName() + "Request" + StringPool.DOT_JAVA;
            }
        });

        String responseTemplatePath = COVER_TEMPLATE_PATH + "/response.java.ftl";
        focList.add(new FileOutConfig(responseTemplatePath) {
            @Override
            public String outputFile(TableInfo tableInfo) {
                String packageValue = MODULE_NAME.replace(".", "/");
                return System.getProperty("user.dir") + "/src/main/java/" + packageValue + "/domain/response/"
                    + tableInfo.getEntityName() + "Response" + StringPool.DOT_JAVA;
            }
        });

        cfg.setFileOutConfigList(focList);
        mpg.setCfg(cfg);
    }

    public static void main(String[] args) {
        mpg.execute();
    }

}