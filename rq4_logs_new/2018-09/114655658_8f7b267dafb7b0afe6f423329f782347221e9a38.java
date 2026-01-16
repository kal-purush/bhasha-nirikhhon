package com.xuebusi.xssm.common.generator.mac;

import org.mybatis.generator.api.MyBatisGenerator;
import org.mybatis.generator.config.Configuration;
import org.mybatis.generator.config.xml.ConfigurationParser;
import org.mybatis.generator.internal.DefaultShellCallback;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * mybatis逆向工程
 * 根据配置生成mybatis的实体类、接口以及映射Mapper.xml文件
 *
 * Created by 学布斯 on 2017/12/18.
 */
public class GeneratorMain {

    public void generator() throws Exception {

        List<String> warnings = new ArrayList<>();
        boolean overwrite = true;
        //指定 逆向工程配置文件
        File configFile = new File("/Users/v_shiyanjun/IdeaProjects/xssm/src/main/java/com/xuebusi/xssm/common/generator/generatorConfig.xml");
        ConfigurationParser cp = new ConfigurationParser(warnings);
        Configuration config = cp.parseConfiguration(configFile);
        DefaultShellCallback callback = new DefaultShellCallback(overwrite);
        MyBatisGenerator myBatisGenerator = new MyBatisGenerator(config, callback, warnings);
        myBatisGenerator.generate(null);

    }

    public static void main(String[] args) throws Exception {
        try {
            GeneratorMain generatorSqlmap = new GeneratorMain();
            generatorSqlmap.generator();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}