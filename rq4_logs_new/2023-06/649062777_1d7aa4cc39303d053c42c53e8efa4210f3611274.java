package cn.techyang.springboot.controller.vo;

import lombok.Data;

import javax.annotation.sql.DataSourceDefinition;

/**
 * @author tecyang@gmail.com
 * @version V1.0
 * @title
 * @description
 * @date 2023-06-10 18:33
 */

@Data
public class HelloRequestVo {
    String userName;
    String age;
}