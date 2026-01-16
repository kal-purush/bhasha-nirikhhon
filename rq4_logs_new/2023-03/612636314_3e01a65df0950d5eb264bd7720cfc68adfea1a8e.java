/**
 * Copyright (C), 2015-2023, XXX有限公司
 * FileName: UserApplication
 * Author:   FanLian
 * Date:     2023/3/11 23:16
 * Description: User应用启动类
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.spark.user;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 〈一句话功能简述〉<br> 
 * 〈User应用启动类〉
 *
 * @author spark
 * @create 2023/3/11
 * @since 1.0.0
 */
@SpringBootApplication
public class UserApplication {
    public static void main(String[] args) {
        SpringApplication.run(UserApplication.class);
    }
}