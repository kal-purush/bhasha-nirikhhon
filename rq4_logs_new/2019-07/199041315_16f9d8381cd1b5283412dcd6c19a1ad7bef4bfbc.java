package com.digitalsoftware.accounting;

import com.digitalsoftware.accounting.configuration.Bootstrap;
import com.digitalsoftware.accounting.mapper.DAO.RoleMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import tk.mybatis.spring.annotation.MapperScan;


@SpringBootApplication
@MapperScan(basePackages = {"com.digitalsoftware.accounting.mapper.DAO"})
@EnableAsync
@EnableCaching
public class AccountingApplication {

    @Autowired
    private Bootstrap bootstrap;

    @Autowired
    private RoleMapper roleMapper;

    @Bean
    public BCryptPasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    public static void main(String[] args) {
        SpringApplication.run(AccountingApplication.class, args);


    }

    ;
//        @Bean
//        @Qualifier("mainDataSource")
//        public DataSource dataSource() {
//                EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
//                EmbeddedDatabase db = builder
//                        .setType(EmbeddedDatabaseType.H2)
//                        .build();
//                return db;
//        }
//


//    @Override
//    public void run(String... strings) throws Exception {
//
//        if (roleDAO.findByRole(RoleType.ROLE_USER) == null) {
//
//            bootstrap.generateAll();
//        }
//
//
//    }
}