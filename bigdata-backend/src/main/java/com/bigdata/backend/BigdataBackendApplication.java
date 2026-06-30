package com.bigdata.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 用户行为分析系统后端启动入口，负责加载 Spring Boot Web、Doris 查询与缓存能力。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@SpringBootApplication
public class BigdataBackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BigdataBackendApplication.class, args);
    }
}
