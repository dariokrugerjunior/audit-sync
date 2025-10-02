package com.example.audit_sync.config;

import com.example.audit_sync.utils.JdbcUrlConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Value("${DB_URL_ADMIN}")
    private String dbUrlAdmin;

    @Bean
    public DataSource dataSource() {
        String jdbcUrl = JdbcUrlConverter.convertToJdbc(dbUrlAdmin);
        return DataSourceBuilder.create()
                .url(jdbcUrl)
                .driverClassName("org.postgresql.Driver")
                .build();
    }
}