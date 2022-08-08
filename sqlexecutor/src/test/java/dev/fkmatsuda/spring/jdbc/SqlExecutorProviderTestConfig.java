package dev.fkmatsuda.spring.jdbc;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

@Configuration
public class SqlExecutorProviderTestConfig {

    @Bean(name = "dataSource")
    public DataSource dataSource() {
        return new SimpleDriverDataSource(new org.hsqldb.jdbcDriver(), "jdbc:hsqldb:mem:testdb;sql.syntax_pgs=true", "sa", "");
    }

    @Bean(name = "sqlExecutorProvider")
    public SqlExecutorProvider sqlExecutorProvider(ApplicationContext applicationContext) {
        return new SqlExecutorProvider(applicationContext);
    }

}
