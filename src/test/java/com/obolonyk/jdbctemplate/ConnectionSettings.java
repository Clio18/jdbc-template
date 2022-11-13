package com.obolonyk.jdbctemplate;

public class ConnectionSettings {
    public static final String JDBC_URL = "jdbc:h2:mem:sample;INIT=RUNSCRIPT FROM 'classpath:dbunit/schema.sql'";
    public static final String USER = "sa";
    public static final String PASSWORD = "";
}
