package com.obolonyk.jdbctemplate;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static com.obolonyk.jdbctemplate.ReflectionHelper.getSetterNameAndClassName;
import static com.obolonyk.jdbctemplate.ReflectionHelper.initPreparedStatement;

@Slf4j
public class JDBCTemplate<T> {


    private DataSource dataSource;

    public JDBCTemplate(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<T> query(String sql, RowMapper<T> rowMapper) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            log.trace("Connection was established, running sql query {}", sql);

            List<T> list = new ArrayList<>();
            while (resultSet.next()) {
                list.add(rowMapper.mapRow(resultSet));
            }
            log.trace("Result list has {} elements", list.size());
            return list;
        }
    }

    public Optional<T> queryObject(String sql, RowMapper<T> rowMapper, Object... args) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.trace("Connection was established, running sql query {}", sql);
            for (int i = 0; i < args.length; i++) {
                int preparedStatementArgIndex = i+1;
                Map<String, Object> data = getSetterNameAndClassName(args[i]);
                initPreparedStatement(preparedStatement, data, preparedStatementArgIndex);
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                T object = rowMapper.mapRow(resultSet);
                return Optional.of(object);
            }
        }
    }

    public int executeUpdate(String sql, Object... args) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.trace("Connection was established, running sql query {}", sql);

            for (int i = 0; i < args.length; i++) {
                int preparedStatementArgIndex = i+1;
                Map<String, Object> data = getSetterNameAndClassName(args[i]);
                initPreparedStatement(preparedStatement, data, preparedStatementArgIndex);
                log.trace("Query index: {}, parameter: {}", i + 1, args[i]);
            }
            return preparedStatement.executeUpdate();
        }
    }

    public List<T> query(String sql, RowMapper<T> rowMapper, Object... args) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.trace("Connection was established, running sql query {}", sql);

            for (int i = 0; i < args.length; i++) {
                int preparedStatementArgIndex = i+1;
                Map<String, Object> data = getSetterNameAndClassName(args[i]);
                initPreparedStatement(preparedStatement, data, preparedStatementArgIndex);
                log.trace("Query index: {}, parameter: {}", i + 1, args[i]);
            }

            List<T> list = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    list.add(rowMapper.mapRow(resultSet));
                }
            }
            log.trace("Result list has {} elements", list.size());
            return list;
        }
    }




}
