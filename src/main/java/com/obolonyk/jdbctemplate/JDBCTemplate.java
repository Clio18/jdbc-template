package com.obolonyk.jdbctemplate;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static com.obolonyk.jdbctemplate.ReflectionHelper.*;

@Slf4j
public class JDBCTemplate  {

    private DataSource dataSource;

    public JDBCTemplate(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @SneakyThrows
    public <T>List<T> query(String sql, RowMapper<T> rowMapper) {
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

    @SneakyThrows
    public <T>Optional<T> queryObject(String sql, RowMapper<T> rowMapper, Object... args) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.trace("Connection was established, running sql query {}", sql);
            for (int i = 0; i < args.length; i++) {
                int preparedStatementArgIndex = i+1;
                Map<String, Object> setterNameAndObjectValueMap = getSetterNameAndObjectValue(args[i]);
                initPreparedStatement(preparedStatement, setterNameAndObjectValueMap, preparedStatementArgIndex);
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

    @SneakyThrows
    public int update(String sql, Object... args) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.trace("Connection was established, running sql query {}", sql);

            for (int i = 0; i < args.length; i++) {
                int preparedStatementArgIndex = i+1;
                Map<String, Object> setterNameAndObjectValueMap = getSetterNameAndObjectValue(args[i]);
                initPreparedStatement(preparedStatement, setterNameAndObjectValueMap, preparedStatementArgIndex);
                log.trace("Query index: {}, parameter: {}", i + 1, args[i]);
            }
            return preparedStatement.executeUpdate();
        }
    }

    @SneakyThrows
    public <T>List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            log.trace("Connection was established, running sql query {}", sql);

            for (int i = 0; i < args.length; i++) {
                int preparedStatementArgIndex = i+1;
                Map<String, Object> setterNameAndObjectValueMap = getSetterNameAndObjectValue(args[i]);
                initPreparedStatement(preparedStatement, setterNameAndObjectValueMap, preparedStatementArgIndex);
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
