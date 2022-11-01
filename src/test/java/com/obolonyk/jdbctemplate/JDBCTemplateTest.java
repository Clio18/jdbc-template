package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import com.obolonyk.rowmapper.ProductRowMapper;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;


class JDBCTemplateTest {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);

    RowMapper<Product> rowMapper = new ProductRowMapper();
    JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);

    @Test
    void testQuery() throws SQLException {

        String SELECT_ALL = "SELECT id, name, price, creation_date, description FROM products;";
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(SELECT_ALL)).thenReturn(resultSet);

        List<Product> products = jdbcTemplate.query(SELECT_ALL, rowMapper);

        assertNotNull(products);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).createStatement();
        verify(statement, times(1)).executeQuery(SELECT_ALL);
        verify(resultSet, times(1)).next();
        verify(resultSet, times(1)).close();
        verify(statement, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test
    void testQueryObject() throws SQLException {
        String SELECT_BY_ID = "SELECT id, name, price, creation_date, description FROM products WHERE id = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_BY_ID)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        Optional<Product> optional = jdbcTemplate.queryObject(SELECT_BY_ID, rowMapper, 1L);
        assertNotNull(optional);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(SELECT_BY_ID);
        verify(preparedStatement, times(1)).setObject(1, 1L);
        verify(preparedStatement, times(1)).executeQuery();
        verify(resultSet, times(1)).next();
        verify(resultSet, times(1)).close();
        verify(preparedStatement, times(1)).close();
        verify(connection, times(1)).close();
    }
}
