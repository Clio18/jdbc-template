package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import com.obolonyk.rowmapper.ProductRowMapper;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;

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

    Product product = Product.builder()
            .creationDate(LocalDateTime.now())
            .name("POP")
            .price(99.0)
            .description("LOW")
            .id(1)
            .build();

    @Test
    void testQuery() throws SQLException {
        String SELECT_ALL = "SELECT id, name, price, creation_date, description FROM products;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(SELECT_ALL)).thenReturn(resultSet);

       jdbcTemplate.query(SELECT_ALL, rowMapper);

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

       jdbcTemplate.queryObject(SELECT_BY_ID, rowMapper, 1L);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(SELECT_BY_ID);
        verify(preparedStatement, times(1)).setObject(1, 1L);
        verify(preparedStatement, times(1)).executeQuery();
        verify(resultSet, times(1)).next();

        verify(resultSet, times(1)).close();
        verify(preparedStatement, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test
    void testExecuteUpdateSave() throws SQLException {
        String SAVE = "INSERT INTO products (name, price, creation_date, description) VALUES (?, ?, ?, ?);";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SAVE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.executeUpdate(SAVE,
                product.getName(),
                product.getPrice(),
                product.getCreationDate(),
                product.getDescription());

        assertEquals(1, i);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(SAVE);

        verify(preparedStatement, times(1)).setObject(1, product.getName());
        verify(preparedStatement, times(1)).setObject(2, product.getPrice());
        verify(preparedStatement, times(1)).setObject(3, product.getCreationDate());
        verify(preparedStatement, times(1)).setObject(4, product.getDescription());

        verify(preparedStatement, times(1)).executeUpdate();

        verify(preparedStatement, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test
    void testGetBySearch() throws SQLException {
        String SEARCH = "SELECT id, name, price, creation_date, description FROM products WHERE name ilike %?% OR description ilike %?%;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SEARCH)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        String [] args = new String[]{"A", "A"};

        jdbcTemplate.queryListObject(SEARCH, rowMapper, args);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(SEARCH);

        verify(preparedStatement, times(1)).setObject(1, "A");
        verify(preparedStatement, times(1)).setObject(2, "A");

        verify(preparedStatement, times(1)).executeQuery();
        verify(resultSet, times(1)).next();

        verify(resultSet, times(1)).close();
        verify(preparedStatement, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test
    void testExecuteUpdateRemove() throws SQLException {
       String DELETE = "DELETE FROM Products WHERE id = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(DELETE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.executeUpdate(DELETE, 1);
        assertEquals(1, i);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(DELETE);

        verify(preparedStatement, times(1)).setObject(1, 1);

        verify(preparedStatement, times(1)).executeUpdate();
        verify(preparedStatement, times(1)).close();
        verify(connection, times(1)).close();
    }

    @Test
    void testExecuteUpdateUpdate() throws SQLException {
        String UPDATE = "UPDATE products SET name = ?, price = ?, description = ? where id = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPDATE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.executeUpdate(UPDATE, product.getName(),
                product.getPrice(),
                product.getDescription(),
                product.getId());

        assertEquals(1, i);

        verify(dataSource, times(1)).getConnection();
        verify(connection, times(1)).prepareStatement(UPDATE);

        verify(preparedStatement, times(1)).setObject(1, product.getName());
        verify(preparedStatement, times(1)).setObject(2, product.getPrice());
        verify(preparedStatement, times(1)).setObject(3, product.getDescription());
        verify(preparedStatement, times(1)).setObject(4, product.getId());

        verify(preparedStatement, times(1)).executeUpdate();
        verify(preparedStatement, times(1)).close();
        verify(connection, times(1)).close();
    }
}
