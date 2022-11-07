package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import com.obolonyk.rowmapper.ProductRowMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class JDBCTemplateITest {
    private static final String SELECT_ALL = "SELECT id, name, price, creation_date, description FROM products;";
    private static final String SELECT_BY_ID = "SELECT id, name, price, creation_date, description FROM products WHERE id = ?;";
    private static final String SAVE = "INSERT INTO products (name, price, creation_date, description) VALUES (?, ?, ?, ?);";
    private static final String DELETE = "DELETE FROM Products WHERE id = ?;";
    private static final String UPDATE = "UPDATE products SET name = ?, price = ?, description = ? where id = ?;";
    private static final String SEARCH = "SELECT id, name, price, creation_date, description FROM products WHERE name ilike ? OR description ilike ?;";

    private DataSource dataSource;
    private final RowMapper<Product> rowMapper = new ProductRowMapper();
    private BufferedReader bufferedReader;
    private Connection connection;

    @BeforeEach
    void init() throws SQLException {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUsername("sa");
        basicDataSource.setPassword("");
        basicDataSource.setUrl("jdbc:h2:mem:test");
        dataSource = basicDataSource;
        connection = dataSource.getConnection();
        bufferedReader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(
                        JDBCTemplateITest.class.getClassLoader().getResourceAsStream("db.sql"))));
        RunScript.execute(connection, bufferedReader);
    }

    @AfterEach
    void after() throws SQLException {
        bufferedReader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(
                        JDBCTemplateITest.class.getClassLoader().getResourceAsStream("drop.sql"))));
        RunScript.execute(connection, bufferedReader);
    }

    @Test
    void testQuery() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        List<Product> list = jdbcTemplate.query(SELECT_ALL, rowMapper);
        assertFalse(list.isEmpty());
        assertEquals(4, list.size());
    }

    @Test
    void testQueryObject() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        Optional<Product> optional = jdbcTemplate.queryObject(SELECT_BY_ID, rowMapper, 1);
        assertFalse(optional.isEmpty());
        assertEquals("A", optional.get().getName());
        assertEquals("AA", optional.get().getDescription());
    }

    @Test
    void testExecuteUpdateSave() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        Product product = Product.builder()
                .creationDate(LocalDateTime.now())
                .name("POP")
                .price(99.0)
                .description("LOW")
                .build();

        int i = jdbcTemplate.executeUpdate(SAVE,
                product.getName(),
                product.getPrice(),
                product.getCreationDate(),
                product.getDescription());
    }

    @Test
    void testGetBySearchWhenPatternValid() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        List<Product> list = jdbcTemplate.query(SEARCH, rowMapper, "A", "A");
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        Product product = list.get(0);
        assertEquals("A", product.getName());
        assertEquals("AA", product.getDescription());
    }

    @Test
    void testGetBySearchWhenPatternInValid() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        List<Product> list = jdbcTemplate.query(SEARCH, rowMapper, "NN", "MM");
        assertTrue(list.isEmpty());
    }

    @Test
    void testExecuteUpdateRemove() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        int i = jdbcTemplate.executeUpdate(DELETE, 1);
        assertEquals(1, i);
    }

    @Test
    void testExecuteUpdateUpdate() throws SQLException {
        JDBCTemplate<Product> jdbcTemplate = new JDBCTemplate<>(dataSource);
        int i = jdbcTemplate.executeUpdate(UPDATE, "POP", 99.0, "LOW", 1);
        assertEquals(1, i);
    }
}