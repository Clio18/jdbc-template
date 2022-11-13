package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import com.obolonyk.rowmapper.ProductRowMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class JDBCTemplateITest {
    private static final String SELECT_ALL = "SELECT id, name, price, creation_date, description FROM products_flyway;";
    private static final String SELECT_BY_ID = "SELECT id, name, price, creation_date, description FROM products_flyway WHERE id = ?;";
    private static final String SAVE = "INSERT INTO products_flyway (name, price, creation_date, description) VALUES (?, ?, ?, ?);";
    private static final String DELETE = "DELETE FROM products_flyway WHERE id = ?;";
    private static final String UPDATE = "UPDATE products_flyway SET name = ?, price = ?, description = ? where id = ?;";
    private static final String SEARCH = "SELECT id, name, price, creation_date, description FROM products_flyway WHERE name ilike ? OR description ilike ?;";
    private final RowMapper<Product> rowMapper = new ProductRowMapper();
    private final DataSource dataSource;
    private final Flyway flyway;


    public JDBCTemplateITest() {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUsername("sa");
        basicDataSource.setPassword("");
        basicDataSource.setUrl("jdbc:h2:mem:test");
        dataSource = basicDataSource;
        flyway = Flyway.configure().dataSource(dataSource).load();
    }

    @BeforeEach
    void before() {
        flyway.migrate();
    }

    @Test
    void testQuery() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> list = jdbcTemplate.query(SELECT_ALL, rowMapper);
        assertFalse(list.isEmpty());
        assertEquals(4, list.size());
    }

    @Test
    void testQueryObject() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        Optional<Product> optional = jdbcTemplate.queryObject(SELECT_BY_ID, rowMapper, 1);
        assertFalse(optional.isEmpty());
        assertEquals("A", optional.get().getName());
        assertEquals("AA", optional.get().getDescription());
    }

    @Test
    void testExecuteUpdateSave() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        Product product = Product.builder()
                .creationDate(LocalDateTime.now())
                .name("POP")
                .price(99.0)
                .description("LOW")
                .build();

        int i = jdbcTemplate.update(SAVE,
                product.getName(),
                product.getPrice(),
                product.getCreationDate(),
                product.getDescription());

        assertEquals(1, i);
    }

    @Test
    void testGetBySearchWhenPatternValid() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> list = jdbcTemplate.query(SEARCH, rowMapper, "C", "CC");
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        Product product = list.get(0);
        assertEquals("C", product.getName());
        assertEquals("CC", product.getDescription());
    }

    @Test
    void testGetBySearchWhenPatternInValid() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> list = jdbcTemplate.query(SEARCH, rowMapper, "NN", "MM");
        assertTrue(list.isEmpty());
    }

    @Test
    void testExecuteUpdateRemove() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        int i = jdbcTemplate.update(DELETE, 1);
        assertEquals(1, i);
    }

    @Test
    void testExecuteUpdateUpdate() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        int i = jdbcTemplate.update(UPDATE, "POP", 99.0, "LOW", 4);
        assertEquals(1, i);
    }
}