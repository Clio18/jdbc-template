package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import com.obolonyk.rowmapper.ProductRowMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.dbunit.Assertion;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.sql.DataSource;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static com.obolonyk.jdbctemplate.ConnectionSettings.*;
import static org.dbunit.Assertion.assertEqualsIgnoreCols;

@RunWith(JUnit4.class)
public class DataSourceDBUnitTest extends DataSourceBasedDBTestCase {
    private static final String SAVE = "INSERT INTO products (name, price, creation_date, description) VALUES (?, ?, ?, ?);";
    private static final String SELECT_ALL = "SELECT id, name, price, creation_date, description FROM products;";
    private static final String SELECT_BY_ID = "SELECT id, name, price, creation_date, description FROM products WHERE id = ?;";
    private static final String DELETE = "DELETE FROM products WHERE id = ?;";
    private static final String UPDATE = "UPDATE products SET name = ?, price = ?, description = ? where id = ?;";
    private static final String SEARCH = "SELECT id, name, price, creation_date, description FROM products WHERE name ilike ? OR description ilike ?;";
    private DataSource dataSource;
    private final ProductRowMapper productRowMapper = new ProductRowMapper();


    public DataSourceDBUnitTest() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(JDBC_URL);
        dataSource.setUsername(USER);
        dataSource.setPassword(PASSWORD);
        this.dataSource = dataSource;
    }

    @Override
    protected DataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("dbunit/products.xml")) {
            return new FlatXmlDataSetBuilder().build(resourceAsStream);
        }
    }

    private IDataSet getDataSet(String pathToResources) throws Exception {
        try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(pathToResources)) {
            return new FlatXmlDataSetBuilder().build(resourceAsStream);
        }
    }

    @Override
    protected DatabaseOperation getSetUpOperation() {
        return DatabaseOperation.REFRESH;
    }

    @Override
    protected DatabaseOperation getTearDownOperation() {
        return DatabaseOperation.DELETE_ALL;
    }

    @Before
    public void before() throws Exception {
        super.setUp();
        dataSource = getDataSource();
    }

    @After
    public void after() throws Exception {
        super.tearDown();
    }

    @Test
    public void givenDataSet_whenInsert_thenTableHasNewProduct() throws Exception {
        IDataSet expectedDataSet = getDataSet("dbunit/expected-product.xml");
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        Product product = Product.builder()
                .creationDate(LocalDateTime.now())
                .name("POP")
                .price(99.0)
                .description("LOW")
                .build();

        jdbcTemplate.update(SAVE,
                product.getName(),
                product.getPrice(),
                product.getCreationDate(),
                product.getDescription());

        ITable actualData = getConnection()
                .createQueryTable(
                        "result_name",
                        "SELECT * FROM PRODUCTS WHERE name= 'POP'");

        assertEqualsIgnoreCols(expectedTable, actualData, new String[]{"id", "creation_date"});

    }

    @Test
    public void givenDataSet_whenInsert_thenTableIncludeTheSameColumns() throws Exception {
        IDataSet expectedDataSet = getDataSet("dbunit/expected-product.xml");
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        Product product = Product.builder()
                .creationDate(LocalDateTime.now())
                .name("POP")
                .price(99.0)
                .description("LOW")
                .build();

        jdbcTemplate.update(SAVE,
                product.getName(),
                product.getPrice(),
                product.getCreationDate(),
                product.getDescription());

        IDataSet databaseDataSet = getConnection().createDataSet();
        ITable actualTable = databaseDataSet.getTable("PRODUCTS");

        ITable filteredActualTable = DefaultColumnFilter.includedColumnsTable(actualTable, expectedTable.getTableMetaData().getColumns());
        Assertion.assertEquals(actualTable, filteredActualTable);
    }

    @Test
    public void givenDataSetEmptySchema_whenDataSetCreated_thenTablesAreEqual() throws Exception {
        IDataSet expectedDataSet = getDataSet();
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        IDataSet databaseDataSet = getConnection().createDataSet();
        ITable actualTable = databaseDataSet.getTable("PRODUCTS");

        Assertion.assertEquals(expectedTable, actualTable);
    }

    @Test
    public void testSelect_AllQueryAndCheckResultNotEmptyAndListSize() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> productList = jdbcTemplate.query(SELECT_ALL, productRowMapper);

        assertFalse(productList.isEmpty());
        assertEquals(5, productList.size());
    }

    @Test
    public void givenData_whenDataSetCreatedBySelectAllQuery_thenTablesAreEqual() throws Exception {
        IDataSet expectedDataSet = getDataSet();
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        ITable actualData = getConnection()
                .createQueryTable(
                        "result_name",
                        SELECT_ALL);

        assertEqualsIgnoreCols(expectedTable, actualData, new String[]{});
    }

    @Test
    public void givenData_getRowCountAndResultListSizeBySelectAllQueryAreEquals() throws Exception {
        IDataSet expectedDataSet = getDataSet();
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> productList = jdbcTemplate.query(SELECT_ALL, productRowMapper);

        assertEquals(expectedTable.getRowCount(), productList.size());
    }

    @Test
    public void testQueryObjectAndCheckResult() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        Optional<Product> optional = jdbcTemplate.queryObject(SELECT_BY_ID, productRowMapper, 11);
        assertFalse(optional.isEmpty());
        assertEquals("A", optional.get().getName());
        assertEquals("AA", optional.get().getDescription());
    }

    @Test
    public void testExecuteUpdateSaveAndCheckResult() {
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
    public void testGetBySearchWhenPatternValidAndCheckResult() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> list = jdbcTemplate.query(SEARCH, productRowMapper, "C", "CC");
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        Product product = list.get(0);
        assertEquals("C", product.getName());
        assertEquals("CC", product.getDescription());
    }

    @Test
    public void testGetBySearchWhenPatternInValidAndCheckListIsEmpty() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> list = jdbcTemplate.query(SEARCH, productRowMapper, "NN", "MM");
        assertTrue(list.isEmpty());
    }

    @Test
    public void testExecuteUpdateRemoveAndCheckResult() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        int i = jdbcTemplate.update(DELETE, 11);
        assertEquals(1, i);
    }

    @Test
    public void givenDataSet_whenRemove_thenCheckTablesRowCount() throws Exception {
        IDataSet expectedDataSet = getDataSet();
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        jdbcTemplate.update(DELETE, 11L);

        IDataSet databaseDataSet = getConnection().createDataSet();
        ITable actualTable = databaseDataSet.getTable("PRODUCTS");

        assertEquals(expectedTable.getRowCount() - 1, actualTable.getRowCount());
    }

    @Test
    public void testExecuteUpdateUpdateAndCheckResult() {
        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        int i = jdbcTemplate.update(UPDATE, "POP", 99.0, "LOW", 14);
        assertEquals(1, i);
    }

    @Test
    public void givenDataSet_whenSearched_thenTableHasTheSameRowCountAsResultList() throws Exception {
        IDataSet expectedDataSet = getDataSet("dbunit/expected-searched-product.xml");
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

        JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
        List<Product> productList = jdbcTemplate.query(SEARCH, productRowMapper, "C", "CC");

        assertEquals(expectedTable.getRowCount(), productList.size());
    }
}
