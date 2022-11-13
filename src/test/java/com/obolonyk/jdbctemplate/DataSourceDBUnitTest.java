package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import org.dbunit.Assertion;
import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.sql.DataSource;
import java.io.InputStream;
import java.time.LocalDateTime;

import static com.obolonyk.jdbctemplate.ConnectionSettings.*;
import static org.dbunit.Assertion.assertEqualsIgnoreCols;

@RunWith(JUnit4.class)
public class DataSourceDBUnitTest extends DataSourceBasedDBTestCase {
    private static final String SAVE = "INSERT INTO products (name, price, creation_date, description) VALUES (?, ?, ?, ?);";
    private DataSource dataSource;

    @Override
    protected DataSource getDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(JDBC_URL);
        dataSource.setUser(USER);
        dataSource.setPassword(PASSWORD);
        return dataSource;
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("dbunit/products.xml")) {
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
    public void setUp() throws Exception {
        super.setUp();
        dataSource = getDataSource();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void givenDataSet_whenInsert_thenTableHasNewClient() throws Exception {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("dbunit/expected-product.xml")) {
            IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(is);
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
    }

    @Test
    public void givenDataSet_whenInsert_thenTableIncludeTheSameColumns() throws Exception {
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

        InputStream is = getClass().getClassLoader().getResourceAsStream("dbunit/expected-product.xml");
        IDataSet expectedDataSet = new FlatXmlDataSetBuilder().build(is);
        ITable expectedTable = expectedDataSet.getTable("PRODUCTS");

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


}
