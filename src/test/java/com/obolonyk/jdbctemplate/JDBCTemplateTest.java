package com.obolonyk.jdbctemplate;

import com.obolonyk.entity.Product;
import com.obolonyk.entity.Role;
import com.obolonyk.entity.User;
import com.obolonyk.rowmapper.ProductRowMapper;
import com.obolonyk.rowmapper.UserRowMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class JDBCTemplateTest {
    private final PreparedStatement preparedStatement = mock(PreparedStatement.class);
    private final DataSource dataSource = mock(DataSource.class);
    private final Connection connection = mock(Connection.class);
    private final ResultSet resultSet = mock(ResultSet.class);

    private final RowMapper<Product> productRowMapper = new ProductRowMapper();
    private final JDBCTemplate jdbcTemplate = new JDBCTemplate(dataSource);
    private final RowMapper<User> userRowMapper = new UserRowMapper();
    private final LocalDateTime time = LocalDateTime.now();

    private final Product product = Product.builder()
            .creationDate(LocalDateTime.now())
            .name("POP")
            .price(99.0)
            .description("LOW")
            .creationDate(time)
            .id(1L)
            .build();
    private final User user = User.builder()
            .id(1L)
            .name("Ki")
            .lastName("Ne")
            .email("ki18@mail.com")
            .login("kim18")
            .password("kim18")
            .salt("cat")
            .role(Role.USER)
            .build();

    @Test
    @SneakyThrows
    void testQuery() {
        String SELECT_ALL = "SELECT id, name, price, creation_date, description FROM products;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_ALL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next())
                .thenReturn(true)
                .thenReturn(false);

        when(resultSet.getLong("id")).thenReturn(1L);
        when(resultSet.getString("name")).thenReturn("POP");
        when(resultSet.getDouble("price")).thenReturn(99.0);
        when(resultSet.getObject("creation_date", LocalDateTime.class)).thenReturn(time);
        when(resultSet.getString("description")).thenReturn("LOW");

        List<Product> products = jdbcTemplate.query(SELECT_ALL, productRowMapper);
        assertFalse(products.isEmpty());
        assertEquals(1, products.size());
        assertEquals(product, products.get(0));

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(SELECT_ALL);
        verify(preparedStatement).executeQuery();
        verify(resultSet, times(2)).next();

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @SneakyThrows
    void testQueryObject() {
        String SELECT_BY_ID = "SELECT id, name, price, creation_date, description FROM products WHERE id = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_BY_ID)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next())
                .thenReturn(true)
                .thenReturn(false);

        when(resultSet.getLong("id")).thenReturn(product.getId());
        when(resultSet.getString("name")).thenReturn(product.getName());
        when(resultSet.getDouble("price")).thenReturn(product.getPrice());
        when(resultSet.getObject("creation_date", LocalDateTime.class)).thenReturn(time);
        when(resultSet.getString("description")).thenReturn(product.getDescription());

        Optional<Product> optionalProduct = jdbcTemplate.queryObject(SELECT_BY_ID, productRowMapper, 1L);
        assertFalse(optionalProduct.isEmpty());
        assertEquals(product, optionalProduct.get());

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(SELECT_BY_ID);

        verify(preparedStatement).setLong(1, 1L);

        verify(preparedStatement).executeQuery();
        verify(resultSet, times(1)).next();

        verify(resultSet).close();
        verify(preparedStatement, times(1)).close();
        verify(connection).close();
    }

    @Test
    @SneakyThrows
    void testExecuteUpdateSave() {
        String SAVE = "INSERT INTO products (name, price, creation_date, description) VALUES (?, ?, ?, ?);";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SAVE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.update(SAVE,
                product.getName(),
                product.getPrice(),
                product.getCreationDate(),
                product.getDescription());

        assertEquals(1, i);

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(SAVE);

        verify(preparedStatement).setString(1, product.getName());
        verify(preparedStatement).setDouble(2, product.getPrice());
        verify(preparedStatement).setTimestamp(3, Timestamp.valueOf(product.getCreationDate()));
        verify(preparedStatement).setString(4, product.getDescription());

        verify(preparedStatement).executeUpdate();

        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @SneakyThrows
    void testGetBySearch() {
        String SEARCH = "SELECT id, name, price, creation_date, description FROM products WHERE name ilike %?% OR description ilike %?%;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SEARCH)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        String[] args = new String[]{"O", "O"};

        when(resultSet.next())
                .thenReturn(true)
                .thenReturn(false);

        when(resultSet.getLong("id")).thenReturn(product.getId());
        when(resultSet.getString("name")).thenReturn(product.getName());
        when(resultSet.getDouble("price")).thenReturn(product.getPrice());

        when(resultSet.getObject("creation_date", LocalDateTime.class)).thenReturn(time);
        when(resultSet.getString("description")).thenReturn("LOW");

        List<Product> products = jdbcTemplate.query(SEARCH, productRowMapper, args);

        assertFalse(products.isEmpty());
        assertEquals(1, products.size());
        assertEquals(product, products.get(0));

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(SEARCH);

        verify(preparedStatement).setString(1, "O");
        verify(preparedStatement).setString(2, "O");

        verify(preparedStatement).executeQuery();
        verify(resultSet, times(2)).next();

        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @SneakyThrows
    void testExecuteUpdateRemove() {
        String DELETE = "DELETE FROM Products WHERE id = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(DELETE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.update(DELETE, 1L);
        assertEquals(1, i);

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(DELETE);

        verify(preparedStatement).setLong(1, 1L);

        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    @SneakyThrows
    void testExecuteUpdateUpdate() {
        String UPDATE = "UPDATE products SET name = ?, price = ?, description = ? where id = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPDATE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.update(UPDATE,
                product.getName(),
                product.getPrice(),
                product.getDescription(),
                product.getId());

        assertEquals(1, i);

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(UPDATE);

        verify(preparedStatement).setString(1, product.getName());
        verify(preparedStatement).setDouble(2, product.getPrice());
        verify(preparedStatement).setString(3, product.getDescription());
        verify(preparedStatement).setLong(4, product.getId());

        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).close();
        verify(connection).close();
    }


    @Test
    @SneakyThrows
    void testQueryObjectUser() {
        String SELECT_BY_LOGIN = "SELECT id, name, last_name, login, email, password, salt, role FROM users WHERE login = ?;";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_BY_LOGIN)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next())
                .thenReturn(true)
                .thenReturn(false);

        when(resultSet.getLong("id")).thenReturn(user.getId());
        when(resultSet.getString("name")).thenReturn(user.getName());
        when(resultSet.getString("last_name")).thenReturn(user.getLastName());
        when(resultSet.getString("login")).thenReturn(user.getLogin());
        when(resultSet.getString("email")).thenReturn(user.getEmail());
        when(resultSet.getString("password")).thenReturn(user.getPassword());
        when(resultSet.getString("salt")).thenReturn(user.getSalt());
        when(resultSet.getString("role")).thenReturn(user.getRole().getUserRole());


        Optional<User> optionalUser = jdbcTemplate.queryObject(SELECT_BY_LOGIN, userRowMapper, user.getLogin());
        assertFalse(optionalUser.isEmpty());
        assertEquals(user, optionalUser.get());

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(SELECT_BY_LOGIN);
        verify(preparedStatement).setString(1, user.getLogin());
        verify(preparedStatement).executeQuery();
        verify(resultSet, times(1)).next();

        verify(resultSet).close();
        verify(preparedStatement, times(1)).close();
        verify(connection).close();
    }

    @Test
    @SneakyThrows
    void testExecuteUpdateSaveUser() {
        String SAVE = "INSERT INTO users (name, last_name, login, email, password, salt, role) VALUES (?, ?, ?, ?, ?, ?, 'USER');";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SAVE)).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        int i = jdbcTemplate.update(SAVE,
                user.getName(),
                user.getLastName(),
                user.getLogin(),
                user.getEmail(),
                user.getPassword(),
                user.getSalt());

        assertEquals(1, i);

        verify(dataSource).getConnection();
        verify(connection).prepareStatement(SAVE);

        verify(preparedStatement).setString(1, user.getName());
        verify(preparedStatement).setString(2, user.getLastName());
        verify(preparedStatement).setString(3, user.getLogin());
        verify(preparedStatement).setString(4, user.getEmail());
        verify(preparedStatement).setString(5, user.getPassword());
        verify(preparedStatement).setString(6, user.getSalt());

        verify(preparedStatement).executeUpdate();

        verify(preparedStatement).close();
        verify(connection).close();
    }

}
