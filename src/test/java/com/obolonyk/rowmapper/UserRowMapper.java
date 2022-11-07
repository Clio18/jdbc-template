package com.obolonyk.rowmapper;


import com.obolonyk.entity.Role;
import com.obolonyk.entity.User;
import com.obolonyk.jdbctemplate.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UserRowMapper implements RowMapper<User> {
    public User mapRow(ResultSet resultSet) throws SQLException {
        return User.builder()
                .id(resultSet.getLong("id"))
                .name(resultSet.getString("name"))
                .lastName(resultSet.getString("last_name"))
                .email(resultSet.getString("email"))
                .login(resultSet.getString("login"))
                .password(resultSet.getString("password"))
                .salt(resultSet.getString("salt"))
                .role(Role.valueOf(resultSet.getString("role")))
                .build();
    }
}
