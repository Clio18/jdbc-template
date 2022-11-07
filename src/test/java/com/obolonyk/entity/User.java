package com.obolonyk.entity;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@Builder
@EqualsAndHashCode
public class User {
    private long id;
    private String name;
    private String lastName;
    private String login;
    private String email;
    private String password;
    private String salt;
    private Role role;
}
