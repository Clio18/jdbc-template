package com.obolonyk.entity;

public enum Role {
    ADMIN("ADMIN"), USER("USER");

    private final String role;

    Role(String userRole) {
        this.role = userRole;
    }

    public String getUserRole() {
        return role;
    }
}
