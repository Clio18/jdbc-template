package com.obolonyk.entity;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class Product {
    private long id;
    private String name;
    private double price;
    private LocalDateTime creationDate;
    private String description;

}
