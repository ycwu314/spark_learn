package com.model.ch04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * Created by ycwu on 2018/6/13.
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Product implements Serializable {
    private int id;
    private String name;
    private double total;
    private int amount;
}
