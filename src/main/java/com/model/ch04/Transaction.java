package com.model.ch04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by ycwu on 2018/6/13.
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Transaction implements Serializable {
    private Date date;
    private String time;
    private int customerId;
    private int productId;
    private int quantity;
    private double total;
}
