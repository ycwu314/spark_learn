package com.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by ycwu on 2018/6/15.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AvgHolder implements Serializable {
    private int num;
    private int total;
}