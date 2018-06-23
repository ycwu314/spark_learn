package com.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Administrator on 2018/6/23.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PV implements Serializable {

    private String site;
    private Date date;
    private int user_count;
}
