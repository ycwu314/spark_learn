package com.example.secondarysort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by ycwu on 2018/6/20.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerPriceKey implements Serializable, Comparable<CustomerPriceKey> {

    private Integer customerId;
    private Double total;

    @Override
    public int compareTo(CustomerPriceKey other) {
        if (this.customerId.intValue() != other.customerId.intValue()) {
            return this.customerId - other.customerId;
        }
        return Double.compare(this.total, other.total);
    }
}
