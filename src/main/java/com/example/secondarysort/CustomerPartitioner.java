package com.example.secondarysort;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.spark.Partitioner;

/**
 * Created by ycwu on 2018/6/20.
 */
@AllArgsConstructor
@NoArgsConstructor
public class CustomerPartitioner extends Partitioner {

    private int numPartitions;

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * hash by customer id, instead of the hashcode of the entire CustomerPriceKey object
     * @param key
     * @return
     */
    @Override
    public int getPartition(Object key) {
        CustomerPriceKey _key = (CustomerPriceKey) key;
        return _key.getCustomerId() % numPartitions;
    }
}
