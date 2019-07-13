package com.zlb.kafka;

import java.util.concurrent.atomic.AtomicLong;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by Onizuka on 2019/7/13
 */

public class RoundRobinPartitioner implements Partitioner {

    private static AtomicLong next = new AtomicLong();

    public RoundRobinPartitioner(VerifiableProperties verifiableProperties) {
    }

    @Override
    public int partition(Object key, int numPartitions) {
        long nextIndex = next.incrementAndGet();
        return (int) nextIndex % numPartitions;
    }
}
