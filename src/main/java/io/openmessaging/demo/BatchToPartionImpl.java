package io.openmessaging.demo;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;

/**
 * Created by mst on 2017/5/14.
 */
public class BatchToPartionImpl implements BatchToPartition {
    @Override
    public void send(BytesMessage message) {

    }

    @Override
    public void send(BytesMessage message, KeyValue properties) {

    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }
}
