/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.embedded;

import com.ververica.cdc.debezium.internal.DebeziumChangeFetcher;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.Header;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * Copied from Debezium project. Make it public to be accessible from {@link DebeziumChangeFetcher}.
 */
public class EmbeddedEngineChangeEvent<K, V, H> implements ChangeEvent<K, V>, RecordChangeEvent<V> {

    private final K key;
    private final V value;
    private final List<Header<H>> headers;
    private final SourceRecord sourceRecord;

    public EmbeddedEngineChangeEvent(
            K key, V value, List<Header<H>> headers, SourceRecord sourceRecord) {
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.sourceRecord = sourceRecord;
    }

    @Override
    public K key() {
        return key;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public List<Header<H>> headers() {
        return headers;
    }

    @Override
    public V record() {
        return value;
    }

    @Override
    public String destination() {
        return sourceRecord.topic();
    }

    public SourceRecord sourceRecord() {
        return sourceRecord;
    }

    @Override
    public String toString() {
        return "EmbeddedEngineChangeEvent [key="
                + key
                + ", value="
                + value
                + ", sourceRecord="
                + sourceRecord
                + "]";
    }
}
