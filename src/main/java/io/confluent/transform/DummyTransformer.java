package io.confluent.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class DummyTransformer<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(DummyTransformer.class);

    public static final ConfigDef CONFIG_DEF = new ConfigDef();


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * "transforms"                          : "dummy",
     * "transforms.dummy.type"               : "io.confluent.transform.DummyTransformer$Value",
     */

    @Override
    public void configure(Map<String, ?> props) {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null || operatingSchema(record) == null) {
            return record;
        }

        return applyWithSchema(record);
    }

    private R applyWithSchema(R record) {

        LOGGER.info("Record: {}", record);

        return record;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

//    public static final class Key<R extends ConnectRecord<R>> extends TokenizeField<R> {
//        @Override
//        protected Schema operatingSchema(R record) {
//            return record.keySchema();
//        }
//
//        @Override
//        protected Object operatingValue(R record) {
//            return record.key();
//        }
//
//        @Override
//        protected R newRecord(R record, Object updatedValue) {
//            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
//        }
//    }

    public static final class Value<R extends ConnectRecord<R>> extends DummyTransformer<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                    record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}
