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

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class TokenizeField<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final String PURPOSE = "tokenize fields";
    private static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.HIGH, "Names of fields to tokenize.");

    private Set<String> maskedFields;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * "transforms"                                 : "maskCC",
     * "transforms.maskCC.type"                     : "org.apache.kafka.connect.transforms.TokenizeField$Value",
     * "transforms.maskCC.fields"                   : "pii-topic.updated.card_number",
    **/

    // TODO
    // nested fields name structure
    // <topic name>.<optional parent>.<optional child>.<field name>

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        maskedFields = new HashSet<>(config.getList(FIELDS_CONFIG));
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

        final Struct original = requireStruct(operatingValue(record), PURPOSE);
        final Struct updated = new Struct(original.schema());

        updateFields(original.schema().fields(), record.topic(), original, updated);

        return newRecord(record, updated);
    }

    private void updateFields(List<Field> fields, String prefix, Struct value, Struct updatedValue) {
        for (Field field : fields) {

            if (Schema.Type.STRUCT.equals(field.schema().type())) {
                final Struct original = value.getStruct(field.name());
                final Struct updated = new Struct(field.schema());

                updateFields(field.schema().fields(), fullName(prefix, field), original, updated);
                updatedValue.put(field, updated);

            } else {
                final Object origFieldValue = value.get(field);
                updatedValue.put(field, maskedFields.contains(fullName(prefix, field)) ? masked(origFieldValue) : origFieldValue);
            }
        }
    }

    private static String fullName(String prefix, Field field) {
        return prefix.isEmpty() ? field.name() : (prefix + "|" + field.name());
    }

    private Object masked(Object value) {

        if (value.getClass().equals(String.class)) {
            return UUID.nameUUIDFromBytes(value.toString().getBytes()).toString();
        }

        throw new DataException("Cannot mask value of type " + value.getClass() + " with UUID.");
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

    public static final class Value<R extends ConnectRecord<R>> extends TokenizeField<R> {
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
