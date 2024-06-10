package io.confluent.transform;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TokenizeFieldTest {

    private static final Schema CHILD_SCHEMA = SchemaBuilder.struct()
            .field("cc_num", Schema.STRING_SCHEMA)
            .build();

    private static final Schema NESTED_SCHEMA = SchemaBuilder.struct()
            .field("secret", Schema.STRING_SCHEMA)
            .build();

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("full_name", Schema.STRING_SCHEMA)
            .field("card_number", Schema.STRING_SCHEMA)
            .field("count", Schema.INT32_SCHEMA)
            .field("child", CHILD_SCHEMA)
            .field("nested", NESTED_SCHEMA)
            .build();

    private static final Struct VALUES_WITH_SCHEMA = new Struct(SCHEMA);

    private static final Struct VALUES_WITH_CHILD_SCHEMA = new Struct(CHILD_SCHEMA);
    private static final Struct VALUES_WITH_NESTED_SCHEMA = new Struct(NESTED_SCHEMA);

    static {
        VALUES_WITH_CHILD_SCHEMA.put("cc_num", "5111 1111 1111 1111");
        VALUES_WITH_NESTED_SCHEMA.put("secret", "confluent");

        VALUES_WITH_SCHEMA.put("full_name", "Harry Potter");
        VALUES_WITH_SCHEMA.put("card_number", "4111 1111 1111 1111");
        VALUES_WITH_SCHEMA.put("count", 42);
        VALUES_WITH_SCHEMA.put("child", VALUES_WITH_CHILD_SCHEMA);
        VALUES_WITH_SCHEMA.put("nested", VALUES_WITH_NESTED_SCHEMA);
    }

    private static TokenizeField<SinkRecord> transform(List<String> fields) {
        final TokenizeField<SinkRecord> transformer = new TokenizeField.Value<>();
        final Map<String, Object> props = new HashMap<>();
        props.put("fields", fields);
        transformer.configure(props);
        return transformer;
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("pii", 0, null, null, schema, value, 0);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenizeFieldTest.class);

    @Test
    void testWhenRootUpdated() {

        final Struct updatedValue = (Struct) transform(Collections.singletonList("pii|card_number"))
                .apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        LOGGER.info(updatedValue.toString());

        assertEquals("Harry Potter", updatedValue.get("full_name"));
        assertNotEquals("4111 1111 1111 1111", updatedValue.get("card_number"));
        assertEquals(42, updatedValue.get("count"));
        assertEquals("5111 1111 1111 1111", ((Struct) updatedValue.get("child")).get("cc_num"));
        assertEquals("confluent", ((Struct) updatedValue.get("nested")).get("secret"));
    }

    @Test
    void testWhenChildUpdated() {

        final Struct updatedValue = (Struct) transform(Collections.singletonList("pii|child|cc_num"))
                .apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        LOGGER.info(updatedValue.toString());

        assertEquals("Harry Potter", updatedValue.get("full_name"));
        assertEquals("4111 1111 1111 1111", updatedValue.get("card_number"));
        assertEquals(42, updatedValue.get("count"));
        assertNotEquals("5111 1111 1111 1111", ((Struct) updatedValue.get("child")).get("cc_num"));
        assertEquals("confluent", ((Struct) updatedValue.get("nested")).get("secret"));
    }

    @Test
    void testWhenManyUpdated() {

        final List<String> fields = new ArrayList<>();
        fields.add("pii|card_number");
        fields.add("pii|child|cc_num");
        fields.add("pii|nested|secret");

        final Struct updatedValue = (Struct) transform(fields)
                .apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value();

        LOGGER.info(updatedValue.toString());

        assertEquals("Harry Potter", updatedValue.get("full_name"));
        assertNotEquals("4111 1111 1111 1111", updatedValue.get("card_number"));
        assertEquals(42, updatedValue.get("count"));
        assertNotEquals("5111 1111 1111 1111", ((Struct) updatedValue.get("child")).get("cc_num"));
        assertNotEquals("confluent", ((Struct) updatedValue.get("nested")).get("secret"));
    }

    @Test
    void testWhenWrongType() {

        final Exception exception = assertThrows(DataException.class,
                () -> transform(Collections.singletonList("pii|count"))
                .apply(record(SCHEMA, VALUES_WITH_SCHEMA)).value());

        LOGGER.info("Failed with [{}]", exception.getMessage());
    }
}