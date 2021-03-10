package com.wuuuk.flink.format;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class NullRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /**
     * Flag indicating whether to fail if a field is missing.
     */
    private final boolean failOnMissingField;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;

    /**
     * TypeInformation of the produced {@link RowData}.
     **/
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Runtime converter that converts {@link JsonNode}s into
     * objects of Flink SQL internal data structures.
     **/

    /**
     * Object mapper for parsing the JSON.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Timestamp format specification which is used to parse timestamp.
     */
    private final TimestampFormat timestampFormat;

    public NullRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    @Override
    // 这里其实是真正的反序列化逻辑，比如说将 json 拍平 (多层嵌套转化为一层嵌套 )
    // 这里是重点，记得关注重点
    public RowData deserialize(byte[] message) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NullRowDataDeserializationSchema that = (NullRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField &&
                ignoreParseErrors == that.ignoreParseErrors &&
                resultTypeInfo.equals(that.resultTypeInfo) &&
                timestampFormat.equals(that.timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo, timestampFormat);
    }
}