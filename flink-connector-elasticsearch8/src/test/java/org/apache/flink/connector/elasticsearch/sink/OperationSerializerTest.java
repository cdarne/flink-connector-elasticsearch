package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;

import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.connector.elasticsearch.sink.AsyncSinkWriterTestUtils.assertThatBufferStatesAreEqual;
import static org.apache.flink.connector.elasticsearch.sink.AsyncSinkWriterTestUtils.getTestState;

/** OperationSerializerTest. */
public class OperationSerializerTest {
    private static final Elasticsearch8SinkBuilder.OperationConverter<Elasticsearch8SinkTest.DummyData> ELEMENT_CONVERTER =
        new Elasticsearch8SinkBuilder.OperationConverter<>((element, context) ->
            new UpdateOperation
                .Builder<Elasticsearch8SinkTest.DummyData, Elasticsearch8SinkTest.DummyData>()
                .index("test")
                .id(element.getId())
                .action(op -> op.doc(element).docAsUpsert(true))
                .retryOnConflict(3)
                .build(), 3);

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<Operation> expectedState =
            getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        Elasticsearch8SinkSerializer serializer = new Elasticsearch8SinkSerializer();
        BufferedRequestState<Operation> actualState =
                serializer.deserialize(1, serializer.serialize(expectedState));
        assertThatBufferStatesAreEqual(actualState, expectedState);
    }

    private int getRequestSize(Operation requestEntry) {
        return new OperationSerializer().size(requestEntry);
    }
}
