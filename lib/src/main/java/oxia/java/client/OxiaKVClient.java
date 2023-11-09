package oxia.java.client;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.streamnative.oxia.proto.*;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OxiaKVClient {

    private final OxiaClientGrpc.OxiaClientBlockingStub blockingStub;
    private List<ShardAssignment> shardAssignments;

    public OxiaKVClient(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        blockingStub = OxiaClientGrpc.newBlockingStub(channel);
        getShardAssignments();
    }

    private void getShardAssignments() {
        ShardAssignmentsRequest request = ShardAssignmentsRequest.newBuilder()
                .setNamespace("default")
                .build();
        Iterator<ShardAssignments> assignmentsIterator = blockingStub.getShardAssignments(request);
        if (assignmentsIterator.hasNext()) {
            ShardAssignments assignments = assignmentsIterator.next();
            shardAssignments = assignments.getNamespacesMap().get("default").getAssignmentsList();
        }
    }

    private long chooseShard(String key) {
        int hash = Hashing.consistentHash(
                Hashing.sha256().hashString(key, StandardCharsets.UTF_8),
                shardAssignments.size());
        return shardAssignments.get(hash).getShardId();
    }

    public void put(String key, byte[] value) {
        long shardId = chooseShard(key);
        PutRequest putRequest = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        WriteRequest writeRequest = WriteRequest.newBuilder()
                .setShardId(shardId)
                .addPuts(putRequest)
                .build();
        WriteResponse writeResponse = blockingStub.write(writeRequest);
        handleWriteResponse(writeResponse);
    }

    public byte[] get(String key) {
        long shardId = chooseShard(key);
        GetRequest getRequest = GetRequest.newBuilder()
                .setKey(key)
                .setIncludeValue(true)
                .build();
        ReadRequest readRequest = ReadRequest.newBuilder()
                .setShardId(shardId)
                .addGets(getRequest)
                .build();
        Iterator<ReadResponse> responses = blockingStub.read(readRequest);
        return handleReadResponse(responses);
    }

    public void delete(String key) {
        long shardId = chooseShard(key);
        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setKey(key)
                .build();
        WriteRequest writeRequest = WriteRequest.newBuilder()
                .setShardId(shardId)
                .addDeletes(deleteRequest)
                .build();
        WriteResponse writeResponse = blockingStub.write(writeRequest);
        handleWriteResponse(writeResponse);
    }

    private void handleWriteResponse(WriteResponse response) {

    }

    private byte[] handleReadResponse(Iterator<ReadResponse> responses) {
        while (responses.hasNext()) {
            ReadResponse response = responses.next();
            for (GetResponse getResponse : response.getGetsList()) {
                if (getResponse.getStatus() == Status.OK) {
                    return getResponse.getValue().toByteArray();
                }
            }
        }
        return null;
    }

    public void shutdown() throws InterruptedException {
        ((ManagedChannel) blockingStub.getChannel()).shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws InterruptedException {
        OxiaKVClient client = new OxiaKVClient("localhost", 6648);
        try {
            String key = "test-key";
            byte[] value = "test-value".getBytes();

            client.put(key, value);
            byte[] retrievedValue = client.get(key);
            if (retrievedValue != null) {
                System.out.println("Retrieved value: " + new String(retrievedValue));
            } else {
                System.out.println("Value not found for key: " + key);
            }
            client.delete(key);
        } finally {
            client.shutdown();
        }
    }
}
