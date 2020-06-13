//package dgraph;
//
//import com.google.gson.Gson;
//import com.google.protobuf.ByteString;
//import io.dgraph.DgraphClient;
//import io.dgraph.DgraphGrpc;
//import io.dgraph.DgraphProto;
//import io.dgraph.Transaction;
//import io.grpc.ManagedChannel;
//import io.grpc.ManagedChannelBuilder;
//import org.junit.Test;
//
//import java.util.Collections;
//import java.util.Map;
//
//public class DGraphTest {
//
//    @Test
//    public void dgraphTest() {
//        ManagedChannel channel1 = ManagedChannelBuilder
//                .forAddress("localhost", 9080)
//                .usePlaintext().build();
//        DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);
//
//        DgraphClient dgraphClient = new DgraphClient(stub1);
//
//        String schema = "name: string @index(exact) .";
//        DgraphProto.Operation operation = DgraphProto.Operation.newBuilder()
//                .setSchema(schema)
//                .setRunInBackground(false) // we DO NOT want to compute indices in the background
//                .build();
//        dgraphClient.alter(operation);
//
//        Gson gson = new Gson();
//
//        try (Transaction txn = dgraphClient.newTransaction()) {
//            // Create data
//            Person person = new Person();
//            person.name = "Alice";
//
//            // Serialize it
//            String json = gson.toJson(person);
//            // Run mutation
//            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
//                    .setCommitNow(true) // ?
//                    .setSetJson(ByteString.copyFromUtf8(json.toString()))
//                    .build();
//            txn.mutate(mu);
//        }
//
//        // Query
//        String query =
//                "query all($a: string){\n" +
//                        "  all(func: eq(name, $a)) {\n" +
//                        "    name\n" +
//                        "  }\n" +
//                        "}\n";
//
//        Map<String, String> vars = Collections.singletonMap("$a", "Alice");
//        DgraphProto.Response response = dgraphClient.newReadOnlyTransaction().queryWithVars(query, vars);
//
//        // Deserialize
//        People ppl = gson.fromJson(response.getJson().toStringUtf8(), People.class);
//
//        // Print results
//        System.out.printf("people found: %d\n", ppl.all.size());
//
//        for (Person person : ppl.all) {
//            System.out.println(person.name);
//            try (Transaction txn = dgraphClient.newTransaction()) {
//                // Create data
//                Person person2 = new Person();
//                person2.name = "Alice";
//
//                // Serialize it
//                String json = gson.toJson(person2);
//                // Run mutation
//                DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
//                        .setCommitNow(true) // ?
//                        .setSetJson(ByteString.copyFromUtf8(json.toString()))
//                        .build();
//                txn.mutate(mu);
//            }
//        }
//
////        ppl.all.forEach(person -> System.out.println(person.name));
//    }
//}
