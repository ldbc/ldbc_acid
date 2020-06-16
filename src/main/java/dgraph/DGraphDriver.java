package dgraph;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import driver.TestDriver;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Map;

public class DGraphDriver extends TestDriver<Transaction, Map<String, String>, DgraphProto.Response> {

    protected DgraphClient client;
    protected Gson gson = new Gson();

    public DGraphDriver() {
        ManagedChannel channel1 = ManagedChannelBuilder
                .forAddress("localhost", 9080)
                .usePlaintext().build();
        DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);

        client = new DgraphClient(stub1);
    }

    @Override
    public Transaction startTransaction() {
        return client.newTransaction();
    }

    @Override
    public void commitTransaction(Transaction tt) {
        tt.commit();
    }

    @Override
    public void abortTransaction(Transaction tt) {
        tt.discard();
    }

    @Override
    public DgraphProto.Response runQuery(Transaction tt, String querySpecification, Map<String, String> queryParameters) {
        return null;
    }

    @Override
    public void nukeDatabase() {
        client.alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());

        String schema = "id: string @index(term) .\n" +
                "name: string @index(term) .\n" +
                "emails: [string] .";
        DgraphProto.Operation operation = DgraphProto.Operation.newBuilder().setSchema(schema).build();
        client.alter(operation);
    }

    @Override
    public void atomicityInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:alice <id> \"1\" .");
            mutationQueries.add("_:alice <name> \"Alice\" .");
            mutationQueries.add("_:alice <dgraph.type> \"Person\" .");
            mutationQueries.add("_:alice <emails> \"alice@aol.com\" .");
            mutationQueries.add("_:bob <id> \"2\" .");
            mutationQueries.add("_:bob <name> \"Bob\" .");
            mutationQueries.add("_:bob <dgraph.type> \"Person\" .");
            mutationQueries.add("_:bob <emails> \"bobby@yahoo.com\" .");
            mutationQueries.add("_:bob <emails> \"bob@hotmail.com\" .");
            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .addMutations(mu)
                    .setCommitNow(true)
                    .build();
            txn.doRequest(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void atomicityC(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        try {
            String query = "{\n" +
                    "    q(func: eq(id, $person1Id)) {\n" +
                    "      v as uid\n" +
                    "    }\n" +
                    "  }";
            query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:p2 <dgraph.type> \"Person\" .");
            mutationQueries.add("uid(v) <knows> _:p2 .");
            mutationQueries.add("uid(v) <emails> \"$newEmail\" .".replace("$newEmail", (String) parameters.get("newEmail")));
            mutationQueries.add("_:p2 <id> \"$person2Id\" .".replace("$person2Id", String.valueOf(parameters.get("person2Id"))));
            mutationQueries.add("uid(v) <knows> _:p2 (since=$since) .".replace("$since", String.valueOf(parameters.get("since"))));

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu)
                    .setCommitNow(true)
                    .build();
            txn.doRequest(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void atomicityRB(Map<String, Object> parameters) {

    }

    @Override
    public Map<String, Object> atomicityCheck() {
        String query = "{\n" +
                "  var(func: type(Person)) {\n" +
                "    numPersons as count(uid)\n" +
                "    nameCount as count(name)\n" +
                "    emailCount as count(emails)\n" +
                "  }\n" +
                "  \n" +
                "  result() {\n" +
                "    finalNumPersons: sum(val(numPersons))\n" +
                "    finalNameCount: sum(val(nameCount))\n" +
                "    finalEmailCount: sum(val(emailCount))\n" +
                "  }\n" +
                "}";

        DgraphProto.Response response = client.newReadOnlyTransaction().query(query);

        AtomicityCheckResponse responseStatistics = gson.fromJson(response.getJson().toStringUtf8(), AtomicityCheckResponse.class);

        final Long[] numPersons = {0L, 0L, 0L};

        responseStatistics.result.forEach(item -> {
            if (item.containsKey("finalNumPersons")) {
                numPersons[0] = item.get("finalNumPersons");
            } else if (item.containsKey("finalNameCount")) {
                numPersons[1] = item.get("finalNameCount");
            } else if (item.containsKey("finalEmailCount")) {
                numPersons[2] = item.get("finalEmailCount");
            }
        });

        return ImmutableMap.of("numPersons", numPersons[0], "numNames", numPersons[1], "numEmails", numPersons[2]);
    }

    @Override
    public void g0Init() {

    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void g1aInit() {
    }

    @Override
    public Map<String, Object> g1a1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1a2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void g1bInit() {
        final Transaction txn = startTransaction();

        // Create data
        Person person = new Person();
        person.name = "Alice";

        // Serialize it
        String json = gson.toJson(person);
        // Run mutation
        DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                .setCommitNow(true) // ?
                .setSetJson(ByteString.copyFromUtf8(json))
                .build();
        txn.mutate(mu);
    }

    @Override
    public Map<String, Object> g1b1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1b2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void g1cInit() {

    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void impInit() {

    }

    @Override
    public Map<String, Object> imp1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> imp2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void pmpInit() {

    }

    @Override
    public Map<String, Object> pmp1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> pmp2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void otvInit() {

    }

    @Override
    public Map<String, Object> otv1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> otv2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void frInit() {

    }

    @Override
    public Map<String, Object> fr1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> fr2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void luInit() {

    }

    @Override
    public Map<String, Object> lu1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> lu2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void wsInit() {

    }

    @Override
    public Map<String, Object> ws1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> ws2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
