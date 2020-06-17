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
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DGraphDriver extends TestDriver<Transaction, Map<String, String>, DgraphProto.Response> {

    protected DgraphClient client;
    protected Gson gson = new Gson();
    protected ManagedChannel channel;

    public DGraphDriver() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 9080)
                .usePlaintext().build();
        DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel);

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
                "versionHistory: [string] .\n" +
                "version: int .\n" +
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
        try {
            final Transaction txn = startTransaction();

            String query = "{\n" +
                    "    q(func: eq(id, $person1Id)) {\n" +
                    "      v as uid\n" +
                    "    }\n" +
                    "  }";
            query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(v) <emails> \"$newEmail\" .".replace("$newEmail", (String) parameters.get("newEmail")));

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request1 = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request1);

            // Part 2
            String query2 = "{\n" +
                    "    all(func: eq(id, $person2Id)) {\n" +
                    "      uid\n" +
                    "    }\n" +
                    "  }";
            query2 = query2.replace("$person2Id", String.valueOf(parameters.get("person2Id")));

            DgraphProto.Response response = client.newReadOnlyTransaction().query(query2);

            People ppl = gson.fromJson(response.getJson().toStringUtf8(), People.class);

            if (!ppl.all.isEmpty()) {
                abortTransaction(txn);
            } else {
                ArrayList<String> p2AddMutation = new ArrayList<>();

                p2AddMutation.add("_:p2 <id> \"$person2Id\" .".replace("$person2Id", String.valueOf(parameters.get("person2Id"))));

                String p2AddMutationQuery = String.join("\n", p2AddMutation);

                DgraphProto.Mutation mu2 = DgraphProto.Mutation.newBuilder()
                        .setSetNquads(ByteString.copyFromUtf8(p2AddMutationQuery))
                        .build();

                DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                        .addMutations(mu2)
                        .setCommitNow(true)
                        .build();

                txn.doRequest(request2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:g1 <id> \"1\" .");
            mutationQueries.add("_:g1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g1 <versionHistory> \"0\" .");
            mutationQueries.add("_:g2 <id> \"2\" .");
            mutationQueries.add("_:g2 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g2 <versionHistory> \"0\" .");
            mutationQueries.add("_:g1 <knows> _:g2 (versionHistory=\"0\") .");

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
    public Map<String, Object> g0(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        try {
            String query = "{\n" +
                    "q(func: eq(id, \"$person1Id\")) {\n" +
                    "      v1 as uid\n" +
                    "      knows @filter(eq(id, \"$person2Id\")) {\n" +
                    "        v2 as uid\n" +
                    "      }\n" +
                    "    }\n" +
                    "}";

            query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));
            query = query.replace("$person2Id", String.valueOf(parameters.get("person2Id")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(v1) <versionHistory> \"$transactionId\" .".replace("$transactionId", String.valueOf(parameters.get("transactionId"))));
            mutationQueries.add("uid(v2) <versionHistory> \"$transactionId\" .".replace("$transactionId", String.valueOf(parameters.get("transactionId"))));
            mutationQueries.add("uid(v1) <knows> uid(v2) (versionHistory=\"$transactionId\") .".replace("$transactionId", String.valueOf(parameters.get("transactionId"))));

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request1 = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        String query = "{all(func: eq(id, \"$person1Id\")) @filter(has(knows)) {\n" +
                "    p1VersionHistory: versionHistory\n" +
                "    knows @filter(eq(id, \"$person2Id\")) {\n" +
                "      versionHistory\n" +
                "      p2VersionHistory: versionHistory\n" +
                "    }\n" +
                "  }}";
        query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));
        query = query.replace("$person2Id", String.valueOf(parameters.get("person2Id")));

        DgraphProto.Response response = txn.query(query);

        G0Response responseStatistics = gson.fromJson(response.getJson().toStringUtf8(), G0Response.class);
        G0ResponseP1Inner p1VersionHistoryRes = responseStatistics.all.get(responseStatistics.all.size() - 1);
        final List<Long> p1VersionHistory = new ArrayList<>(p1VersionHistoryRes.p1VersionHistory);
        G0ResponseKnows knowsRes = p1VersionHistoryRes.knows.get(p1VersionHistoryRes.knows.size() - 1);
        final List<Long> kVersionHistory = new ArrayList<>(knowsRes.versionHistory);
        final List<Long> p2VersionHistory = new ArrayList<>(knowsRes.p2VersionHistory);

        return ImmutableMap.of("p1VersionHistory", p1VersionHistory, "kVersionHistory", kVersionHistory, "p2VersionHistory", p2VersionHistory);
    }

    @Override
    public void g1aInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:g1 <id> \"1\" .");
            mutationQueries.add("_:g1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g1 <version> \"1\" .");

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
    public Map<String, Object> g1a1(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        String queryLookup = "{\n" +
                "  all(func: eq(id, \"$personId\")) {\n" +
                "    uid\n" +
                "  }\n" +
                "}";
        queryLookup = queryLookup.replace("$personId", String.valueOf(parameters.get("person1Id")));

        DgraphProto.Response response = client.newReadOnlyTransaction().query(queryLookup);

        People responseStatistics = gson.fromJson(response.getJson().toStringUtf8(), People.class);

        if (responseStatistics.all.isEmpty()) throw new IllegalStateException("G1a1 StatementResult empty");

        String query = "{\n" +
                "  all(func: eq(id, \"$personId\")) {\n" +
                "    p1 as uid\n" +
                "  }\n" +
                "}";

        query = query.replace("$personId", String.valueOf(parameters.get("person1Id")));

        sleep((Long) parameters.get("sleepTime"));

        ArrayList<String> mutationQueries = new ArrayList<>();

        mutationQueries.add("uid(p1) <version> \"2\" .");

        String joinedQueries = String.join("\n", mutationQueries);

        DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                .build();

        DgraphProto.Request request = DgraphProto.Request.newBuilder()
                .setQuery(query)
                .addMutations(mu)
                .setCommitNow(false)
                .build();
        txn.doRequest(request);

        sleep((Long) parameters.get("sleepTime"));

        abortTransaction(txn);

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1a2(Map<String, Object> parameters) {
        String queryLookup = "{\n" +
                "  all(func: eq(id, \"$personId\")) {\n" +
                "    version\n" +
                "  }\n" +
                "}";
        queryLookup = queryLookup.replace("$personId", String.valueOf(parameters.get("person1Id")));

        DgraphProto.Response response = client.newReadOnlyTransaction().query(queryLookup);

        People responseStatistics = gson.fromJson(response.getJson().toStringUtf8(), People.class);

        if (responseStatistics.all.isEmpty()) throw new IllegalStateException("G1a T2 StatementResult empty");

        final long pVersion = Long.parseLong(responseStatistics.all.get(0).version);

        return ImmutableMap.of("pVersion", pVersion);
    }

    @Override
    public void g1bInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:g1 <id> \"1\" .");
            mutationQueries.add("_:g1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g1 <version> \"99\" .");

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
    public Map<String, Object> g1b1(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        try {
            String query = "{\n" +
                    "  all(func: eq(id, \"$personId\")) {\n" +
                    "    p1 as uid\n" +
                    "  }\n" +
                    "}";

            query = query.replace("$personId", String.valueOf(parameters.get("personId")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(p1) <version> \"$even\" .".replace("$even", String.valueOf(parameters.get("even"))));

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);

            sleep((Long) parameters.get("sleepTime"));

            ArrayList<String> mutationQueries2 = new ArrayList<>();

            mutationQueries2.add("uid(p1) <version> \"$odd\" .".replace("$odd", String.valueOf(parameters.get("odd"))));

            String joinedQueries2 = String.join("\n", mutationQueries2);

            DgraphProto.Mutation mu2 = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries2))
                    .build();

            DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu2)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request2);

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1b2(Map<String, Object> parameters) {
        String queryLookup = "{\n" +
                "  all(func: eq(id, \"$personId\")) {\n" +
                "    version\n" +
                "  }\n" +
                "}";
        queryLookup = queryLookup.replace("$personId", String.valueOf(parameters.get("personId")));

        DgraphProto.Response response = client.newReadOnlyTransaction().query(queryLookup);

        People responseStatistics = gson.fromJson(response.getJson().toStringUtf8(), People.class);

        if (responseStatistics.all.isEmpty()) throw new IllegalStateException("G1b T2 StatementResult empty");

        final long pVersion = Long.parseLong(responseStatistics.all.get(0).version);

        return ImmutableMap.of("pVersion", pVersion);
    }

    @Override
    public void g1cInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:g1 <id> \"1\" .");
            mutationQueries.add("_:g1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g1 <version> \"0\" .");
            mutationQueries.add("_:g2 <id> \"2\" .");
            mutationQueries.add("_:g2 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g2 <version> \"0\" .");

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
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        long person2Version = -1;

        final Transaction txn = startTransaction();

        try {
            String query = "{\n" +
                    "  all(func: eq(id, \"$person1Id\")) {\n" +
                    "    p1 as uid\n" +
                    "  }\n" +
                    "}";

            query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(p1) <version> \"$transactionId\" .".replace("$transactionId", String.valueOf(parameters.get("transactionId"))));

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);

            String queryLookup = "{\n" +
                    "  all(func: eq(id, \"$person2Id\")) {\n" +
                    "    version\n" +
                    "  }\n" +
                    "}";
            queryLookup = queryLookup.replace("$person2Id", String.valueOf(parameters.get("person2Id")));

            DgraphProto.Response response = client.newReadOnlyTransaction().query(queryLookup);

            People responseStatistics = gson.fromJson(response.getJson().toStringUtf8(), People.class);

            person2Version = Long.parseLong(responseStatistics.all.get(0).version);

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of("person2Version", person2Version);
    }

    @Override
    public void impInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:g1 <id> \"1\" .");
            mutationQueries.add("_:g1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:g1 <version> \"1\" .");

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> imp1(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        try {
            String query = "{\n" +
                    "    all(func: eq(id, \"$personId\")) {\n" +
                    "      p1 as uid\n" +
                    "      prevVersion as version\n" +
                    "      nextVersion as math(prevVersion + 1)\n" +
                    "    }\n" +
                    "  }";

            query = query.replace("$personId", String.valueOf(parameters.get("personId")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(p1) <version> val(nextVersion) .");

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> imp2(Map<String, Object> parameters) {
        String queryLookup = "{\n" +
                "  all(func: eq(id, \"$personId\")) {\n" +
                "    version\n" +
                "  }\n" +
                "}";
        queryLookup = queryLookup.replace("$personId", String.valueOf(parameters.get("personId")));

        DgraphProto.Response response1 = client.newReadOnlyTransaction().query(queryLookup);

        People response1Statistics = gson.fromJson(response1.getJson().toStringUtf8(), People.class);

        if (response1Statistics.all.isEmpty()) throw new IllegalStateException("IMP result1 empty");

        final long firstRead = Long.parseLong(response1Statistics.all.get(0).version);

        sleep((Long) parameters.get("sleepTime"));

        DgraphProto.Response response2 = client.newReadOnlyTransaction().query(queryLookup);

        People response2Statistics = gson.fromJson(response2.getJson().toStringUtf8(), People.class);

        if (response2Statistics.all.isEmpty()) throw new IllegalStateException("IMP result2 empty");

        final long secondRead = Long.parseLong(response2Statistics.all.get(0).version);

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
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
        channel.shutdown();
    }
}
