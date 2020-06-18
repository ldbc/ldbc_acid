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
import java.util.Random;

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

        String schema = "id: int @index(int) .\n" +
                "name: string @index(term) .\n" +
                "versionHistory: [string] .\n" +
                "version: int .\n" +
                "numFriends: int .\n" +
                "value: int .\n" +
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
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);

            commitTransaction(txn);
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

            commitTransaction(txn);
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
                        .setCommitNow(false)
                        .build();

                txn.doRequest(request2);
                commitTransaction(txn);
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
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);
            commitTransaction(txn);
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

            commitTransaction(txn);
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
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);
            commitTransaction(txn);
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
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);
            commitTransaction(txn);
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
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request);
            commitTransaction(txn);
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
        final Transaction txn = startTransaction();

        String queryLookup = "{\n" +
                "  all(func: eq(id, \"$personId\")) {\n" +
                "    version\n" +
                "  }\n" +
                "}";
        queryLookup = queryLookup.replace("$personId", String.valueOf(parameters.get("personId")));

        DgraphProto.Request request = DgraphProto.Request.newBuilder()
                .setQuery(queryLookup)
                .setCommitNow(false)
                .build();
        DgraphProto.Response response1 = txn.doRequest(request);

        People response1Statistics = gson.fromJson(response1.getJson().toStringUtf8(), People.class);

        if (response1Statistics.all.isEmpty()) throw new IllegalStateException("IMP result1 empty");

        final long firstRead = Long.parseLong(response1Statistics.all.get(0).version);

        sleep((Long) parameters.get("sleepTime"));

        DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                .setQuery(queryLookup)
                .setCommitNow(false)
                .build();
        DgraphProto.Response response2 = txn.doRequest(request2);

        People response2Statistics = gson.fromJson(response2.getJson().toStringUtf8(), People.class);

        if (response2Statistics.all.isEmpty()) throw new IllegalStateException("IMP result2 empty");

        final long secondRead = Long.parseLong(response2Statistics.all.get(0).version);

        commitTransaction(txn);

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void pmpInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:g1 <id> \"1\" .");
            mutationQueries.add("_:g1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p1 <id> \"1\" .");
            mutationQueries.add("_:p1 <dgraph.type> \"Post\" .");

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
    public Map<String, Object> pmp1(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        try {
            String query = "{\n" +
                    "    person(func: eq(id, \"$personId\")) @filter(type(Person)) {\n" +
                    "      pe as uid\n" +
                    "    }\n" +
                    "    post(func: eq(id, \"$postId\")) @filter(type(Post)) {\n" +
                    "      po as uid\n" +
                    "    }\n" +
                    "  }";

            query = query.replace("$personId", String.valueOf(parameters.get("personId")));
            query = query.replace("$postId", String.valueOf(parameters.get("postId")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(po) <liked_by> uid(pe) .");

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
    public Map<String, Object> pmp2(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        long firstRead = -1;
        long secondRead = -1;

        try {
            String query = "{\n" +
                    "  all(func: eq(id, \"$postId\")) @filter(type(Post)) {\n" +
                    "    liked_by @filter(type(Person)) {\n" +
                    "       totalPeople: count(uid)\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            query = query.replace("$postId", String.valueOf(parameters.get("postId")));

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response1 = txn.doRequest(request);

            PmpResponse pmpResponse1 = gson.fromJson(response1.getJson().toStringUtf8(), PmpResponse.class);

            if (pmpResponse1.all.isEmpty()) throw new IllegalStateException("PMP result1 empty");

            firstRead = Long.parseLong(pmpResponse1.all.get(0).liked_by.get(0).totalPeople);

            sleep((Long) parameters.get("sleepTime"));

            DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response2 = txn.doRequest(request2);

            PmpResponse pmpResponse2 = gson.fromJson(response2.getJson().toStringUtf8(), PmpResponse.class);

            if (pmpResponse2.all.isEmpty()) throw new IllegalStateException("PMP result2 empty");

            secondRead = Long.parseLong(pmpResponse2.all.get(0).liked_by.get(0).totalPeople);
            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void otvInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:p1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p1 <id> \"1\" .");
            mutationQueries.add("_:p1 <version> \"0\" .");

            mutationQueries.add("_:p2 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p2 <id> \"2\" .");
            mutationQueries.add("_:p2 <version> \"0\" .");

            mutationQueries.add("_:p3 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p3 <id> \"3\" .");
            mutationQueries.add("_:p3 <version> \"0\" .");

            mutationQueries.add("_:p4 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p4 <id> \"4\" .");
            mutationQueries.add("_:p4 <version> \"0\" .");

            mutationQueries.add("_:p1 <knows> _:p2 .");
            mutationQueries.add("_:p2 <knows> _:p3 .");
            mutationQueries.add("_:p3 <knows> _:p4 .");
            mutationQueries.add("_:p4 <knows> _:p1 .");

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
    public Map<String, Object> otv1(Map<String, Object> parameters) {

        try {
            Random random = new Random();
            for (int i = 0; i < 100; i++) {
                final Transaction txn = startTransaction();
                long personId = random.nextInt((int) parameters.get("cycleSize") + 1);

                String query = "{\n" +
                        "  all(func: eq(id, \"$personId\")) @filter(type(Person)){\n" +
                        "    p1 as uid\n" +
                        "    p1PrevVersion as version\n" +
                        "    p1NextVersion as math(p1PrevVersion + 1)\n" +
                        "    knows {\n" +
                        "      p2 as uid\n" +
                        "      p2PrevVersion as version\n" +
                        "    \tp2NextVersion as math(p2PrevVersion + 1)\n" +
                        "      knows {\n" +
                        "        p3 as uid\n" +
                        "        p3PrevVersion as version\n" +
                        "        p3NextVersion as math(p3PrevVersion + 1)\n" +
                        "        knows {\n" +
                        "          p4 as uid\n" +
                        "          p4PrevVersion as version\n" +
                        "          p4NextVersion as math(p4PrevVersion + 1)\n" +
                        "          knows @filter(eq(id, \"$personId\")) {\n" +
                        "            uid\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";

                query = query.replace("$personId", String.valueOf(personId));

                ArrayList<String> mutationQueries = new ArrayList<>();

                mutationQueries.add("uid(p1) <version> val(p1NextVersion) .");
                mutationQueries.add("uid(p2) <version> val(p2NextVersion) .");
                mutationQueries.add("uid(p3) <version> val(p3NextVersion) .");
                mutationQueries.add("uid(p4) <version> val(p4NextVersion) .");

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
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> otv2(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        ArrayList<Long> firstRead = new ArrayList<>();
        ArrayList<Long> secondRead = new ArrayList<>();

        try {
            String query = "{\n" +
                    "  all(func: eq(id, \"$personId\")) @filter(type(Person)){\n" +
                    "    version\n" +
                    "    knows {\n" +
                    "      version\n" +
                    "      knows {\n" +
                    "        version\n" +
                    "        knows {\n" +
                    "          version\n" +
                    "          knows @filter(eq(id, \"$personId\")) {\n" +
                    "            \n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            query = query.replace("$personId", String.valueOf(parameters.get("personId")));

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response1 = txn.doRequest(request);

            People peopleResponse1 = gson.fromJson(response1.getJson().toStringUtf8(), People.class);

            if (peopleResponse1.all.isEmpty()) throw new IllegalStateException("OTV2 result1 empty");

            firstRead.add(Long.valueOf(peopleResponse1.all.get(0).version));
            if (!peopleResponse1.all.get(0).knows.isEmpty())
                firstRead.add(Long.valueOf(peopleResponse1.all.get(0).knows.get(0).version));
            if (!peopleResponse1.all.get(0).knows.get(0).knows.isEmpty())
                firstRead.add(Long.valueOf(peopleResponse1.all.get(0).knows.get(0).knows.get(0).version));
            if (!peopleResponse1.all.get(0).knows.get(0).knows.get(0).knows.isEmpty())
                firstRead.add(Long.valueOf(peopleResponse1.all.get(0).knows.get(0).knows.get(0).knows.get(0).version));

            sleep((Long) parameters.get("sleepTime"));

            DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response2 = txn.doRequest(request2);

            People peopleResponse2 = gson.fromJson(response2.getJson().toStringUtf8(), People.class);

            if (peopleResponse2.all.isEmpty()) throw new IllegalStateException("OTV2 result2 empty");

            secondRead.add(Long.valueOf(peopleResponse2.all.get(0).version));
            if (!peopleResponse2.all.get(0).knows.isEmpty())
                secondRead.add(Long.valueOf(peopleResponse2.all.get(0).knows.get(0).version));
            if (!peopleResponse2.all.get(0).knows.get(0).knows.isEmpty())
                secondRead.add(Long.valueOf(peopleResponse2.all.get(0).knows.get(0).knows.get(0).version));
            if (!peopleResponse2.all.get(0).knows.get(0).knows.get(0).knows.isEmpty())
                secondRead.add(Long.valueOf(peopleResponse2.all.get(0).knows.get(0).knows.get(0).knows.get(0).version));

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void frInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:p1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p1 <id> \"1\" .");
            mutationQueries.add("_:p1 <version> \"0\" .");

            mutationQueries.add("_:p2 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p2 <id> \"2\" .");
            mutationQueries.add("_:p2 <version> \"0\" .");

            mutationQueries.add("_:p3 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p3 <id> \"3\" .");
            mutationQueries.add("_:p3 <version> \"0\" .");

            mutationQueries.add("_:p4 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p4 <id> \"4\" .");
            mutationQueries.add("_:p4 <version> \"0\" .");

            mutationQueries.add("_:p1 <knows> _:p2 .");
            mutationQueries.add("_:p2 <knows> _:p3 .");
            mutationQueries.add("_:p3 <knows> _:p4 .");
            mutationQueries.add("_:p4 <knows> _:p1 .");

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
    public Map<String, Object> fr1(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        try {

            String query = "{\n" +
                    "  all(func: eq(id, \"$personId\")) @filter(type(Person)){\n" +
                    "    p1 as uid\n" +
                    "    p1PrevVersion as version\n" +
                    "    p1NextVersion as math(p1PrevVersion + 1)\n" +
                    "    knows {\n" +
                    "      p2 as uid\n" +
                    "      p2PrevVersion as version\n" +
                    "    \tp2NextVersion as math(p2PrevVersion + 1)\n" +
                    "      knows {\n" +
                    "        p3 as uid\n" +
                    "        p3PrevVersion as version\n" +
                    "        p3NextVersion as math(p3PrevVersion + 1)\n" +
                    "        knows {\n" +
                    "          p4 as uid\n" +
                    "          p4PrevVersion as version\n" +
                    "          p4NextVersion as math(p4PrevVersion + 1)\n" +
                    "          knows @filter(eq(id, \"$personId\")) {\n" +
                    "            uid\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            query = query.replace("$personId", String.valueOf(parameters.get("personId")));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(p1) <version> val(p1NextVersion) .");
            mutationQueries.add("uid(p2) <version> val(p2NextVersion) .");
            mutationQueries.add("uid(p3) <version> val(p3NextVersion) .");
            mutationQueries.add("uid(p4) <version> val(p4NextVersion) .");

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
    public Map<String, Object> fr2(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        ArrayList<Long> firstRead = new ArrayList<>();
        ArrayList<Long> secondRead = new ArrayList<>();

        try {
            String query = "{\n" +
                    "  all(func: eq(id, \"$personId\")) @filter(type(Person)){\n" +
                    "    version\n" +
                    "    knows {\n" +
                    "      version\n" +
                    "      knows {\n" +
                    "        version\n" +
                    "        knows {\n" +
                    "          version\n" +
                    "          knows @filter(eq(id, \"$personId\")) {\n" +
                    "            \n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            query = query.replace("$personId", String.valueOf(parameters.get("personId")));

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response1 = txn.doRequest(request);

            People peopleResponse1 = gson.fromJson(response1.getJson().toStringUtf8(), People.class);

            if (peopleResponse1.all.isEmpty()) throw new IllegalStateException("OTV2 result1 empty");

            firstRead.add(Long.valueOf(peopleResponse1.all.get(0).version));
            if (!peopleResponse1.all.get(0).knows.isEmpty())
                firstRead.add(Long.valueOf(peopleResponse1.all.get(0).knows.get(0).version));
            if (!peopleResponse1.all.get(0).knows.get(0).knows.isEmpty())
                firstRead.add(Long.valueOf(peopleResponse1.all.get(0).knows.get(0).knows.get(0).version));
            if (!peopleResponse1.all.get(0).knows.get(0).knows.get(0).knows.isEmpty())
                firstRead.add(Long.valueOf(peopleResponse1.all.get(0).knows.get(0).knows.get(0).knows.get(0).version));

            sleep((Long) parameters.get("sleepTime"));

            DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response2 = txn.doRequest(request2);

            People peopleResponse2 = gson.fromJson(response2.getJson().toStringUtf8(), People.class);

            if (peopleResponse2.all.isEmpty()) throw new IllegalStateException("OTV2 result2 empty");

            secondRead.add(Long.valueOf(peopleResponse2.all.get(0).version));
            if (!peopleResponse2.all.get(0).knows.isEmpty())
                secondRead.add(Long.valueOf(peopleResponse2.all.get(0).knows.get(0).version));
            if (!peopleResponse2.all.get(0).knows.get(0).knows.isEmpty())
                secondRead.add(Long.valueOf(peopleResponse2.all.get(0).knows.get(0).knows.get(0).version));
            if (!peopleResponse2.all.get(0).knows.get(0).knows.get(0).knows.isEmpty())
                secondRead.add(Long.valueOf(peopleResponse2.all.get(0).knows.get(0).knows.get(0).knows.get(0).version));

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void luInit() {
        final Transaction txn = startTransaction();

        try {
            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("_:p1 <dgraph.type> \"Person\" .");
            mutationQueries.add("_:p1 <id> \"1\" .");
            mutationQueries.add("_:p1 <numFriends> \"0\" .");

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
    public Map<String, Object> lu1(Map<String, Object> parameters) {

        final Transaction txn = startTransaction();

        String query = "{\n" +
                "    all(func: eq(id, \"$person1Id\")) @filter(type(Person)) {\n" +
                "      p1 as uid\n" +
                "      prevNumFriends as numFriends\n" +
                "      newNumFriends: nextNumFriends as math(prevNumFriends + 1)\n" +
                "    }\n" +
                "  }";

        query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));

        ArrayList<String> mutationQueries = new ArrayList<>();

        mutationQueries.add("_:p2 <dgraph.type> \"Person\" .");
        mutationQueries.add("_:p2 <id> \"$person2Id\" .".replace("$person2Id", String.valueOf(parameters.get("person2Id"))));
        mutationQueries.add("uid(p1) <knows> _:p2 .");
        mutationQueries.add("uid(p1) <numFriends> val(nextNumFriends) .");

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
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> lu2(Map<String, Object> parameters) {

        final Transaction txn = startTransaction();

        long numKnowsEdges = -1;
        long numFriends = -1;

        try {
            String query = "{\n" +
                    "  all(func: eq(id, \"$personId\")) @filter(type(Person)){\n" +
                    "    uid\n" +
                    "    numKnowsEdges: count(knows)\n" +
                    "    numFriendsProp: numFriends\n" +
                    "  }\n" +
                    "}";
            query = query.replace("$personId", String.valueOf(parameters.get("personId")));

            DgraphProto.Request request = DgraphProto.Request.newBuilder()
                    .setQuery(query)
                    .setCommitNow(false)
                    .build();
            DgraphProto.Response response1 = txn.doRequest(request);

            People peopleResponse = gson.fromJson(response1.getJson().toStringUtf8(), People.class);

            if (!peopleResponse.all.isEmpty()) {
                numKnowsEdges = Long.parseLong(peopleResponse.all.get(0).numKnowsEdges);
                numFriends = Long.parseLong(peopleResponse.all.get(0).numFriendsProp);
            }

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ImmutableMap.of("numKnowsEdges", numKnowsEdges, "numFriendsProp", numFriends);
    }

    @Override
    public void wsInit() {
        final Transaction txn = startTransaction();

        try {
            // create 10 pairs of persons with indices (1,2), ..., (19,20)
            for (int i = 1; i <= 10; i++) {

                ArrayList<String> mutationQueries = new ArrayList<>();

                mutationQueries.add("_:p" + (2 * i - 1) + " <dgraph.type> \"Person\" .");
                mutationQueries.add("_:p" + (2 * i - 1) + " <id> \"$person1Id\" .".replace("$person1Id", String.valueOf(2 * i - 1)));
                mutationQueries.add("_:p" + (2 * i - 1) + " <value> \"70\" .");

                mutationQueries.add("_:p" + 2 * i + " <dgraph.type> \"Person\" .");
                mutationQueries.add("_:p" + 2 * i + " <id> \"$person2Id\" .".replace("$person2Id", String.valueOf(2 * i)));
                mutationQueries.add("_:p" + 2 * i + " <value> \"80\" .");

                String joinedQueries = String.join("\n", mutationQueries);

                DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                        .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                        .build();

                DgraphProto.Request request = DgraphProto.Request.newBuilder()
                        .addMutations(mu)
                        .setCommitNow(false)
                        .build();
                txn.doRequest(request);
            }

            commitTransaction(txn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> ws1(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        String query = "{\n" +
                "  \n" +
                "  person1(func: eq(id, \"$person1Id\")) @filter(type(Person)) {\n" +
                "    value\n" +
                "  }\n" +
                "    \n" +
                "  person2(func: eq(id, \"$person2Id\")) @filter(type(Person)) {\n" +
                "    value\n" +
                "  }\n" +
                "}\n";

        query = query.replace("$person1Id", String.valueOf(parameters.get("person1Id")));
        query = query.replace("$person2Id", String.valueOf(parameters.get("person2Id")));

        DgraphProto.Request request1 = DgraphProto.Request.newBuilder()
                .setQuery(query)
                .setCommitNow(false)
                .build();
        DgraphProto.Response response1 = txn.doRequest(request1);

        Ws1Response peopleResponse = gson.fromJson(response1.getJson().toStringUtf8(), Ws1Response.class);

        if (!peopleResponse.person1.isEmpty() &&
                !peopleResponse.person2.isEmpty() &&
                (Integer.parseInt(peopleResponse.person1.get(0).value) +
                        Integer.parseInt(peopleResponse.person2.get(0).value) >= 0)) {

            sleep((Long) parameters.get("sleepTime"));

            long personId = new Random().nextBoolean() ?
                    (long) parameters.get("person1Id") :
                    (long) parameters.get("person2Id");

            String query2 = "{\n" +
                    "  \n" +
                    "  person1(func: eq(id, \"$personId\")) @filter(type(Person)) {\n" +
                    "    p1 as uid\n" +
                    "    preValue as value\n" +
                    "    nextValue as math(preValue - 100)\n" +
                    "  }\n" +
                    "}";

            query2 = query2.replace("$personId", String.valueOf(personId));

            ArrayList<String> mutationQueries = new ArrayList<>();

            mutationQueries.add("uid(p1) <value> val(nextValue) .");

            String joinedQueries = String.join("\n", mutationQueries);

            DgraphProto.Mutation mu = DgraphProto.Mutation.newBuilder()
                    .setSetNquads(ByteString.copyFromUtf8(joinedQueries))
                    .build();

            DgraphProto.Request request2 = DgraphProto.Request.newBuilder()
                    .setQuery(query2)
                    .addMutations(mu)
                    .setCommitNow(false)
                    .build();
            txn.doRequest(request2);
        }

        commitTransaction(txn);

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> ws2(Map<String, Object> parameters) {
        final Transaction txn = startTransaction();

        long p1id;
        long p1value;
        long p2id;
        long p2value;

        String query = "{\n" +
                "  \n" +
                "  person1(func: type(Person)) {\n" +
                "    p1Id as id\n" +
                "    value\n" +
                "    nextId as math(p1Id + 1)\n" +
                "  }\n" +
                "    \n" +
                "  person2(func: eq(id, val(nextId))) @filter(type(Person)) {\n" +
                "    id\n" +
                "    value\n" +
                "  }\n" +
                "}";

        DgraphProto.Request request1 = DgraphProto.Request.newBuilder()
                .setQuery(query)
                .setCommitNow(false)
                .build();
        DgraphProto.Response response1 = txn.doRequest(request1);

        Ws1Response peopleResponse = gson.fromJson(response1.getJson().toStringUtf8(), Ws1Response.class);

        commitTransaction(txn);

        if (!peopleResponse.person1.isEmpty() &&
                !peopleResponse.person2.isEmpty() &&
                peopleResponse.person1.size() == peopleResponse.person2.size()) {

            for (int i = 0; i < peopleResponse.person1.size(); i++) {
                if (Integer.parseInt(peopleResponse.person1.get(i).value) +
                        Integer.parseInt(peopleResponse.person2.get(i).value) <= 0) {

                    p1id = Long.parseLong(peopleResponse.person1.get(i).id);
                    p1value = Long.parseLong(peopleResponse.person1.get(i).value);
                    p2id = Long.parseLong(peopleResponse.person2.get(i).id);
                    p2value = Long.parseLong(peopleResponse.person2.get(i).value);

                    return ImmutableMap.of(
                            "p1id", p1id,
                            "p1value", p1value,
                            "p2id", p2id,
                            "p2value", p2value);
                }
            }
        }

        return ImmutableMap.of();
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
    }
}
