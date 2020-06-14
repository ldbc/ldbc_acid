package dgraph;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import driver.TestDriver;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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
        // TODO
    }

    @Override
    public void g0Init() {

    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
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
    public Void lu1() {
        return null;
    }

    @Override
    public long lu2() {
        return 0;
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
