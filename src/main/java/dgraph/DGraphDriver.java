//package dgraph;
//
//import driver.TestDriver;
//import io.dgraph.DgraphClient;
//import io.dgraph.DgraphGrpc;
//import io.dgraph.DgraphProto;
//import io.dgraph.Transaction;
//import io.grpc.ManagedChannel;
//import io.grpc.ManagedChannelBuilder;
//
//import java.util.Map;
//
//public class DGraphDriver extends TestDriver<Transaction, Map<String, String>, DgraphProto.Response> {
//
//    protected DgraphClient client;
//
//    public DGraphDriver() {
//        ManagedChannel channel1 = ManagedChannelBuilder
//                .forAddress("localhost", 9080)
//                .usePlaintext().build();
//        DgraphGrpc.DgraphStub stub1 = DgraphGrpc.newStub(channel1);
//
//        client = new DgraphClient(stub1);
//    }
//
//    @Override
//    public void close() throws Exception {
//        // do nothing
//    }
//
//    @Override
//    public Transaction startTransaction() {
//        return client.newTransaction();
//    }
//
//    @Override
//    public DgraphProto.Response runQuery(Transaction tt, String querySpecification, Map<String, String> stringStringMap) {
//        return null;
//    }
//
//    @Override
//    public void commitTransaction(Transaction tt) {
//        tt.commit();
//    }
//
//    @Override
//    public void abortTransaction(Transaction tt) {
//        tt.discard();
//    }
//
//    @Override
//    protected void luTx1(Transaction tt) {
//
//    }
//
//    @Override
//    protected void luInit(Transaction tt) {
//
//    }
//
//    @Override
//    public long luTx2() {
//
//        return 0;
//    }
//
//}
