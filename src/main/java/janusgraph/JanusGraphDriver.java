package janusgraph;

import driver.TestDriver;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

import java.util.Map;

public class JanusGraphDriver extends TestDriver {

    private JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-cassandra-es-server.properties");

    @Override
    public JanusGraphTransaction startTransaction() throws Exception {
        return graph.newTransaction();
    }

    @Override
    public void commitTransaction(Object tt) throws Exception {
        JanusGraphTransaction transaction = (JanusGraphTransaction) tt;
        transaction.commit();
    }

    @Override
    public void abortTransaction(Object tt) throws Exception {
        JanusGraphTransaction transaction = (JanusGraphTransaction) tt;
        transaction.rollback();
    }

    @Override
    public void close() throws Exception {
        graph.close();
    }


    @Override
    public void nukeDatabase() {
        try {
            JanusGraphTransaction transaction = startTransaction();
            GraphTraversalSource g = transaction.traversal();
            g.V().drop().iterate();
            commitTransaction(transaction);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void g0Init() {

    }

    @Override
    public void g1aInit() {

    }

    @Override
    public void g1bInit() {

    }

    @Override
    public void g1cInit() {

    }

    @Override
    public void impInit() {

    }

    @Override
    public void pmpInit() {

    }

    @Override
    public void otvInit() {

    }

    @Override
    public void frInit() {

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
    public Map<String, Object> ws2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> ws1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> fr2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> fr1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> otv2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> otv1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> pmp2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> pmp1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> imp2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> imp1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1c2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1c1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1b2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1b1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1a2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1a1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g0(Map parameters) {
        return null;
    }


    @Override
    public Object runQuery(Object tt, String querySpecification, Object o) throws Exception {
        return null;
    }

}
