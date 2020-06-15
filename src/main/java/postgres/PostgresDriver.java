package postgres;

import driver.TestDriver;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class PostgresDriver extends TestDriver<Connection, Map<String, String>, ResultSet> {

    protected PGConnectionPoolDataSource ds;

    public PostgresDriver() {
    }

    public void initDataSource(String host, int port, String username, String password, String dbName) {
        ds = new PGConnectionPoolDataSource();
        ds.setDefaultAutoCommit(false);
        ds.setDatabaseName(dbName);
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setUser(username);
        ds.setPassword(password);
    }

    public String getPGVersion() throws SQLException {
        Connection conn = ds.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select version()");
        rs.next();
        return rs.getString(1);
    }

    @Override
    public Connection startTransaction() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public void commitTransaction(Connection tt) throws SQLException {
        tt.commit();
    }

    @Override
    public void abortTransaction(Connection tt) throws SQLException {
        tt.rollback(); // rollback VS. abort?
    }

    @Override
    public ResultSet runQuery(Connection tt, String querySpecification, Map<String, String> stringStringMap) {
        return null;
    }

    @Override
    public void nukeDatabase() {
        // TODO: delete all rows from the DB
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

    }

}
