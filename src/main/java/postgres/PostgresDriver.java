package postgres;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgresDriver extends TestDriver<Connection, Map<String, Object>, ResultSet> {

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
        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);
        return conn;
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
    public ResultSet runQuery(Connection tt, String querySpecification, Map<String, Object> stringStringMap) throws Exception {
        ResultSet rs = null;
        querySpecification = substituteParameters(querySpecification, stringStringMap);
        // don't close this staement before returning (e.g. in a try-with-resources block) so we can retrive elements of the resultset
        Statement st = tt.createStatement();
        rs = st.executeQuery(querySpecification);
        return rs;
    }

    protected List<Object> toObjectList(Object o) {
        Object[] ol = (Object[])o;
        List<Object> lo = new ArrayList<Object>();
        for(Object i: ol) {
            lo.add(i);
        }
        return lo;
    }

    public String substituteParameters(String querySpecification, Map<String, Object> stringStringMap) {
        // substitute parameters
        if (stringStringMap != null) {
            for (Map.Entry<String, Object> param : stringStringMap.entrySet()) {
                querySpecification = querySpecification.replace("$" + param.getKey(), param.getValue().toString());
            }
        }
        return querySpecification;
    }

    protected void executeUpdates(String[] commands) {
        executeUpdates(commands, true);
    }

    protected void executeUpdates(String[] commands, boolean doCommit) {
        try (Connection conn = startTransaction(); Statement st = conn.createStatement()) {
            for (String sql : commands) {
                st.executeUpdate(sql);
            }
            if (doCommit) {
                commitTransaction(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void executeUpdates(String[] commands, Map<String, Object> parameters, boolean doCommit) {
        for(int i=0; i<commands.length; i++) {
            commands[i]=substituteParameters(commands[i], parameters);
        }
        executeUpdates(commands, doCommit);
    }



    protected void createSchema() {
        executeUpdates(PostgresQueries.tablesCreate);
    }

    @Override
    public void nukeDatabase() {
        // drop all the tables
        executeUpdates(PostgresQueries.tablesClear);
    }

    @Override
    public void atomicityInit() {
        createSchema();
        executeUpdates(PostgresQueries.atomicityInit);
    }

    @Override
    public void atomicityC(Map<String, Object> parameters) {
        executeUpdates(PostgresQueries.atomicityCTx, parameters, true);
    }

    @Override
    public void atomicityRB(Map<String, Object> parameters) {

    }

    @Override
    public Map<String, Object> atomicityCheck() {
        try (Connection conn = startTransaction()) {
            ResultSet rs = runQuery(conn, PostgresQueries.atomicityCheck, null);
            rs.next();

            final long numPersons = rs.getLong(1);
            final long numNames = rs.getLong(2);
            final long numEmails = rs.getLong(3);

            return ImmutableMap.of("numPersons", numPersons, "numNames", numNames, "numEmails", numEmails);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void g0Init() {
        createSchema();
        executeUpdates(PostgresQueries.g0Init);
    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        executeUpdates(PostgresQueries.g0, parameters, true);

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            ResultSet rs = runQuery(conn, PostgresQueries.g0check, parameters);
            rs.next();

            List<Object> p1VersionHistory = toObjectList((Object[])rs.getArray(1).getArray());
            List<Object> kVersionHistory  = toObjectList((Object[])rs.getArray(2).getArray());
            List<Object> p2VersionHistory = toObjectList((Object[])rs.getArray(3).getArray());

            return ImmutableMap.of("p1VersionHistory", p1VersionHistory, "kVersionHistory", kVersionHistory, "p2VersionHistory", p2VersionHistory);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
