package tigergraph;

import com.google.common.collect.ImmutableMap;
import com.google.gson.internal.LinkedTreeMap;
import driver.TestDriver;
import io.github.karol_brejna_i.tigergraph.restppclient.api.DefaultApi;
import io.github.karol_brejna_i.tigergraph.restppclient.api.QueryApi;
import io.github.karol_brejna_i.tigergraph.restppclient.invoker.ApiClient;
import io.github.karol_brejna_i.tigergraph.restppclient.invoker.ApiException;
import io.github.karol_brejna_i.tigergraph.restppclient.invoker.Configuration;
import io.github.karol_brejna_i.tigergraph.restppclient.model.HelloResponse;
import io.github.karol_brejna_i.tigergraph.restppclient.model.QueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

// No transaction in TigerGraph
public class TigerGraphDriver extends TestDriver<Integer, Map<String, String>, Map<String, Object>> {

    private static final int MAX_TIMEOUT = 600000;

    private final String graphName;
    private final QueryApi apiInstance;
    private final DefaultApi defaultApi;
    private final boolean debug;

    public TigerGraphDriver(String endpoint, String graphName) {
        this.graphName = graphName;
        this.debug = true;

        ApiClient defaultApiClient = Configuration.getDefaultApiClient();
        defaultApiClient.setBasePath(endpoint)
                .setConnectTimeout(MAX_TIMEOUT).setReadTimeout(MAX_TIMEOUT).setWriteTimeout(MAX_TIMEOUT);
        Configuration.setDefaultApiClient(defaultApiClient);

        this.apiInstance = new QueryApi();
        this.defaultApi = new DefaultApi();
    }

    private Map<String, String> toStringMap(Map<String, Object> input) {
        ImmutableMap.Builder<String, String> tempMap = ImmutableMap.builder();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            tempMap.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return tempMap.build();
    }

    @Override
    public Integer startTransaction() throws Exception {
        return 0;
    }

    @Override
    public void commitTransaction(Integer tt) throws Exception {
    }

    @Override
    public void abortTransaction(Integer tt) throws Exception {
    }

    @Override
    public void close() throws Exception {
    }

    public Map<String, Object> runQuery(String querySpecification, Map<String, String> queryParameters) {
        return runQuery(0, querySpecification, queryParameters);
    }

    private String mapToString(Map<String, String> map) {
        if (isMapNullOrEmpty(map)) return "";

        StringBuilder mapAsString = new StringBuilder("{");
        for (String key : map.keySet()) {
            mapAsString.append(key).append(":").append(map.get(key)).append(", ");
        }
        mapAsString.delete(mapAsString.length() - 2, mapAsString.length()).append("}");

        return mapAsString.toString();
    }

    private boolean isMapNullOrEmpty(Map<String, String> map) {
        return map == null || map.size() == 0;
    }

    private boolean isDebugMode() {
        return debug;
    }

    @Override
    public Map<String, Object> runQuery(Integer x, String querySpecification, Map<String, String> queryParameters) {

        if (isDebugMode()) {
            System.out.println(querySpecification + ":" + mapToString(queryParameters));
        }

        QueryResponse queryResponse = getQueryResponse(querySpecification, queryParameters);

        return extractAndPrintResultsFromResponse(queryResponse);
    }

    private LinkedTreeMap<String, Object> extractAndPrintResultsFromResponse(QueryResponse queryResponse) {
        List<Object> results = queryResponse.getResults();
        LinkedTreeMap<String, Object> result = null;

        printResults(results);

        if (results != null && results.size() > 0) {
            result = (LinkedTreeMap<String, Object>) results.get(0);
        }

        return result;
    }

    private void printResults(List<Object> results) {
        if (isDebugMode()) {
            System.out.println("Num of results: " + results.size());
            for (int i = 0; i < results.size(); i++) {
                String trail = i == results.size() - 1 ? "\n" : ",";
                System.out.print(results.get(i) + trail);
            }
        }
    }

    private QueryResponse getQueryResponse(String querySpecification, Map<String, String> queryParameters) {
        QueryResponse queryResponse = new QueryResponse();

        try {
            queryResponse = apiInstance.runInstalledQueryGet(this.graphName, querySpecification,
                    null, null, MAX_TIMEOUT, null,
                    null, null,
                    queryParameters);
        } catch (ApiException e) {
            System.err.println("Exception when calling " + querySpecification);
            e.printStackTrace();
        }
        return queryResponse;
    }

    @Override
    public void nukeDatabase() {
        deleteVerticesByType("Person");
        deleteVerticesByType("Post");
    }

    private void deleteVerticesByType(String type) {
        try {
            HelloResponse result = defaultApi.graphGraphNameDeleteByTypeVerticesVertexTypeDelete(this.graphName, type);
            System.out.println(result);
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void atomicityInit() {
        Map<String, String> param1, param2;
        param1 = ImmutableMap.of("id", "1", "name", "Alice", "emails[0]", "alice@aol.com");
        runQuery("insertPerson", param1);
        param2 = ImmutableMap.of("id", "2", "name", "Bob", "emails[0]", "bob@hotmail.com", "emails[1]", "bobby@yahoo.com");
        runQuery("insertPerson", param2);
    }

    @Override
    public void atomicityC(Map<String, Object> parameters) {
        runQuery("atomicityC", toStringMap(parameters));
    }

    @Override
    public void atomicityRB(Map<String, Object> parameters) {
        runQuery("atomicityRB", toStringMap(parameters));
    }

    @Override
    public Map<String, Object> atomicityCheck() {
        Map<String, Object> results = runQuery("atomicityCheck", ImmutableMap.of());
        results.put("numPersons", (long) (double) results.get("numPersons"));
        results.put("numNames", (long) (double) results.get("numNames"));
        results.put("numEmails", (long) (double) results.get("numEmails"));
        return results;
    }

    @Override
    public void g0Init() {
        runQuery("initKnow", ImmutableMap.of());
    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        runQuery("g0", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        return runQuery("g0check", toStringMap(parameters));
    }

    // G1a Intermediate Reads

    @Override
    public void g1aInit() {
        runQuery("g1Init", ImmutableMap.of("id", "1", "version", "1"));
    }

    @Override
    public Map<String, Object> g1aW(Map<String, Object> parameters) {
        runQuery("g1aW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1aR(Map<String, Object> parameters) {
        runQuery("g1R", toStringMap(parameters));
        return ImmutableMap.of();
    }

    // G1b Intermediate Reads

    @Override
    public void g1bInit() {
        runQuery("g1Init", ImmutableMap.of("id", "1", "version", "99"));
    }

    @Override
    public Map<String, Object> g1bW(Map<String, Object> parameters) {
        runQuery("g1bW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1bR(Map<String, Object> parameters) {
        runQuery("g1R", toStringMap(parameters));
        return ImmutableMap.of();
    }

    // G1c Circular Information Flow

    @Override
    public void g1cInit() {
        runQuery("g1Init", ImmutableMap.of("id", "1", "version", "0"));
        runQuery("g1Init", ImmutableMap.of("id", "2", "version", "0"));
    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        Map<String, Object> results = runQuery("g1c", toStringMap(parameters));
        results.put("person2Version", (long) (double) results.get("person2Version"));
        return results;
    }

    // IMP

    @Override
    public void impInit() {
        runQuery("g1Init", ImmutableMap.of("id", "1", "version", "1"));
    }

    @Override
    public Map<String, Object> impW(Map<String, Object> parameters) {
        runQuery("impW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> impR(Map<String, Object> parameters) {
        Map<String, Object> results = runQuery("impR", toStringMap(parameters));
        results.put("firstRead", (long) (double) results.get("firstRead"));
        results.put("secondRead", (long) (double) results.get("secondRead"));
        return results;
    }

    // PMP

    @Override
    public void pmpInit() {
        runQuery("g1Init", ImmutableMap.of("id", "1", "version", "1"));
    }

    @Override
    public Map<String, Object> pmpW(Map<String, Object> parameters) {
        runQuery("pmpW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> pmpR(Map<String, Object> parameters) {
        Map<String, Object> results = runQuery("pmpR", toStringMap(parameters));
        results.put("firstRead", (long) (double) results.get("firstRead"));
        results.put("secondRead", (long) (double) results.get("secondRead"));
        return results;
    }

    // OTV

    @Override
    public void otvInit() {
        runQuery("initCycle", ImmutableMap.of());
    }

    @Override
    public Map<String, Object> otvW(Map<String, Object> parameters) {
        runQuery("otvW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> otvR(Map<String, Object> parameters) {
        return runQuery("otvR", toStringMap(parameters));
    }

    // FR

    @Override
    public void frInit() {
        runQuery("initCycle", ImmutableMap.of());
    }

    @Override
    public Map<String, Object> frW(Map<String, Object> parameters) {
        runQuery("otvW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> frR(Map<String, Object> parameters) {
        return runQuery("otvR", toStringMap(parameters));
    }

    // LU

    @Override
    public void luInit() {
        runQuery("insertPerson", ImmutableMap.of("id", "1", "name", ""));
    }

    @Override
    public Map<String, Object> luW(Map<String, Object> parameters) {
        runQuery("luW", toStringMap(parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> luR(Map<String, Object> parameters) {
        Map<String, Object> results = runQuery("luR", toStringMap(parameters));
        results.put("numKnowsEdges", (long) (double) results.get("numKnowsEdges"));
        results.put("numFriendsProp", (long) (double) results.get("numFriendsProp"));
        return results;
    }

    // WS

    @Override
    public void wsInit() {
        for (int i = 1; i <= 10; i++) {
            runQuery("wsInit", ImmutableMap.of("person1Id", Integer.toString(2 * i - 1), "person2Id", Integer.toString(2 * i)));
        }
    }

    @Override
    public Map<String, Object> wsW(Map<String, Object> parameters) {
        long personId = new Random().nextBoolean() ?
                (long) parameters.get("person1Id") :
                (long) parameters.get("person2Id");
        ImmutableMap<String, String> param =
                new ImmutableMap.Builder()
                        .putAll(toStringMap(parameters))
                        .put("personId", String.valueOf(personId))
                        .build();
        runQuery("wsW", param);
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> wsR(Map<String, Object> parameters) {
        Map<String, Object> results = runQuery("wsR", toStringMap(parameters));
        if (results.get("result") instanceof ArrayList) return ImmutableMap.of();
        LinkedTreeMap<String, Object> record = (LinkedTreeMap<String, Object>) results.get("result");
        return record;
    }
}
