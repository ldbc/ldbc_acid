package tigergraph;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import com.google.gson.internal.LinkedTreeMap;

import io.github.karol_brejna_i.tigergraph.restppclient.api.QueryApi;
import io.github.karol_brejna_i.tigergraph.restppclient.api.DefaultApi;
import io.github.karol_brejna_i.tigergraph.restppclient.invoker.ApiException;
import io.github.karol_brejna_i.tigergraph.restppclient.invoker.ApiClient;
import io.github.karol_brejna_i.tigergraph.restppclient.invoker.Configuration;
import io.github.karol_brejna_i.tigergraph.restppclient.model.QueryResponse;
import io.github.karol_brejna_i.tigergraph.restppclient.model.HelloResponse;

import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import java.io.*;

// No transaction in TigerGraph
public class TigergraphDriver extends TestDriver<Integer, Map<String, String>, Map<String, Object>>{
    private final String endpoint;
    private final String graphName;
    private final QueryApi apiInstance;
    private final DefaultApi defaultApi;
    private final boolean debug;
    public TigergraphDriver(String endpoint, String graphName) {
        this.endpoint = endpoint;
        this.graphName = graphName;
        this.debug = true;
        
        ApiClient defaultApiClient = Configuration.getDefaultApiClient();
        defaultApiClient.setBasePath(this.endpoint);
        Configuration.setDefaultApiClient(defaultApiClient);
        
        this.apiInstance = new QueryApi();
        this.defaultApi = new DefaultApi();
    }

    private Map<String, String> toStringMap(Map<String, Object> input) {
        ImmutableMap.Builder<String, String> mymap = ImmutableMap.<String, String>builder();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            mymap.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return mymap.build();
    }
    
    @Override
    public Integer startTransaction() throws Exception {return 0;}
    @Override
    public void commitTransaction(Integer tt) throws Exception {}
    @Override
    public void abortTransaction(Integer tt) throws Exception {}
    @Override
    public void close() throws Exception {}

    public Map<String, Object> runQuery(String querySpecification, Map<String, String> queryParameters) {
        return runQuery(0, querySpecification, queryParameters);
    };

    private String mapToString(Map<String, String> map) {
        if (map == null || map.size()==0) return "";
        StringBuilder mapAsString = new StringBuilder("{");
        for (String key : map.keySet()) {
            mapAsString.append(key + ":" + map.get(key) + ", ");
        }
        mapAsString.delete(mapAsString.length()-2, mapAsString.length()).append("}");
        return mapAsString.toString();
    }

    @Override
    public Map<String, Object> runQuery(Integer x, String querySpecification, Map<String, String> queryParameters) {
        QueryResponse queryResponse = new QueryResponse();
        if (this.debug) {
            System.out.println(querySpecification + ":" + mapToString(queryParameters));
        }
        try {
            queryResponse = apiInstance.runInstalledQueryGet(this.graphName, querySpecification, 
            null, null, null, null, null, null,
            queryParameters);
        } catch (ApiException e) {
            System.err.println("Exception when calling " + querySpecification);
            e.printStackTrace();
        }
        
        List<Object> results = queryResponse.getResults();
        LinkedTreeMap<String, Object> result = null;

        if (this.debug) {
            System.out.println("Num of results" + results.size() + ":");
            for(int i=0; i<results.size(); i++){
                String trail = i == results.size() - 1 ? "\n" : ",";
                System.out.print(results.get(i) + trail);
            }
        }

        if (results != null && results.size() > 0) {
            result = (LinkedTreeMap<String, Object>) results.get(0);
        }
        return result;
    }

    @Override
    public void nukeDatabase() {
        try {
            HelloResponse result = defaultApi.graphGraphNameDeleteByTypeVerticesVertexTypeDelete(this.graphName,"Person");
            System.out.println(result);
        } catch (ApiException e) {
            e.printStackTrace();
        }
        try {
            HelloResponse result = defaultApi.graphGraphNameDeleteByTypeVerticesVertexTypeDelete(this.graphName,"Post");
            System.out.println(result);
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void atomicityInit() {
        Map<String,String> param1, param2;
        param1 = ImmutableMap.<String, String>of("id", "1", "name", "Alice", "emails[0]", "alice@aol.com");
        runQuery("insertPerson", param1);
        param2 = ImmutableMap.<String, String>of("id", "2", "name", "Bob", "emails[0]", "bob@hotmail.com", "emails[1]", "bobby@yahoo.com");
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
        Map<String, Object> results = runQuery("atomicityCheck", ImmutableMap.<String, String>of());
        results.put("numPersons", (long)(double)results.get("numPersons"));
        results.put("numNames", (long)(double)results.get("numNames"));
        results.put("numEmails", (long)(double)results.get("numEmails"));
        return results;
    }

    @Override
    public void g0Init() {
        runQuery("initKnow", ImmutableMap.<String, String>of());
    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        runQuery("g0", toStringMap(parameters));
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        return runQuery("g0check", toStringMap(parameters));
    }

    // G1a Intermediate Reads

    @Override
    public void g1aInit() {
        runQuery("g1Init", ImmutableMap.<String, String>of("id","1","version","1"));
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
        runQuery("g1Init", ImmutableMap.<String, String>of("id","1","version","99"));
    }

    @Override
    public Map<String, Object> g1bW(Map<String, Object> parameters) {
        runQuery("g1bW", toStringMap(parameters));
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> g1bR(Map<String, Object> parameters) {
        runQuery("g1R", toStringMap(parameters));
        return ImmutableMap.of();
    }

    // G1c Circular Information Flow

    @Override
    public void g1cInit() {
        runQuery("g1Init", ImmutableMap.<String, String>of("id","1","version","0"));
        runQuery("g1Init", ImmutableMap.<String, String>of("id","2","version","0"));
    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        Map<String, Object> results = runQuery("g1c", toStringMap(parameters));
        results.put("person2Version", (long)(double)results.get("person2Version"));
        return results;
    }

    // IMP

    @Override
    public void impInit() {}

    @Override
    public Map<String, Object> impW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> impR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // PMP

    @Override
    public void pmpInit() {}

    @Override
    public Map<String, Object> pmpW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> pmpR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // OTV

    @Override
    public void otvInit() {}

    @Override
    public Map<String, Object> otvW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> otvR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // FR

    @Override
    public void frInit() {}

    @Override
    public Map<String, Object> frW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> frR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // LU

    @Override
    public void luInit() {}

    @Override
    public Map<String, Object> luW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> luR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // WS

    @Override
    public void wsInit() {}

    @Override
    public Map<String, Object> wsW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> wsR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }
}
