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
        this.debug = false;
        
        ApiClient defaultApiClient = Configuration.getDefaultApiClient();
        defaultApiClient.setBasePath(this.endpoint);
        Configuration.setDefaultApiClient(defaultApiClient);
        
        this.apiInstance = new QueryApi();
        this.defaultApi = new DefaultApi();
    }

    private Map<String, String> toStringMap(Map<String, Object> input) {
        Map<String,String> newMap = input.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (String)e.getValue()));
        return newMap;
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

    @Override
    public Map<String, Object> runQuery(Integer x, String querySpecification, Map<String, String> queryParameters) {
        QueryResponse queryResponse = new QueryResponse();
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
        if (results != null) {
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
        param1 = ImmutableMap.<String, String>of("id", "1", "name", "Alice", "emails", "(\"alice@aol.com\")");
        runQuery("insertPerson", param1);
        param2 = ImmutableMap.<String, String>of("id", "2", "name", "Bob", "emails", "(\"bob@hotmail.com\", \"bobby@yahoo.com\")");
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
        return runQuery("atomicityCheck", ImmutableMap.<String, String>of());
    }

    @Override
    public void g0Init() {}

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // G1a Intermediate Reads

    @Override
    public void g1aInit() {}

    @Override
    public Map<String, Object> g1aW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> g1aR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // G1b Intermediate Reads

    @Override
    public void g1bInit() {

    }

    @Override
    public Map<String, Object> g1bW(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    @Override
    public Map<String, Object> g1bR(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
    }

    // G1c Circular Information Flow

    @Override
    public void g1cInit() {}

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        return ImmutableMap.<String, Object>of();
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
