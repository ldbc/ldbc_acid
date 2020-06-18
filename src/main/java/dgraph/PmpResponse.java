package dgraph;

import java.util.List;

public class PmpResponse {
    List<PmpInner> all;
}

class PmpInner {
    List<PeopleCount> liked_by;
}

class PeopleCount {
    String totalPeople;
}
