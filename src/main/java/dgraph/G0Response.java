package dgraph;

import java.util.List;

public class G0Response {
    List<G0ResponseP1Inner> all;
    G0Response() {}
}

class G0ResponseP1Inner {
    List<Long> p1VersionHistory;
    List<G0ResponseKnows> knows;
}

class G0ResponseKnows {
    List<Long> versionHistory;
    List<Long> p2VersionHistory;
}
