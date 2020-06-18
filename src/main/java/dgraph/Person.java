package dgraph;

import java.util.List;

public class Person {
    String uid;
    String id;
    String name;
    String version;
    String value;
    List<String> emails;
    List<Person> knows;
    String numKnowsEdges;
    String numFriendsProp;
    Person() {}
}
