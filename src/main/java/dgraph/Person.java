package dgraph;

import java.util.List;

public class Person {
    String uid;
    String id;
    String name;
    String version;
    List<String> emails;
    List<Person> knows;
    Person() {}
}
