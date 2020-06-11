MATCH (p1:Person {id: $personId})
CREATE (p2:Person)
//CREATE (p1)-[:KNOWS]->(p2)
SET
  p1.numFriends = p1.numFriends + 1
