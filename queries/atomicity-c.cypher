MATCH
  (p1:Person {id: $person1Id})
CREATE
  (p2:Person)
CREATE
  (p1)-[k:KNOWS]->(p2)
SET
  p1.email = p1.email + $newEmail,
  p2.id = $person2Id,
  k.creationDate = $creationDate
