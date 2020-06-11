MATCH (p1:Person {id: $person1Id})
SET p1.email = p1.email + $newEmail

WITH count(*) AS dummy

OPTIONAL MATCH (p2:Person {id: $person2Id})
WITH count(p2) AS p2c

// if no persons are found, crash the transaction by dividing by zero
WITH 1/(CASE WHEN p2c > 0 THEN 1 ELSE 0 END) AS punisher

CREATE (p2:Person)
SET p2.id = $person2Id
