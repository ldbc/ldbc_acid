MATCH (p:Person {id: $personId})
OPTIONAL MATCH (p)-[k:KNOWS]->()
WITH p, count(k) AS numKnowsEdges
RETURN numKnowsEdges,
       p.numFriends AS numFriendsProp
