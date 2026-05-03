#set page(margin: 1.5cm)

#show raw.where(block: false): box.with(
  fill: luma(240),
  inset: (x: 3pt, y: 0pt),
  outset: (y: 3pt),
  radius: 2pt,
)
#show raw.where(block: true): block.with(
  inset: 10pt,
  radius: 2pt,
  stroke: 1pt + luma(200),
)

#set page(header: none, margin: 1.5cm)
#set text(font: "Cantarell", size: 12pt)

#align(center)[

  #text(fill: red, size: 25pt)[Laboratory 2 - Diving deeper with Neo4j]


  #text(size: 14pt)[Advanced Database - 04/03/2026]

  #text(size: 10pt)[*Léna Beust and Iléane Crocq*]
]


This report explains the steps we followed for this second laboratory on Neo4J.


= #text(fill: red)[Group informations]
Here are the important informations about our group :
- Groupe ID : *???????*
- Namespace : *beu-cro-adv-daba-26*
- ID of the pod containing Neo4j : *neo4j-7b94c6b485-7ln46*
- Neo4j credentials :
  - username : *neo4j*
  - password : *test_neo4j*
- ID of the pod whose logs prove that the loading actually happened : *streamer-77d77b4f9f*
- link to the repository with all the useful material : * https://github.com/Lena-Beust/mse-advDaBa-2022 *

= #text(fill: red)[Followed steps]

== Streaming the database
The entire database was streamed directly from the following link : "http://vmrum.isc.heia-fr.ch/files/DBLP-Citation-network-V18.jsonl", by batches of size 1000 in order to manage its significant amount of data.

== Importing the database with java
Firstly, we cleaned the articles extracted from the database because some authors were missing an id in the database. To solve this problem, we cerated an id which is either : - The hash of the name of the author in lowercase (if present).
- unknown_\<articleID\>_i where i indicates that this author is the ith author of the article. If the name of the author is absent (which doesn't happen in the database).
We also added the non-requested properties : year, venue, doi and n_citation in addition to the title which are set to "" by default if absent (except for year being set to 0 by default).
Eventualy, we loaded the data base in neo4J, by the transaction : ```java
session.writeTransaction(tx -> {
		    tx.run("""
		            UNWIND $batch AS row

		            MERGE (a:ARTICLE {_id: row.id})
		            SET a.title = row.title,
		                a.year = row.year,
		                a.venue = row.venue,
		                a.doi = row.doi,
		                a.n_citation = row.n_citation

		            WITH a, row

		            UNWIND row.authors AS author
		            MERGE (au:AUTHOR {_id: author.id})
		            SET au.name = author.name
		            MERGE (au)-[:AUTHORED]->(a)

		            WITH a, row

		            UNWIND row.references AS refId
		            MERGE (ref:ARTICLE {_id: refId})
		            MERGE (a)-[:CITE]->(ref)
		            """, parameters("batch", rows));
= #text(fill: red)[Loading time]
```
= #text(fill: red)[Loading time]
The loading time can be retrieved in the maven logs of the streamer pod. We achieved a total running time of *?* hours to load *N?* articles and *K?* authors.
