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


  #text(size: 14pt)[Advanced Database - 2026]

  #text(size: 10pt)[*Léna Beust and Iléane Crocq*]
]


This report will explain the steps we followed for this second laboratory on Neo4J.


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
The entire database was streamed directly from the following link : "http://vmrum.isc.heia-fr.ch/files/DBLP-Citation-network-V18.jsonl".
...

== Importing the database with java
...

= #text(fill: red)[Loading time]
Loading time can be retrieve in the maven logs of the streamer pod. We achieve a total running time of *?* hours to load *N?* articles and *K?* authors.
