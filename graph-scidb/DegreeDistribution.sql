-- We hard code the number of nodes in graphs, so we give an example of 100 nodes in sample code.
-- change raw data to scidb format file <src, des>
csv2scidb -p NNN -d ' ' <./new.txt> ./new1.scidb

-- create array edge that stores <src, des> pair
create array edge<src: uint64, des: uint64, e:uint64>[i=0:99, 100, 0];

-- Loading phase
load edge from './new1.scidb'

-- create array of outdegree for each source node
create Array outdegree<out: uint64 null default null>[src=0:99,100,0];

-- redimension array edge to array outdegree
redimension_store(edge, outdegree, count(*) as out);

-- create array for final distribution
create Array finaldis<dis: uint64 null>[out(uint64)=*,100,0];

-- redimension array outdegree to array finaldis
redimension_store(outdegree, finaldis, count(*) as dis);
