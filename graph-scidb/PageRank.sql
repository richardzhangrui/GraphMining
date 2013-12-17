-- We hard code the number of nodes in graphs, so we give an example of 100 nodes in sample code.
-- change raw data to scidb format file <src, des, edge>, with delimiter SPACE
csv2scidb -p NNN -d ' ' <./new.txt> ./new2.scidb

-- create array edge that stores <src, des> pair.
create array edge<src: uint64, des: uint64, e:uint64>[i=0:99, 100, 0];

-- Loading phase
load edge from './new2.scidb'

-- create adjacent matrix from array edge, with dimension bounded by 99, and chunk size 100, and 0 overlap
create Array fix<e: uint64>[des=0:99,100,0, src=0:99,100,0];

-- redimension the edge array to adjacent matrix with two dimensions
redimension_store(edge, fix);

-- create initial pagerank value for each node
create array pagerank<val:uint64>[i(uint64)=100,100,0, j(uint64)=1,1,0];
store(build(pagerank,1),pagerank);

-- Here is a loop, we write loop in a file, and use "iquery -af input" to do iteration
-- FOR iterations
-- The multiply of matrix and vector. e.g. PageRank` = AdjMatrix * PageRank
multiply(fix, pagerank);
-- END FOR