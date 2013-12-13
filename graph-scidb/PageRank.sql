-- change raw data to scidb format file <src, des, edge>
csv2scidb -p NNN -d ' ' <./new.txt> ./new2.scidb

-- create array edge that stores <src, des> pair
create array edge<src: uint64, des: uint64, e:uint64>[i=0:99, 100, 0];

-- Loading phase
load edge from './new2.scidb'

-- create adjacent matrix from array edge
create Array fix<e: uint64>[des=0:99,100,0, src=0:99,100,0];

redimension_store(edge, fix);

-- create initial pagerank value for each node
create array pagerank<val:uint64>[i(uint64)=100,100,0, j(uint64)=1,1,0];
store(build(pagerank,1),pagerank);
-- FOR iterations
multiply(fix, pagerank);
-- END FOR