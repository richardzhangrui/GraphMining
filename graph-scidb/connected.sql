-- We hard code the number of nodes in graphs, so we give an example of 100 nodes in sample code.
-- change raw data to scidb format file <src, des>, with delimiter SPACE and two columns.
csv2scidb -p NN -d ' ' <./new.txt> ./new3.scidb
create array edge<src: uint64, des: uint64, e:uint64>[i=1:100, 100, 0];
-- loading data to array edge
load edge from './new3.scidb'

-- create a adjacent matrix
create array connect<e:uint64 null default null>[src=1:100,100,0, des=1:100,100,0];
redimension_store(edge, connect, count(*) as e);

-- create initial label for each node
create array label<l:uint64 null default null>[src=1:100,100,0];
store(build(label, src), label);

-- create a temp label array for each node
create array tmplabel<la:uint64 null default null>[src=1:100,100,0, des=1:100,100,0];

-- Here is a loop, we write loop in a file, and use "iquery -af input" to do iteration
-- FOR
	select label.l into tmplabel from connect join label on connect.des=label.src;		
	select min(tmplabel.la) into label from tmplabel group by tmplabel.src;
-- END FOR