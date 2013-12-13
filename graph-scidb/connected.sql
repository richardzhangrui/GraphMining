-- change raw data to scidb format file <src, des>
csv2scidb -p NN -d ' ' <./new.txt> ./new3.scidb

-- loading data to array
load edge from './new3.scidb'

-- create a adjacent matrix
create array connect<e:uint64 null default null>[src=1:100,100,0, des=1:100,100,0];
redimension_store(edge, connect, count(*) as e);

-- create initial label for each node
create array label<l:uint64 null default null>[src=1:100,100,0];
store(build(label, src), label);
create array tmplabel<la:uint64 null default null>[src=1:100,100,0, des=1:100,100,0];
-- FOR
	select label.l into tmplabel from connect join label on connect.des=label.src;		
	select min(tmplabel.la) into label from tmplabel group by tmplabel.src;
-- END FOR