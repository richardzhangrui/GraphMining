create or replace function connected(text,n Integer) 
	returns table(label Integer, cnt bigint) as $$
	DECLARE
		cnt Integer := 0;
	BEGIN
		EXECUTE 'create temp table tempgraph as select * from '||$1||'';
		create temp table if not exists labels(id Integer, label Integer);
		insert into tempgraph select dst,src from tempgraph;
		insert into labels select src as id,src as label from 
			(select src from tempgraph union select dst from tempgraph) as t;
		create temp table if not exists templabels(id Integer, label Integer);
		while  cnt < n loop
			raise notice 'enterloop';
			delete from templabels;
			insert into templabels 
			(select tempgraph.src as id,min(labels.label) as label 
			from tempgraph join labels on tempgraph.dst=labels.id group by tempgraph.src);
			update labels set label = templabels.label from templabels where labels.id = templabels.id and labels.label > templabels.label;
			cnt := cnt + 1;
		end loop;
		return query select labels.label,count(*) as cnt from labels group by labels.label;
		drop table tempgraph;
		drop table templabels;
		drop table labels;
		
	END
	$$ language plpgsql;
