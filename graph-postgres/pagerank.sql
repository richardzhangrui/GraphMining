CREATE OR REPLACE FUNCTION calpagerank() RETURNS void AS $$

	DECLARE 

		flag BOOLEAN := true;

		nodeNum Integer;

		delta double precision;

	BEGIN

		DROP TABLE IF EXISTS pagerank;

		DROP TABLE IF EXISTS edgeWithOuterDegree;

		SELECT COUNT(*) INTO nodeNum FROM (SELECT DISTINCT src FROM edge

							  UNION

							  SELECT DISTINCT des FROM edge) foo;

		CREATE TABLE pagerank AS SELECT * , 1.00/nodeNum AS pr FROM (SELECT DISTINCT src FROM edge

							  UNION

							  SELECT DISTINCT des FROM edge) foo;

		CREATE TABLE weight AS SELECT edge.src, 1.00/COUNT(edge.des) AS wei FROM edge GROUP BY edge.src;



		CREATE TABLE edgeWithOuterDegree AS SELECT edge.src, edge.des, weight.wei FROM edge JOIN weight ON edge.src = weight.src;

		DROP TABLE weight;



		WHILE flag LOOP

			flag := false;

			CREATE TABLE pagerank1 AS SELECT edgeWithOuterDegree.des as src, SUM(pagerank.pr*edgeWithOuterDegree.wei*0.85) AS pr

					FROM pagerank LEFT JOIN edgeWithOuterDegree ON pagerank.src = edgeWithOuterDegree.src GROUP BY edgeWithOuterDegree.des;

			CREATE TABLE currentpagerank AS SELECT pagerank.src, 0.15/nodeNum+COALESCE(pagerank1.pr,0) as pr FROM pagerank LEFT JOIN pagerank1 ON pagerank.src = pagerank1.src;

			DROP TABLE pagerank1;

			SELECT |/SUM((pagerank.pr - currentpagerank.pr)^2) INTO delta FROM pagerank JOIN currentpagerank ON pagerank.src = currentpagerank.src;

			IF delta < 0.0000001 THEN

				flag = true;

			END IF;

			DROP TABLE pagerank;

			ALTER TABLE currentpagerank RENAME TO pagerank;

		END LOOP;

		DROP TABLE IF EXISTS edgeWithOuterDegree;

	END;

	$$ LANGUAGE plpgsql;
