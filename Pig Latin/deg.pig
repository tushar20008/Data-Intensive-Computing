/* 
Extend node degree counting program to determine the
following (submit graph.pig):

*/
-- Q1 the frequency of each degree value

A = LOAD 'ex_data/roadnet/roadNet-CA.txt' as (nodeA:chararray, nodeB:chararray);
B = GROUP A by nodeA;
C = FOREACH B GENERATE COUNT(A) as freq, group;
D = GROUP C by freq;
E = FOREACH D GENERATE group, COUNT(C) as freq ;
dump E;

/* 
output:

(1,30)
(2,18)
(3,153)
(4,111)
(5,5)
(6,1)


*/

-- Q2 the percentage of dead-end nodes

Copy_A = FOREACH A GENERATE nodeA as nodeA_Copy, nodeB as nodeB_Copy;
R_join = JOIN A by nodeA RIGHT, Copy_A by nodeB_Copy;
Null_rows = FILTER R_join by nodeA is null;

Grp = GROUP R_join all;
COUNT_F = FOREACH Grp GENERATE COUNT(R_join) as total_count;

Dead_grp = GROUP Null_rows all;
COUNT_D = FOREACH Dead_grp GENERATE COUNT(Null_rows) as dead_count;

Cross_join = CROSS COUNT_D, COUNT_F ;
Result = FOREACH Cross_join GENERATE 100*(double)dead_count/(double)total_count;

dump Result;

/* 
output:

(0.0)

*/


-- Q3 the average degree of the graph

Group_for_avg = GROUP C all;
AVRG = FOREACH Group_for_avg GENERATE AVG(C.freq);

dump AVRG;

/* 
output:
(3.1446540880503147)

*/