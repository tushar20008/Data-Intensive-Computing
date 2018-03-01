emp = load 'ex_data/emp_dept/emp.csv' as (empno:int, ename:chararray, job:chararray, mgr:int, hiredate:datetime, sal:float,deptno: int);
dept = load 'ex_data/emp_dept/dept.csv' as (deptno:int, dname:chararray, loc: chararray);
salgrade = load 'ex_data/emp_dept/salgrade.csv' as (grade:int,losal:int, hisal:int);

dump emp;

dump dept;

dump salgrade;

-- Q1 Smith’s employment date

A = FILTER emp BY ename MATCHES 'SMITH';
result_q1 = FOREACH A GENERATE ename,hiredate;
DUMP result_q1;
/* 
output: (SMITH,1980-12-17T00:00:00.000+08:00)
*/

--Q2 Ford’s job title

B = FILTER emp BY ename MATCHES 'FORD';
result_q2 = FOREACH B GENERATE ename,job;
DUMP result_q2;

/* 
output: (FORD,ANALYST)
*/


--Q3 The first employee (by the hiredate)

C = FOREACH emp GENERATE ename,hiredate;
D = ORDER C BY hiredate;
result_q3 = LIMIT D 1;
DUMP result_q3;

/* 
output: (SMITH,1980-12-17T00:00:00.000+08:00)

*/


--Q4 The number of employees in each department

emp_dept_join = JOIN emp BY deptno, dept BY deptno;
dept_groups = GROUP emp_dept_join BY dept::dname;
result_q4 = FOREACH dept_groups GENERATE group,COUNT(emp_dept_join);
dump result_q4; 

/* 
output: 
(SALES,6)
(RESEARCH,5)
(ACCOUNTING,3)
*/

--Q5 The number of employees in each city

loc_groups = GROUP emp_dept_join BY dept::loc;
result_q5 = FOREACH loc_groups GENERATE group,COUNT(emp_dept_join);
dump result_q5;

/* 
output: 
(DALLAS,5)
(CHICAGO,6)
(NEW YORK,3)
*/


--Q6 The average salary in each city

groups = GROUP emp_dept_join BY dept::loc;
result_q6 = FOREACH groups GENERATE group,AVG(emp_dept_join.sal);
dump result_q6;

/* 
output: 
(DALLAS,2175.0)
(CHICAGO,1566.6666666666667)
(NEW YORK,2916.6666666666665)

*/

--Q7 The highest paid employee in each department

dept_groups = GROUP emp by deptno;
sal_max = FOREACH dept_groups GENERATE group, MAX(emp.sal) as maxsal;
join_sal_max = JOIN sal_max by maxsal, emp by sal;
result_q7 = FOREACH join_sal_max GENERATE emp::deptno, emp::ename, sal_max::maxsal;
dump result_q7;

/* 
output:
(30,BLAKE,2850.0)
(20,SCOTT,3000.0)
(20,FORD,3000.0)
(10,KING,5000.0)

*/

--Q8 Managers whose subordinates have at least one subordinate

emp_copy_1 = FOREACH emp GENERATE empno, ename, mgr, job;
managers_only = FILTER emp_copy_1 BY $3 MATCHES 'MANAGER';
emp_copy_2 = FOREACH emp GENERATE empno, ename, mgr;
emp_self_join = JOIN managers_only BY $0, emp_copy_2 BY $2;
-- emp_self_join yields a relation with 7 fields
two_level_subordinates = JOIN emp_self_join by $4, emp_copy_1 by $2;
manager_groups = GROUP two_level_subordinates BY ($0, $1);
greater_than_one = FILTER manager_groups BY COUNT(two_level_subordinates) > 1;
result_q8 = FOREACH greater_than_one GENERATE group;
dump result_q8;

/* 
output: 
((7655,JONES))

*/

--Q9 The number of employees for each hiring year

get_years = FOREACH emp GENERATE (chararray)GetYear(hiredate) as hire_year, ename;
q9_groups = GROUP get_years BY hire_year;
result_q9 = FOREACH q9_groups GENERATE group, COUNT(get_years);
dump result_q9;

/* 
output:
(1980,1)
(1981,10)
(1987,2)
(1991,1)
*/

--Q10 The pay grade of each employee

crossed = CROSS emp, salgrade;
emp_salgrade = FILTER crossed by sal>=losal and sal <=hisal;
result_q10 = FOREACH emp_salgrade GENERATE emp::empno, emp::ename, salgrade::grade;
dump result_q10;

/* 
output:

(7934,MILLER,2)
(7902,FORD,4)
(7900,JAMES,1)
(7876,ADAMS,1)
(7844,TURNER,3)
(7839,KING,5)
(7788,SCOTT,4)
(7782,CLARK,4)
(7698,BLAKE,4)
(7654,MARTIN,2)
(7655,JONES,4)
(7521,WARD,2)
(7499,ALLEN,3)
(7369,SMITH,1)

*/
