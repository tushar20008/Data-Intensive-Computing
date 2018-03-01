-- POSTGRESQL Queries
-- Unzip data into /home/bitnami/Desktop/ (change path in the copy statements if different)

DROP TABLE emp;
CREATE TABLE emp(
   empno  INTEGER  NOT NULL PRIMARY KEY 
  ,ename  VARCHAR(10) NOT NULL
  ,job    VARCHAR(10) NOT NULL
  ,mgr    INTEGER
  ,hiredate DATE  NOT NULL
  ,sal    INTEGER  NOT NULL
  ,deptno INTEGER  NOT NULL
);
copy emp from '/home/bitnami/Desktop/emp_dept/emp.csv' with (format csv, delimiter E'\t',null '\N');

DROP TABLE dept;
CREATE TABLE dept(
   deptno INTEGER  NOT NULL PRIMARY KEY 
  ,dname  VARCHAR(12) NOT NULL
  ,loc    VARCHAR(10) NOT NULL
);
copy dept from '/home/bitnami/Desktop/emp_dept/dept.csv' with (format csv, delimiter E'\t');

DROP TABLE salgrade;
CREATE TABLE salgrade(
   grade  INTEGER  NOT NULL PRIMARY KEY 
  ,losal  INTEGER  NOT NULL
  ,hisal  INTEGER  NOT NULL
);
copy salgrade from '/home/bitnami/Desktop/emp_dept/salgrade.csv' with (format csv, delimiter E'\t');

-- Q1 Smith’s employment date
SELECT ename, hiredate 
FROM emp 
WHERE ename LIKE 'SMITH%';

--Q2 Ford’s job title
SELECT ename,job
FROM emp
WHERE ename LIKE 'FORD%';

--Q3 The first employee (by the hiredate)
SELECT empno,ename, hiredate
FROM emp
ORDER BY hiredate ASC
LIMIT 1;

--Q4 The number of employees in each department
SELECT dname,COUNT(*) 
FROM (SELECT dname FROM emp,dept WHERE emp.deptno = dept.deptno) AS result
GROUP BY dname;

--Q5 The number of employees in each city
SELECT result.loc,COUNT(*) 
FROM (SELECT loc FROM emp,dept WHERE emp.deptno = dept.deptno) AS result
GROUP BY loc;

--Q6 The average salary in each city
SELECT loc, AVG(sal)
FROM (SELECT loc,sal FROM emp,dept WHERE emp.deptno = dept.deptno) AS result
GROUP BY loc;

--Q7 The highest paid employee in each department
SELECT dname, ename, maxsal 
FROM emp INNER JOIN (SELECT dept.dname,emp.deptno, MAX(emp.sal) as maxsal FROM dept, emp WHERE dept.deptno=emp.deptno GROUP BY emp.deptno,dept.dname) as result ON (emp.deptno = result.deptno AND emp.sal = maxsal);

--Q8 Managers whose subordinates have at least one subordinate
SELECT result.m_no,result.m_name,COUNT(*) as subordinates_of_subordinates 
FROM (SELECT A.empno as m_no,A.ename as m_name,A.mgr as manager,B.empno as sub_no,B.ename as sub_name,B.mgr as sub_mgr FROM emp as A INNER JOIN  emp as B ON (A.empno=B.mgr) WHERE A.job LIKE 'MANAGER%') as result INNER JOIN emp AS E ON (result.sub_no=E.mgr)
GROUP BY result.m_name,result.m_no
HAVING COUNT(*) > 1;

--Q9 The number of employees for each hiring year
SELECT EXTRACT(YEAR FROM hiredate) as hireyear, COUNT(*)
FROM emp
GROUP BY hireyear;

--Q10 The pay grade of each employee
SELECT empno,ename,grade FROM (SELECT * FROM emp,salgrade) AS result WHERE sal >= losal and sal <= hisal;