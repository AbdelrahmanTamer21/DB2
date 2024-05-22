import java.sql.*;
import java.util.HashMap;


public class Schema2 {

//	CREATE TABLE Employee(Fname CHAR(20), Minit CHAR(10), Lname CHAR(20), ssn INT PRIMARY KEY, Bdate date, address CHAR(20), sex CHARACTER(1), salary INT, Super_snn INT REFERENCES Employee(ssn), dno INT);

    public static long insertEmployee(String Fname, String Minit,String Lname,int ssn, Date Bdate, String address, String sex, int salary, int superSSN, int dno, Connection conn) {
        String SQL = "INSERT INTO Employee(Fname,Minit,Lname,ssn,Bdate,address,sex,salary,Super_snn,dno) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?);";

        long id = 0;
        try{
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, Fname);
            pstmt.setString(2, Minit);
            pstmt.setString(3, Lname);
            pstmt.setInt(4, ssn);
            pstmt.setDate(5, Bdate);
            pstmt.setString(6, address);
            pstmt.setString(7, sex);
            pstmt.setInt(8, salary);
            pstmt.setInt(9, superSSN);
            pstmt.setInt(10, dno);

            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(4);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }
//	 CREATE TABLE Department(Dname CHAR(20), Dnumber INT PRIMARY KEY, Mgr_snn int REFERENCES employee, Mgr_start_date date );

    public static long insertDepartment(String Dname, int Dnumber,int MgrSSN, Date startDate, Connection conn) {
        String SQL = "INSERT INTO Department(Dname,Dnumber,Mgr_snn,Mgr_start_date) "
                + "VALUES(?,?,?,?);";

        long id = 0;
        try{
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, Dname);
            pstmt.setInt(2, Dnumber);
            pstmt.setInt(3, MgrSSN);
            pstmt.setDate(4, startDate);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(2);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }
    //	 CREATE TABLE Dept_locations(Dnumber integer REFERENCES Department, Dlocation CHAR(20), PRIMARY KEY(Dnumber,Dlocation));
    public static long insertDeptLocations(int Dnumber,String Dlocation, Connection conn) {
        String SQL = "INSERT INTO Dept_locations(Dnumber,Dlocation) "
                + "VALUES(?,?);";

        long id = 0;
        try{
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(2, Dlocation);
            pstmt.setInt(1, Dnumber);



            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(1);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }

    //	 CREATE TABLE Project(Pname CHAR(20), Pnumber INT PRIMARY KEY, Plocation CHAR(50), Dnumber INT REFERENCES Department);
    public static long insertProject(String Pname, int Pnumber,String pLocation, int Dnumber, Connection conn) {
        String SQL = "INSERT INTO Project(Pname,Pnumber,Plocation,Dnumber) "
                + "VALUES(?,?,?,?);";

        long id = 0;
        try{
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, Pname);
            pstmt.setInt(2, Pnumber);
            pstmt.setString(3, pLocation);
            pstmt.setInt(4, Dnumber);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(2);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }
//	 CREATE TABLE Works_on(Essn int REFERENCES Employee, Pno int REFERENCES Project, Hours int, PRIMARY KEY(Essn,Pno));

    public static long insertWorksOn(int Essn,int pNo, int hours, Connection conn) {
        String SQL = "INSERT INTO Works_on(Essn,Pno,Hours) "
                + "VALUES(?,?,?);";

        long id = 0;
        try{
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setInt(2, pNo);
            pstmt.setInt(1, Essn);
            pstmt.setInt(3, hours);



            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(1);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }
    //	 CREATE TABLE Dependent(Essn INT REFERENCES Employee, Dependent_name CHAR(20), sex CHARACTER(1), Bdate date, Relationship CHAR(20), PRIMARY KEY(Essn, Dependent_name));
    public static long insertDependent(int Essn, String dependentName,String sex, Date Bdate,String relationship, Connection conn) {
        String SQL = "INSERT INTO Dependent(Essn,Dependent_name,sex,Bdate,Relationship) "
                + "VALUES(?,?,?,?,?);";

        long id = 0;
        try{
            conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setInt(1, Essn);
            pstmt.setString(2, dependentName);
            pstmt.setString(3, sex);
            pstmt.setDate(4, Bdate);
            pstmt.setString(5, relationship);


            int affectedRows = pstmt.executeUpdate();
            System.out.println("Number of affected rows is " + affectedRows);
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
//                	 System.out.println(rs.next());
                    if (rs.next()) {
                        id = rs.getLong(1);
                        pstmt.close();
                        conn.commit();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                    System.out.println(ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
        return id;
    }

    /////////////////////////////////////////////// Data Population Methods //////////////////////////////////////////////////////////////
    @SuppressWarnings("deprecation")
    public static void populateEmployee(Connection conn) {
        for (int i = 1; i <= 16000; i++) {
            String fname = "Name" + i;
            String sex = i <= 600 ? "M" : (i % 2 == 0 ? "M" : "F");
            int salary = 0;
            int dno = (((i+3)%150)+1);
            if (i < 14900){
                dno = 1;
            }
            else if (i < 15900){
                dno = (i%99)+2;

            }else dno= (i%50)+101;
            if (i <= 600){
                salary = 70000;
            }
            else if(dno <= 100 ){salary = 40001 + (int) (Math.random() * 900);}
            else salary = 10000 + (int) (Math.random() * 900);

            if (dno == 5)
                salary = 50000;


            if(insertEmployee(fname, "M" + i,"employee" + i, i, new Date(22,1,1999), "address" + i ,sex,salary,i,dno, conn)==0){
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    @SuppressWarnings("deprecation")
    public static void populateDepartment(Connection conn) {
        for (int i = 1; i <= 150; i++) {
            if (insertDepartment("Department" + i, i,((i%100)+1),new Date(1,1,1990), conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }
    public static void populateDeptLocations(Connection conn) {
        for (int i = 1; i <= 150; i++) {
            if (insertDeptLocations(i, "Location" + i, conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    public static void populateProject(Connection conn) {
        for (int i = 1; i <= 9200; i++) {
            if (insertProject("Project" + i, i,"Location1" + i,((i%150)+1), conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }
    public static void populateWorksOn(Connection conn) {
        for (int i = 1; i <= 16000; i++) {
            int ssn=i;
            if (i <= 600){
                ssn=1;
            }

            if (insertWorksOn(ssn, ((i%9200)+1), i, conn) == 0) {
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    @SuppressWarnings("deprecation")
    public static void populateDependent(Connection conn) {
        for (int i = 1; i <= 600; i++) {
            String dependentName = "Name" + i;
            String sex = "M";
            if( insertDependent(i , dependentName , sex , new Date(1,1,1999) ,"child" , conn) == 0){
                System.err.println("insertion of record " + i + " failed");
                break;
            } else
                System.out.println("insertion was successful");
        }
    }

    public static void insertSchema2(Connection connection) {
        populateEmployee(connection);
        populateDepartment(connection);
        populateDeptLocations(connection);
        populateProject(connection);
        populateWorksOn(connection);
        populateDependent(connection);
    }
    public static void executeQuery(String query, Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData r = rs.getMetaData();
            int columnCount = r.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(r.getColumnName(i) + ": " + columnValue);
                }
                System.out.println("");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
    public static void main(String[] args) {

        System.out.println("-------- PostgreSQL "
                + "JDBC Connection Testing ------------");

        try {

            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();
            return;

        }

        System.out.println("PostgreSQL JDBC Driver Registered!");

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:5432/schema2", "postgres",
                    "12qwaszxZ");

            String query2 = "SELECT DISTINCT pnumber FROM project " +
                    "WHERE pnumber IN (SELECT pnumber FROM project, department d, employee e " +
                    "WHERE e.dno = d.dnumber AND d.mgr_snn = e.ssn AND e.lname = 'employee1') " +
                    "OR pnumber IN (SELECT pno FROM works_on, employee WHERE essn = ssn AND lname = 'employee1');";

            String query3 = "SELECT lname, fname FROM employee " +
                    "WHERE salary > ALL (SELECT salary FROM employee WHERE dno = 5);";

            String query4 = "SELECT e.fname, e.lname FROM employee e " +
                    "WHERE e.ssn IN (SELECT essn FROM dependent d WHERE e.fname = d.dependent_name AND e.sex = d.sex);";

            String query5 = "SELECT fname, lname FROM employee " +
                    "WHERE EXISTS (SELECT * FROM dependent WHERE ssn = essn);";

            String query6 = "SELECT dnumber, count(*) FROM department, employee " +
                    "WHERE dnumber = dno AND salary > 40000 " +
                    "AND dno IN (SELECT dno FROM employee GROUP BY dno HAVING count(*) > 5) " +
                    "GROUP BY dnumber;";

            // Execute the queries
//            insertSchema2(connection);
//            executeQuery(query2, connection);
//            executeQuery(query3, connection);
//            executeQuery(query4, connection);
//            executeQuery(query5, connection);
//            executeQuery(query6, connection);
            System.out.println("Executing query 2 without index");
            Queries.executeWithNoIndex(query2, connection);
            System.out.println("Executing query 3 without index");
            Queries.executeWithNoIndex(query3, connection);
            System.out.println("Executing query 4 without index");
            Queries.executeWithNoIndex(query4, connection);
            System.out.println("Executing query 5 without index");
            Queries.executeWithNoIndex(query5, connection);
            System.out.println("Executing query 6 without index");
            Queries.executeWithNoIndex(query6, connection);

            HashMap<String, String> hashMap = new HashMap<>();
            hashMap.put("pnumber", "project");
            System.out.println("Executing query 2 with B+ tree index");
            Queries.executeWithBTreeIndex(query2, hashMap, connection);


        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;

        }

        if (connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }
    }
}