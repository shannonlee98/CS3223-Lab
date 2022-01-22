package simpledb.test;
// package embedded;
// import java.sql.*;
import java.util.Scanner;
// import simpledb.jdbc.embedded.EmbeddedDriver;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;

public class FindMajors {
   public static void main(String[] args) {
      System.out.print("Enter a department name: ");
      Scanner sc = new Scanner(System.in);
      String major = sc.next();
      sc.close();
      System.out.println("Here are the " + major + " majors");
      System.out.println("Name\tGradYear");

      // String url = "jdbc:simpledb:studentdb";
      String qry = "select sname, gradyear "
            + "from student, dept "
            + "where did = majorid "
            + "and dname = '" + major + "'";
 
      // Driver d = new EmbeddedDriver();

      // analogous to the driver
      SimpleDB db = new SimpleDB("studentdb");

      try {
         // (Connection conn = d.connect(url, null);
         //    Statement stmt = conn.createStatement();
         //    ResultSet rs = stmt.executeQuery(qry)) {

         // analogous to the connection
         Transaction tx  = db.newTx();
         Planner planner = db.planner();
         
         // analogous to the statement
         Plan p = planner.createQueryPlan(qry, tx);
         
         // analogous to the result set
         Scan rs = p.open();

         while (rs.next()) {
            String sname = rs.getString("sname");
            int gradyear = rs.getInt("gradyear");
            System.out.println(sname + "\t" + gradyear);
         }
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
