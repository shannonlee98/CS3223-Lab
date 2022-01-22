package simpledb.test;
// package embedded;
// import java.sql.*;
// import simpledb.jdbc.embedded.EmbeddedDriver;

import simpledb.tx.Transaction;
// import simpledb.plan.Plan;
import simpledb.plan.Planner;
// import simpledb.query.*;
import simpledb.server.SimpleDB;

public class ChangeMajor {
   public static void main(String[] args) {
      // Driver d = new EmbeddedDriver();
      // String url = "jdbc:simpledb:studentdb";

      try {
         // (Connection conn = d.connect(url, null); 
         //    Statement stmt = conn.createStatement()) {

         // analogous to the driver
         SimpleDB db = new SimpleDB("studentdb");

         // analogous to the connection
         Transaction tx  = db.newTx();
         Planner planner = db.planner();

         String qry = "update STUDENT "
                    + "set MajorId = 30 "
                    + "where SName = 'amy'";
         // analogous to the statement
         planner.executeUpdate(qry, tx);
         // stmt.executeUpdate(cmd);
         System.out.println("Amy is now a drama major.");
      }
      catch(Exception e) {
         e.printStackTrace();
      }
   }
}
