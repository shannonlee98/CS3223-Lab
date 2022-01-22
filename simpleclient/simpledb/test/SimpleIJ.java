package simpledb.test;

import java.sql.*;
import java.util.Scanner;
// import simpledb.jdbc.embedded.EmbeddedDriver;
// import simpledb.jdbc.network.NetworkDriver;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.record.Schema;
import static java.sql.Types.INTEGER;

public class SimpleIJ {
   public static void main(String[] args) {
      Scanner sc = new Scanner(System.in);
      // System.out.println("Connect> ");
      // String s = sc.nextLine();
      // Driver d = (s.contains("//")) ? new NetworkDriver() : new EmbeddedDriver();

      // analogous to the driver
      SimpleDB db = new SimpleDB("studentdb");

      try {
         // (Connection conn = d.connect(s, null);
         //   Statement stmt = conn.createStatement()) {

         // analogous to the connection
         Transaction tx  = db.newTx();
         Planner planner = db.planner();

         System.out.print("\nSQL> ");
         while (sc.hasNextLine()) {
            // process one line of input
            String cmd = sc.nextLine().trim();
            if (cmd.startsWith("exit"))
               break;
            else if (cmd.startsWith("select"))
               // doQuery(stmt, cmd);
               doQuery(planner, tx, cmd);
            else
               // doUpdate(stmt, cmd);
               doUpdate(planner, tx, cmd);
            System.out.print("\nSQL> ");
         }
      }
      catch (Exception e) {
         e.printStackTrace();
      }
      sc.close();
   }

   private static void doQuery(Planner planner, Transaction tx, String cmd) {
      // private static void doQuery(Statement stmt, String cmd) {
      try {
         // (ResultSet rs = stmt.executeQuery(cmd)) {
         Plan p = planner.createQueryPlan(cmd, tx);
         
         // analogous to the result set
         Scan s = p.open();
         Schema sch = p.schema();
         
         // ResultSetMetaData md = rs.getMetaData();
         // int numcols = md.getColumnCount();
         int numcols = sch.fields().size();
         int totalwidth = 0;

         // print header
         for(int i=1; i<=numcols; i++) {
            // String fldname = md.getColumnName(i);
            String fldname = sch.fields().get(i-1);
            // int width = md.getColumnDisplaySize(i);
            int fldtype = sch.type(fldname);
            int fldlength = (fldtype == INTEGER) ? 6 : sch.length(fldname);
            int width = Math.max(fldname.length(), fldlength) + 1;
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for(int i=0; i<totalwidth; i++)
            System.out.print("-");
         System.out.println();

         // print records
         while(s.next()) {
            for (int i=1; i<=numcols; i++) {
               String fldname = sch.fields().get(i-1);
               int fldtype = sch.type(fldname);
               int fldlength = (fldtype == INTEGER) ? 6 : sch.length(fldname);
               String fmt = "%" + (Math.max(fldname.length(), fldlength) + 1);
               if (fldtype == Types.INTEGER) {
                  fldname = fldname.toLowerCase(); // to ensure case-insensitivity
                  int ival = s.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               }
               else {
                  fldname = fldname.toLowerCase(); // to ensure case-insensitivity
                  String sval = s.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
      }
      catch (Exception e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }

   private static void doUpdate(Planner planner, Transaction tx, String cmd) {
      // private static void doUpdate(Statement stmt, String cmd) {
      try {
         
         // analogous to the statement
         Plan p = planner.createQueryPlan(cmd, tx);
         // analogous to the result set
         Scan howmany = p.open();
         // int howmany = stmt.executeUpdate(cmd);

         System.out.println(howmany + " records processed");
      }
      catch (Exception e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }
}