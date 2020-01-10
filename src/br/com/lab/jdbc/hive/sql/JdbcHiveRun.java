package br.com.lab.jdbc.hive.sql;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Properties;

public class JdbcHiveRun {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection con;
    public Statement stmt;

    public JdbcHiveRun(String caminhoHive, String user, String password) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Properties conProperties = new Properties();
        conProperties.put("hive.support.concurrency", "true");
        conProperties.put("hive.exec.dynamic.partition.mode", "nonstrict");
        conProperties.put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        conProperties.put("hive.compactor.initiator.on", "true");
        conProperties.put("hive.compactor.worker.threads", "1");
        conProperties.put("user", user);
        conProperties.put("password", password);
        con = DriverManager.getConnection(caminhoHive, conProperties);
        stmt = con.createStatement();
    }

    public  void executeQuery(String query) {

        ResultSet set = null;

        try {
            System.out.println(query);
            ResultSet res = stmt.executeQuery(query);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
}