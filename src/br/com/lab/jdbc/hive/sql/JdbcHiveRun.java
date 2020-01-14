package br.com.lab.jdbc.hive.sql;

import br.com.lab.jdbc.lib.Utilities;
import br.com.lab.jdbc.sql.TypeStatement;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.List;
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

        //Lista com as data atual e anterior
        List<String> dates = Utilities.datePartition();

        String dtCurrent = dates.get(0);
        String dtPass = dates.get(1);

        //Configuração das propeties da sessão
        Properties conProperties = new Properties();
        conProperties.put("hive.support.concurrency", "true");
        conProperties.put("hive.exec.dynamic.partition.mode", "nonstrict");
        conProperties.put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        conProperties.put("hive.compactor.initiator.on", "true");
        conProperties.put("hive.compactor.worker.threads", "1");
        conProperties.put("user", user);
        conProperties.put("password", password);

        //Inicializando sessão
        con = DriverManager.getConnection(caminhoHive, conProperties);
        stmt = con.createStatement();

        //Set de variaveis de data
        stmt.executeUpdate("set dtCurrent=" +"'"+ dtCurrent + "'");
        stmt.executeUpdate("set dtPass=" +"'"+ dtPass + "'");
        stmt.executeUpdate("set hive.exec.dynamic.partition=true");
        stmt.executeUpdate("set hive.exec.dynamic.partition.mode=nonstrict");
    }

    public  void execute(TypeStatement typeQuery) {

        ResultSet res = null;

        try {
            System.out.println(typeQuery.statement);
            //se for um update irá rodar aqui
            if(typeQuery.update) {
                System.out.print("Linhas processadas: ");
                System.out.println(stmt.executeUpdate(typeQuery.statement));
            }
            else if(typeQuery.select) {
                //se for do tipo select ira rodar aqui
                res = stmt.executeQuery(typeQuery.statement);
                System.out.println(res.getMetaData().getColumnCount());
                while (res.next()) {
                    for(int i=1; i < res.getMetaData().getColumnCount(); i++){
                        System.out.print(res.getString(i) + " ");
                    }
                    System.out.println(" ");
                }
            }
            else{
                //se for qlqr outra coisa ira rodar aqui
                res = stmt.executeQuery(typeQuery.statement);
                while (res.next()) {
                    System.out.println(res.getString(1));
                }
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
}