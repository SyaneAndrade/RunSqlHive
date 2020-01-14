package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbc.io.FileIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;
import br.com.lab.jdbc.sql.TypeStatement;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        String urlHiveJdbc = "";
        String file = "";

        String userHive = ((args[1] != "none")) ? args[1] : "";
        String passwordHive = ((args[2] != "none")) ? args[2] : "";

        try {
            urlHiveJdbc = args[0];
        }
        catch(Exception e){
            System.out.println("Necessário para como primeiro parametro a url para conexão jdbc com o Hive;");
        }


        try{
            file = args[3];
        }
        catch (Exception e){
            System.out.println("Necessário passar como segundo parametro o arquivo com as querys que serão executadas;");
            System.exit(1);
        }

        JdbcHiveRun jdbcrun = new JdbcHiveRun(urlHiveJdbc, userHive, passwordHive);

        FileIO qrio_hive = new FileIO();

        String contentFile = qrio_hive.readFile(file);

        String[] querys = qrio_hive.makeArrayQuerys(contentFile);

        for (String query: querys){
            TypeStatement typeQuery = new TypeStatement(query);
            jdbcrun.execute(typeQuery);
        }

    }
}
