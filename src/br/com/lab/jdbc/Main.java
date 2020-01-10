package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import br.com.lab.jdbc.io.FileIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        String urlHiveJdbc = "";
        String urlPrestoJdbc = "";
        String arquivo = "";

        String userHive = ((args[1] != "none")) ? args[1] : "";
        String passwordHive = ((args[2] != "none")) ? args[2] : "";

        try {
            urlHiveJdbc = args[0];
        }
        catch(Exception e){
            System.out.println("Necessário para como primeiro parametro a url para conexão jdbc com o Hive;");
        }


        try{
            arquivo = args[3];
        }
        catch (Exception e){
            System.out.println("Necessário passar como segundo parametro o arquivo com as querys que serão executadas;");
            System.exit(1);
        }

        JdbcHiveRun jdbcrun = new JdbcHiveRun(urlHiveJdbc, userHive, passwordHive);

        FileIO qrio_presto = new FileIO();

        String conteudoArquivo = qrio_presto.lerArquivo(arquivo);

        String[] querys = qrio_presto.criaArrayQuerys(conteudoArquivo);

        for (String query: querys){
            jdbcrun.executeQuery(query);
        }

    }
}
