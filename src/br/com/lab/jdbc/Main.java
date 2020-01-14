package br.com.lab.jdbc;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import br.com.lab.jdbc.io.FileIO;
import br.com.lab.jdbc.hive.sql.JdbcHiveRun;
import br.com.lab.jdbc.sql.TypeStatement;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        //url de conexão do jdbc hive
        String urlHiveJdbc = "";

        //arquivo de sql
        String file = "";

        //Parametros de autenticação
        String userHive = ((args[1] != "none")) ? args[1] : "";
        String passwordHive = ((args[2] != "none")) ? args[2] : "";

        //Pegando o argumento da conexao
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

        //Criando a conexão
        JdbcHiveRun jdbcrun = new JdbcHiveRun(urlHiveJdbc, userHive, passwordHive);

        //Lendo o arquivo
        FileIO qrio_hive = new FileIO();
        String contentFile = qrio_hive.readFile(file);
        List<String> querys = qrio_hive.makeArrayQuerys(contentFile);

        //Processando as querys
        for (String query: querys){
            TypeStatement typeQuery = new TypeStatement(query);
            jdbcrun.execute(typeQuery);
        }

    }
}
