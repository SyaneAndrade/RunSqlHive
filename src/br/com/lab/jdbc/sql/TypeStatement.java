package br.com.lab.jdbc.sql;

public class TypeStatement {

    public String statement;
    public Boolean update;
    public Boolean select;

    public TypeStatement(String query){
        String[] verify = query.split(" ");
        update = !(verify[0].toUpperCase().contains("SELECT") || verify[0].toUpperCase().contains("SHOW"));
        select = (verify[0].toUpperCase().contains("SELECT"));
        statement = query;
    }
}
