package br.com.lab.jdbc.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileIO {

    private BufferedReader buffer;

    public String lerArquivo(String arquivo) throws IOException {

        String linha;
        StringBuilder criaString = new StringBuilder();

        try {
            buffer = new BufferedReader(new FileReader(arquivo));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        while((linha = buffer.readLine()) != null) {
            criaString.append(linha);
        }
        return criaString.toString();
    }

    public String[] criaArrayQuerys(String stringcriada){
        String[] querys;
        querys = stringcriada.split(";");
        return querys;
    }
}
