package br.com.lab.jdbc.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileIO {

    private BufferedReader buffer;

    public String readFile(String arquivo) throws IOException {

        String line;
        StringBuilder makeString = new StringBuilder();

        try {
            buffer = new BufferedReader(new FileReader(arquivo));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        while((line = buffer.readLine()) != null) {
            makeString.append(line);
        }
        return makeString.toString();
    }

    public String[] makeArrayQuerys(String stringcriada){
        String[] querys;
        querys = stringcriada.split(";");
        return querys;
    }
}
