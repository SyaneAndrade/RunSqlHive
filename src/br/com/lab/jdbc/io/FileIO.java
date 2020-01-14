package br.com.lab.jdbc.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            makeString.append(line + " ");
        }
        return makeString.toString();
    }

    public List<String> makeArrayQuerys(String stringcriada){

        String[] querys_;
        querys_ = stringcriada.split(";");

        List<String> querys = new ArrayList<>();
        for(String query: querys_){
            querys.add(query);
        }
        querys.remove(querys.size() -1);
        return querys;
    }
}
