package br.com.lab.jdbc.lib;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import com.opencsv.CSVWriter;

public abstract class Utilities {

    public static List<String> datePartition(){

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
        List<String> datePartition = new ArrayList<>();
        datePartition.add(dtf.format(now));
        datePartition.add(dtf.format(yesterday));
        System.out.println("Current Date: " + dtf.format(now));
        System.out.println("Yesterday: " + dtf.format(yesterday));
        return datePartition;

    }

    public static int writeResultCSV(ResultSet resultDados, String query) throws IOException, SQLException {
        String arquivoName = query.toLowerCase().split("from")[1];
        String now = datePartition().get(0);
        File path = new File(now);
        path.mkdir();
        arquivoName = now + "/" + arquivoName.split(" ")[1] + ".csv";
        CSVWriter csvWriter = new CSVWriter( new FileWriter(arquivoName),
                '^', '"', '"', "\n");
        return csvWriter.writeAll(resultDados, true);
    }

}
