package br.com.lab.jdbc.lib;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

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
}
