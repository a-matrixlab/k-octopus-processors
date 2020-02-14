/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author alexmy
 */
public class readCsvFile {
    
    public static void main(String[] args) {
        String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        
        String string = "\"(512) ALT\",German Altbier,6,57,3.75,(512) Brewing Company,\"Brewery, Bar\",\"407 Radam Ln, "
                + "Ste F200\",Texas,Austin,United States,78745-1197,(512) 921-1545,http://512brewing.com";
        
        String[] split = string.replaceAll("'", "").split(regex);
        
        for (String split1 : split) {
            System.out.println(split1);
        }
    }
}