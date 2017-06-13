package de.hpi.isg.pyro.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Interaction utilities.
 */
public class User {

    private static BufferedReader stdInReader = new BufferedReader(new InputStreamReader(System.in));

    public static String prompt(String prompt) {
        System.out.println(prompt);
        try {
            return stdInReader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
