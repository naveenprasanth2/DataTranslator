package com.symphony;

public class Main {

    private final String folderPath = "";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar DataTranslator.jar <folder_path>");
            System.exit(1);
        }

        String folderPath = args[0];
        Translator translator = new Translator(folderPath);

        try {
            translator.process();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}