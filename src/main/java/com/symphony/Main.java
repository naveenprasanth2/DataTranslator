package com.symphony;


import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {

    public static void main(String[] args) {
        String folderPath = "src/main/resources/data";
        Translator translator = new Translator(folderPath);

        try {
            translator.process();
        } catch (Exception e) {
            log.error("An error occurred during processing", e);
        }
    }
}