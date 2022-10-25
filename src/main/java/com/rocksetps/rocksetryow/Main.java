package com.rocksetps.rocksetryow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {
    private static Properties m_props;
    private static File propFile;

    public static void main(String[] args) throws IOException, InterruptedException {

        //load properties files
        m_props = new Properties();
        propFile = new File("configuration.properties");
        try {
            LoadProperties(propFile);
        } catch (Exception e) {
            System.out.println(e);
        }

        HttpClient client = HttpClient.newHttpClient();
        WriteRocksetRest write = new WriteRocksetRest(m_props, client);
        ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(m_props.getProperty("write_threads")));

        while(true) {
            if (((ThreadPoolExecutor) executor).getActiveCount() < Integer.parseInt(m_props.getProperty("write_threads"))){
//                System.out.println(((ThreadPoolExecutor) executor).getActiveCount());
                Thread newThread = new Thread(write);
                executor.execute(newThread);
            };
        }
    }

        public static void LoadProperties(File f) throws IOException {
            FileInputStream propStream = null;
            propStream = new FileInputStream(f);
            m_props.load(propStream);
            propStream.close();
        }

}
