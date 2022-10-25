package com.rocksetps.rocksetryow;

import rockset.com.google.gson.Gson;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;


public class WriteRocksetRest implements Runnable {
    private Properties m_props;
    private HttpClient client;
    private static Random rand;

    public WriteRocksetRest(Properties _m_props, HttpClient _client) {
        m_props = _m_props;
        client = _client;
        rand = new Random();
    }

    public void run() {
//        System.setProperty("jdk.httpclient.HttpClient.log", "all");
        // load json from a file or create a list of objects to be inserted
        LinkedList<Object> list = new LinkedList<>();
        int maxDocs = Integer.parseInt(m_props.getProperty("max_docs"));
        int numDocs = rand.nextInt(maxDocs) + 1;
        for (int i = 0; i < numDocs; i++) {
//            System.out.println(Integer.parseInt(m_props.getProperty("body_length")));
             list.add(makeDoc(Integer.parseInt(m_props.getProperty("body_length"))));
        }
        try {
            Gson gson = new Gson();
            String jsonArray = "{\"data\": " + gson.toJson(list) + "}";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(m_props.get("API_SERVER") +
                            "/v1/orgs/self/ws/" +
                            m_props.get("workspace") +
                            "/collections/" +
                            m_props.get("collection") +
                            "/docs"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "ApiKey " + m_props.get("API_KEY"))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonArray))
                    .build();
            Long start_time = System.currentTimeMillis();
            HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {

                JsonResponse convertedResponse = gson.fromJson(response.body(), JsonResponse.class);
                String offset = convertedResponse.last_offset;
                Long write_time = System.currentTimeMillis() - start_time;
                int count = CheckOffset(m_props, offset, gson, client);
                Long total_time = System.currentTimeMillis() - start_time;
                System.out.println("WriteAPI: " + Long.toString(write_time) +
                        " FencesAPI: " + Long.toString(total_time - write_time) +
                        " Total: " + Long.toString(total_time) +
                        " fence tries: " + Integer.toString(count));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static Integer CheckOffset(Properties m_props, String offset, Gson gson, HttpClient client) {
        String fence = "{\"name\": [\"" + offset + "\"]}";
        int count = 0;
        try {
            String passed = "false";
            while (passed == "false") {
                ++count;
                int maxJitter = Integer.parseInt(m_props.getProperty("max_jitter"));
                Thread.sleep(100 * count + rand.nextInt(maxJitter));
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(m_props.getProperty("API_SERVER") +
                                "/v1/orgs/self/ws/" +
                                m_props.getProperty("workspace") +
                                "/collections/" +
                                m_props.getProperty("collection") +
                                "/offsets/commit"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "ApiKey " + m_props.getProperty("API_KEY"))
                        .POST(HttpRequest.BodyPublishers.ofString(fence))
                        .build();

                HttpResponse<String> response = client.send(request,
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    FenceResponse convertedResponse = gson.fromJson(response.body(), FenceResponse.class);
                    passed = convertedResponse.data.passed;
                } else {
                    if (response.statusCode() == 429) {
                        int retryInterval = (int) Math.pow(2, (count + 5) * 2);
                        Thread.sleep(retryInterval + rand.nextInt(maxJitter));
                    }
                    ;
                    System.out.println("fences error code: " + response.statusCode() + " count: " + count + " fence: " + offset);
                }
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        return count;
    }

    public static Map<String, Object> makeDoc(Integer body_length) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("_id", getDocId());
        json.put("body", getDocBody(body_length));
        return json;
    }

    private static String getDocId() {
        int upperbound = 8999999;
        int int_random = rand.nextInt(upperbound) + 1000000;
        String docId = Integer.toString(int_random);
        return docId;
    }

    private static String getDocBody(Integer body_length) {
        String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder chars = new StringBuilder();
        while (chars.length() < body_length) { // length of the random string.
            int index = (int) (rand.nextFloat() * CHARS.length());
            chars.append(CHARS.charAt(index));
        }
        String charString = chars.toString();
        return charString;
    }

    public class JsonResponse {
        private List<Object> data;
        private String last_offset;
    }

    public class FenceResponse {
        Data data;
        Offsets offsets;
    }

    public class Data {
        private String fence;
        private String passed;
    }

    public class Offsets {
        private String commit;
    }
}
