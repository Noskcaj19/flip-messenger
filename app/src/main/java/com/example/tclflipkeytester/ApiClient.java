package com.example.tclflipkeytester;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class ApiClient {
    private final String baseUrl;
    private final String apiToken;

    ApiClient(String baseUrl, String apiToken) {
        boolean secure = baseUrl.startsWith("https://");
        boolean debugHTTP = BuildConfig.DEBUG && baseUrl.startsWith("http://");
        if (!secure && !debugHTTP) {
            throw new IllegalArgumentException("Server URL must use HTTPS outside debug builds");
        }
        this.baseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        this.apiToken = apiToken;
    }

    Snapshot bootstrap() throws IOException, JSONException {
        JSONObject response = request("GET", "/v1/bootstrap", null);
        List<Channel> channels = new ArrayList<>();
        JSONArray channelValues = response.getJSONArray("channels");
        for (int i = 0; i < channelValues.length(); i++) {
            JSONObject value = channelValues.getJSONObject(i);
            channels.add(new Channel(
                    value.getString("channel_id"),
                    value.getString("qualified_name"),
                    value.getString("display_name")));
        }
        return new Snapshot(
                response.getString("cursor"),
                channels,
                parseMessages(response.getJSONArray("messages")));
    }

    SyncResult sync(String after) throws IOException, JSONException {
        JSONObject response = request("GET", "/v1/sync?after=" + after, null);
        JSONArray events = response.getJSONArray("events");
        List<Message> messages = new ArrayList<>();
        String cursor = after;
        for (int i = 0; i < events.length(); i++) {
            JSONObject event = events.getJSONObject(i);
            cursor = event.getString("cursor");
            if ("message.created".equals(event.getString("type"))) {
                messages.add(parseMessage(event.getJSONObject("body")));
            }
        }
        if (events.length() == 0) {
            cursor = response.getString("high_watermark");
        }
        return new SyncResult(cursor, messages);
    }

    void sendMessage(String commandId, String clientMessageId, String channelId, String text)
            throws IOException, JSONException {
        JSONObject body = new JSONObject()
                .put("client_message_id", clientMessageId)
                .put("channel_id", channelId)
                .put("text", text);
        JSONObject command = new JSONObject()
                .put("v", 1)
                .put("kind", "command")
                .put("type", "message.send")
                .put("command_id", commandId)
                .put("body", body);
        request("POST", "/v1/messages", command);
    }

    private JSONObject request(String method, String path, JSONObject body)
            throws IOException, JSONException {
        HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(8_000);
        connection.setReadTimeout(8_000);
        connection.setRequestProperty("Authorization", "Bearer " + apiToken);
        connection.setRequestProperty("Accept", "application/json");
        if (body != null) {
            byte[] encoded = body.toString().getBytes(StandardCharsets.UTF_8);
            connection.setDoOutput(true);
            connection.setFixedLengthStreamingMode(encoded.length);
            connection.setRequestProperty("Content-Type", "application/json");
            try (OutputStream output = connection.getOutputStream()) {
                output.write(encoded);
            }
        }

        int status = connection.getResponseCode();
        InputStream stream = status >= 200 && status < 300
                ? connection.getInputStream()
                : connection.getErrorStream();
        String response = readAll(stream);
        connection.disconnect();
        if (status < 200 || status >= 300) {
            throw new IOException("HTTP " + status + ": " + response);
        }
        return new JSONObject(response);
    }

    private static List<Message> parseMessages(JSONArray values) throws JSONException {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < values.length(); i++) {
            messages.add(parseMessage(values.getJSONObject(i)));
        }
        return messages;
    }

    private static Message parseMessage(JSONObject value) throws JSONException {
        return new Message(
                value.getString("message_id"),
                value.getString("channel_id"),
                value.getString("author"),
                value.getString("text"));
    }

    private static String readAll(InputStream stream) throws IOException {
        if (stream == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
        }
        return result.toString();
    }
}
