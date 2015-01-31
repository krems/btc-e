package com.krems;


import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TickerListener {
    private static final long TIMEOUT = 2;
    private static final String filePath = "logs/ticker.csv";
    private static final String ignoreInvalid = "?ignore_invalid=1";
    private static final String TARGET_URL = "https://btc-e.com/api/3/";
    private static final String TICKER = "ticker/";
    private static final String INFO = "info/";
    private static final Set<String> ccyPairs = new HashSet<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> task;
    private FileWriter fileWriter;

    public static void main(String[] args) throws IOException {
        TickerListener tickerListener = new TickerListener();
        tickerListener.init();
        tickerListener.start();
    }

    private void init() throws IOException {
        fileWriter = new FileWriter(filePath);
        fileWriter.write("ccy_pair,high,low,avg,vol,vol_cur,last,buy,sell,updated\n");
    }

    public void start() throws IOException {
        task = executor.scheduleAtFixedRate(this::updateMarketData, 0, TIMEOUT, TimeUnit.SECONDS);
    }

    public void updateMarketData() {
        populateCcyPairs();
        mineTickData();
    }

    private void mineTickData() {
        URLConnection connection = null;
        try {
            String address = getAddressString();
            URL url = new URL(address);
            connection = url.openConnection();
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1500);
            Scanner scanner = new Scanner(connection.getInputStream());
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                JSONObject json = new JSONObject(line);
                for (String ccyPair : ccyPairs) {
                    JSONObject jsonCcyPair = json.getJSONObject(ccyPair);
                    parseAndSaveCcyPair(ccyPair, jsonCcyPair);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            task.cancel(true);
            try {
                this.start();
            } catch (IOException e1) {
                e1.printStackTrace();
                System.exit(-1);
            }
        } 
    }

    private String getAddressString() {
        StringBuilder address = new StringBuilder(TARGET_URL).append(TICKER);
        for (String ccyPair : ccyPairs) {
            address.append(ccyPair).append("-");
        }
        address.deleteCharAt(address.lastIndexOf("-")).append(ignoreInvalid);
        return address.toString();
    }

    private void parseAndSaveCcyPair(String ccyPair, JSONObject jsonCcyPair) throws JSONException, IOException {
        double high = jsonCcyPair.getDouble("high");
        double low = jsonCcyPair.getDouble("low");
        double avg = jsonCcyPair.getDouble("avg");
        double vol = jsonCcyPair.getDouble("vol");
        double volCur = jsonCcyPair.getDouble("vol_cur");
        double last = jsonCcyPair.getDouble("last");
        double buy = jsonCcyPair.getDouble("buy");
        double sell = jsonCcyPair.getDouble("sell");
        long updated = jsonCcyPair.getLong("updated");
        StringBuilder stats = new StringBuilder(ccyPair);
        stats.append(",")
                .append(high).append(",")
                .append(low).append(",")
                .append(avg).append(",")
                .append(vol).append(",")
                .append(volCur).append(",")
                .append(last).append(",")
                .append(buy).append(",")
                .append(sell).append(",")
                .append(updated).append("\n");
        String str = stats.toString();
        fileWriter.write(str);
        fileWriter.flush();
    }

    private void populateCcyPairs() {
        ccyPairs.clear();
        ccyPairs.add("btc_usd");
        ccyPairs.add("btc_rur");
        try {
            URL url = new URL(TARGET_URL + INFO);
            URLConnection connection = url.openConnection();
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1500);
            Scanner scanner = new Scanner(connection.getInputStream());
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                JSONObject json = new JSONObject(line);
                JSONObject pairs = json.getJSONObject("pairs");
                Iterator keys = pairs.keys();
                while (keys.hasNext()) {
                    String pair = (String) keys.next();
                    ccyPairs.add(pair);
                }
            }
        } catch (IOException | JSONException e) {
            e.printStackTrace();
            try {
                this.start();
            } catch (IOException e1) {
                e1.printStackTrace();
                System.exit(-1);
            }
        }
    }
}
