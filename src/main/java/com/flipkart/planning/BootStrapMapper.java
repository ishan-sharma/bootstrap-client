package com.flipkart.planning;

import com.fasterxml.jackson.databind.ObjectMapper;
import flipkart.dsp.santa.bernard.model.pricing.PricingNotificationKafkaMessage;
import flipkart.pricing.libs.fatak.PriceObject;
import flipkart.pricing.libs.fatak.PriceResponseV2;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class BootStrapMapper extends Mapper<LongWritable, Text, Text, Text> {
    private KafkaProducer<String, PricingNotificationKafkaMessage> kafkaProducer = null;
    private List<String> listingIds = new ArrayList<>();
    private int batchSize = 0;
    ObjectMapper objectMapper = new ObjectMapper();


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        listingIds.add(line);
        batchSize++;

        if(batchSize < 30) {
            return;
        }

        try {
            postReprice(getRepricingRequest(listingIds));
        }
        catch (Exception e) {
            System.out.println("request failed");
        }
        finally {
            if(batchSize == 30){
                batchSize = 0;
                listingIds.clear();
            }
        }
    }

    private void postReprice(RepricingRequest repricingRequest) throws IOException {
        String endpoint = "http://10.47.6.229/v1/listingRepriceEvents";

        HttpURLConnection connection;
        URL url = new URL(endpoint);
        connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("X-Flipkart-Client", "test");
        connection.setRequestProperty( "Content-Type", "application/json");
        connection.setConnectTimeout(2000);
        connection.getOutputStream().write(objectMapper.writeValueAsBytes(repricingRequest));

        for (int retry = 0; retry < 10; retry++) {
            try {
                connection.connect();
            if (connection.getResponseCode() == 200) {
                return;
            } else {
                System.out.println("repricing request retry for " + repricingRequest.getListingIds());
            }
            connection.disconnect();
            } catch (Exception e) {
                continue;
            }
        }
        System.out.println("repricing retry exhausted for " + String.join(",",repricingRequest.getListingIds()));
    }

    private RepricingRequest getRepricingRequest(List<String> listingIds) {
        return new RepricingRequest(listingIds,0);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(!listingIds.isEmpty()) {
            try {
                postReprice(getRepricingRequest(listingIds));
            }
            catch (Exception e) {
                System.out.println("request failed");
            }
        }
        super.cleanup(context);
    }
}