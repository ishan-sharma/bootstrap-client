package com.flipkart.planning;

import com.fasterxml.jackson.databind.ObjectMapper;
import flipkart.dsp.santa.bernard.model.pricing.PricingNotificationKafkaMessage;
import flipkart.pricing.libs.fatak.PriceObject;
import flipkart.pricing.libs.fatak.PriceResponseV2;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class BootStrapMapperTest {

    private static KafkaProducer<String, PricingNotificationKafkaMessage> kafkaProducer = null;
    private static List<String> listingIds = new ArrayList<>();
    private static int batchSize = 0;

    public static void setup() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaSerializer.class.getName());
        kafkaProducer =
                new KafkaProducer<>(props);
    }


    protected static void map(String value) throws IOException, InterruptedException {
        String line = value;
        List<String> fatakListings = new ArrayList<>();

        listingIds.add(line);
        batchSize++;

        if (batchSize < 30) {
            //TODO: identify weather system.exit is correct ?
            System.out.println("batchsize is "+batchSize);
            return;
        }
        System.out.println("batchsize is 30 "+batchSize);

        System.out.println(line);

        String commaSeparatedListingIds = listingIds.stream().collect(Collectors.joining(","));
        System.out.println("Executing line " + commaSeparatedListingIds);
        try {
            PriceResponseV2 priceResponseV2 = getFatakResponse(commaSeparatedListingIds);
            if (priceResponseV2 == null) {
                //TODO: write in hdfs file no1 fatak response not 200 after 3 retries
                batchSize = 0;
                listingIds.clear();
                return;
            } else {
                priceResponseV2.getSuccess().forEach((PriceObject priceObject) ->
                        fatakListings.add(priceObject.getListingId()));
            }

            String commaSeparatedListingIdsForZulu = fatakListings.stream().collect(Collectors.joining(","));
            ZuluViewResponse zuluViewResponse = getZuluViewResponse(commaSeparatedListingIdsForZulu);
            if (zuluViewResponse == null) {
                //TODO: write in hdfs file no2 zulu response not 200 after 3 retries (for zulu retries)
            } else {
                zuluViewResponse.getEntityViews().forEach((EntityView entityView) -> {
                    String listingId = entityView.getEntityId();

                    PricingNotificationKafkaMessage pricingNotificationKafkaMessage = new PricingNotificationKafkaMessage(
                            listingId,
                            "pricing",
                            System.currentTimeMillis());

                    ProducerRecord<String, PricingNotificationKafkaMessage> producerRecord =
                            new ProducerRecord<>("tac-updates-preindex"
                                    , line
                                    , pricingNotificationKafkaMessage);
                    kafkaProducer.send(producerRecord);
                });
                zuluViewResponse.getUnavailableViews().forEach((UnavailableView unavailableView) -> {
                    //TODO: log entity id in hdfs file no 3 unavailableView.getEntityId(); (for zulu view not available)
                });
            }

        } catch (Exception e) {
            System.out.println("request failed");
        } finally {
            if (batchSize == 30) {
                batchSize = 0;
                listingIds.clear();
            }
        }

    }

    private static PriceResponseV2 getFatakResponse(String listingIds) throws IOException {
        String endpoint = "http://10.47.0.106:80/v2/listings/" + listingIds;

        HttpURLConnection connection;
        URL url = new URL(endpoint);
        connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("GET");
        connection.setRequestProperty("X-Flipkart-Client", "kaizen");
        connection.setRequestProperty("X-Request-ID", "dummy");
        connection.setRequestProperty("X-Request-Host", "test");
        connection.setReadTimeout(2000);
        connection.setConnectTimeout(2000);
        PriceResponseV2 priceResponseV2 = null;
        for (int retry = 0; retry < 10; retry++) {
            try {
                connection.connect();
                if (connection.getResponseCode() == 200) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String message = IOUtils.toString(bufferedReader);
                    priceResponseV2 = new ObjectMapper().readValue(message, PriceResponseV2.class);
                    connection.disconnect();
                    return priceResponseV2;
                } else {
                    System.out.println("fatak request retry for " + listingIds);
                }
                connection.disconnect();
            } catch (Exception e) {
                continue;
            }
        }
        System.out.println("fatak retry exhausted for " + listingIds);
        return null;

    }

    private static ZuluViewResponse getZuluViewResponse(String listingId) throws IOException {
        String endpoint = "http://10.47.1.8:31200/views?viewNames=dsp_pricing&entityIds=" + listingId;

        HttpURLConnection connection;
        URL url = new URL(endpoint);
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("z-clientId", "retail.pricing");
        connection.setRequestProperty("z-requestId", String.valueOf(UUID.randomUUID()));
        connection.setRequestProperty("z-timestamp", String.valueOf(System.currentTimeMillis()));
        connection.setReadTimeout(2000);
        connection.setConnectTimeout(2000);
        ZuluViewResponse zuluViewResponse = null;
        for (int retry = 0; retry < 10; retry++) {
            try {
                connection.connect();
                if (connection.getResponseCode() == 200) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String message = IOUtils.toString(bufferedReader);

                    zuluViewResponse = new ObjectMapper().readValue(message, ZuluViewResponse.class);
                    connection.disconnect();
                    return zuluViewResponse;
                } else {
                    System.out.println("zulu request retry for " + listingIds);
                }
                connection.disconnect();
            } catch (Exception e) {
                continue;
            }
        }
        System.out.println("zulu retry exhausted for " + listingIds);
        return null;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        setup();
        String filePath = args[0];
        String line;
        File file = new File(filePath);

        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        while ((line = bufferedReader.readLine()) != null) {
            map(line.trim());
        }
        System.out.print("appended the file");
        Thread.sleep(4000);
    }
}
