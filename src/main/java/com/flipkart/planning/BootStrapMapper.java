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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class BootStrapMapper extends Mapper<LongWritable, Text, Text, Text> {
    private KafkaProducer<String, PricingNotificationKafkaMessage> kafkaProducer = null;
    private List<String> listingIds = new ArrayList<>();
    private int batchSize = 0;


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.33.18.124:9092,10.34.175.1:9092, 10.33.59.73:9092,10.33.219.21:9092, 10.34.181.45:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaSerializer.class.getName());
       kafkaProducer =
                new KafkaProducer<>(props);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        List<String> fatakListings = new ArrayList<>();

        listingIds.add(line);
        batchSize++;

        if(batchSize < 30) {
            return;
        }
//        System.out.println("Executing line " + line);
//        System.out.println(line);

        String commaSeparatedListingIds = listingIds.stream().collect(Collectors.joining(","));
        try {
            PriceResponseV2 priceResponseV2 = getFatakResponse(commaSeparatedListingIds);
            if(priceResponseV2 == null)
            {
                context.write(new Text(commaSeparatedListingIds), new Text("F"));
                batchSize = 0;
                listingIds.clear();
                return;
            }
            else {
                priceResponseV2.getSuccess().forEach((PriceObject priceObject) ->
                        fatakListings.add(priceObject.getListingId()));
            }

            String commaSeparatedListingIdsForZulu = fatakListings.stream().collect(Collectors.joining(","));
            ZuluViewResponse zuluViewResponse = getZuluViewResponse(commaSeparatedListingIdsForZulu);
            if (zuluViewResponse == null) {
                context.write(new Text(commaSeparatedListingIdsForZulu), new Text("Z"));
            } else {
                zuluViewResponse.getEntityViews().forEach((EntityView entityView) -> {
                    String listingId = entityView.getEntityId();

                    PricingNotificationKafkaMessage pricingNotificationKafkaMessage = new PricingNotificationKafkaMessage(
                            listingId,
                            "pricing",
                            System.currentTimeMillis());

                    ProducerRecord<String, PricingNotificationKafkaMessage> producerRecord =
                            new ProducerRecord<>("tac-listing-updates-bootstrap"
                                    , listingId
                                    , pricingNotificationKafkaMessage);
                    kafkaProducer.send(producerRecord);
                });
                //TODO: log entity id in hdfs file no 3 unavailableView.getEntityId(); (for zulu view not available)
                for (UnavailableView unavailableView : zuluViewResponse.getUnavailableViews()) {
                    context.write(new Text(unavailableView.getEntityId()), new Text("UZ"));
                }
            }

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

    private PriceResponseV2 getFatakResponse(String listingIds) throws IOException {
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

    private ZuluViewResponse getZuluViewResponse(String listingId) throws IOException {
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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Thread.sleep(5000);
        kafkaProducer.close();
        for(String listingId: listingIds) {
            context.write(new Text(listingId), new Text("I"));
        }
        super.cleanup(context);
    }
}