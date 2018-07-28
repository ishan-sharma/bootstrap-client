package com.flipkart.planning;

import flipkart.dsp.santa.bernard.model.pricing.PricingNotificationKafkaMessage;
import flipkart.pricing.libs.fatak.PriceResponseV2;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.io.IOException;
import java.util.UUID;

public class BootStrapMapper extends Mapper<LongWritable, Text, Text, Text> {
    private KafkaProducer<String, PricingNotificationKafkaMessage> kafkaProducer = null;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.34.37.255:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaSerializer.class.getName());
       kafkaProducer =
                new KafkaProducer<String, PricingNotificationKafkaMessage>(props);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("Executing line " + line);
        System.out.println(line);


        PricingNotificationKafkaMessage pricingNotificationKafkaMessage = new PricingNotificationKafkaMessage(
                line,
                "pricing",
                System.currentTimeMillis());

        ProducerRecord<String, PricingNotificationKafkaMessage> producerRecord =
                new ProducerRecord<String, PricingNotificationKafkaMessage>("tac_bootstrap_test"
                        ,line
                        ,pricingNotificationKafkaMessage);

        try {
            PriceResponseV2 priceResponseV2 = getFatakResponse(line);

            if(priceResponseV2!=null && priceResponseV2.getSuccess().size()==1 && priceResponseV2.getFailure().size()==0)
            {
                ZuluViewResponse zuluViewResponse = getZuluViewResponse(line);

                if ((zuluViewResponse != null && zuluViewResponse.getEntityViews().size() == 1
                        && zuluViewResponse.getUnavailableViews().size() == 0)) {
                    kafkaProducer.send(producerRecord).get();
                }
                else {
                    //TODO: to write in line variable in HDFS
                    context.write(new Text(line), new Text("1"));
                }
            }
        }
        catch (Exception e) {
            System.out.println("request failed");
        }
    }

    private PriceResponseV2 getFatakResponse(String listingId) throws IOException {
        String endpoint = "http://10.47.0.106:80/v2/listings/" + listingId;

        HttpURLConnection connection;
        URL url = new URL(endpoint);
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("X-Flipkart-Client", "kaizen");
        connection.setRequestProperty("X-Request-ID", "dummy");
        connection.setRequestProperty("X-Request-Host", "test");
        connection.connect();

        if(connection.getResponseCode() == 200) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String message = IOUtils.toString(bufferedReader);

            PriceResponseV2 priceResponseV2 = new ObjectMapper().readValue(message,PriceResponseV2.class);
            return priceResponseV2;
        }
        else {
            System.out.println("fatak request failed for " + listingId);
            return null;
        }
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
        connection.connect();

        if(connection.getResponseCode() == 200) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String message = IOUtils.toString(bufferedReader);

            ZuluViewResponse zuluViewResponse = new ObjectMapper().readValue(message,ZuluViewResponse.class);
            return zuluViewResponse;
        }
        else {
            System.out.println("zulu request failed for " + listingId + "response code " + connection.getResponseCode());
            return null;
        }
    }
}
