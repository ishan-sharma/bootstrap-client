package com.flipkart.planning;

import flipkart.dsp.santa.bernard.model.pricing.PricingNotificationKafkaMessage;
import flipkart.pricing.libs.fatak.PriceResponseV2;
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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.io.IOException;

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
            String endpoint = "http://10.47.0.106:80/v2/listings/"+line;

            HttpURLConnection connection;
            URL url = new URL(endpoint);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("X-Flipkart-Client", "kaizen");
            connection.setRequestProperty("X-Request-ID", "dummy");
            connection.setRequestProperty("X-Request-Host", "test");
            connection.connect();

            if(connection.getResponseCode() == 200) {
                System.out.println("response is " + connection.getResponseMessage());
                System.out.println("executing request");
                System.out.println(producerRecord.value().getListingId());
                if(kafkaProducer == null) {
                    System.out.println("kafkaproducer is null");
                }
                kafkaProducer.send(producerRecord);
            }
            else {
                System.out.println("request failed for " + line);
                System.out.println("error code is " + connection.getResponseCode());
            }

        }
        catch (Exception e) {
            System.out.println("request failed");
        }
    }
}
