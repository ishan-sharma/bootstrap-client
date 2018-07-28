package com.flipkart.planning;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import flipkart.pricing.libs.fatak.PriceResponseV2;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class CustomResponseHandler implements ResponseHandler<PriceResponseV2> {


    private final ObjectMapper objectMapper;

    @Inject
    public CustomResponseHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    public PriceResponseV2 handleResponse(HttpResponse httpResponse) throws IOException {

        if(httpResponse.getStatusLine().getStatusCode()==200) {
            PriceResponseV2 priceResponseV2 = objectMapper.readValue(EntityUtils.toString(httpResponse.getEntity())
                    , PriceResponseV2.class);

            return priceResponseV2;
        }
        else
            return null;
    }


}