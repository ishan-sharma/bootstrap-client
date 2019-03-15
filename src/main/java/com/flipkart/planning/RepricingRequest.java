package com.flipkart.planning;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class RepricingRequest {
    private final List<String> listingIds;
    private final long timestamp;
}
