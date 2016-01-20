package com.example;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;

class AmazonSqsClientFactory {
    public static AmazonSQSClient create() {
        // here we can read everything from config or get it injected
        final AmazonSQSClient client = new AmazonSQSClient();
        Region region = Region.getRegion(Regions.EU_WEST_1);
        client.setRegion(region);

        return client;
    }
}
