package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;

import java.util.Map;


public class UploadImageHandler implements RequestHandler<S3Event, String> {

    private static final Logger logger = LoggerFactory.getLogger(UploadImageHandler.class);

    private final Region REGION = Region.EU_CENTRAL_1;

    private final RekognitionClient rekognitionClient = RekognitionClient.builder().region(REGION).build();
    private final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(REGION).build();

    private final String TABLE_NAME = System.getenv("DYNAMODB_TABLE_NAME");

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        if (s3Event == null || s3Event.getRecords() == null || s3Event.getRecords().isEmpty()) {
            logger.warn("Received empty or null S3 event");
            return "No records to process";
        }

        if (TABLE_NAME == null || TABLE_NAME.isEmpty()) {
            logger.error("DYNAMODB_TABLE_NAME environment variable is not set");
            throw new IllegalStateException("Missing required environment variable: DYNAMODB_TABLE_NAME");
        }

        try {
            for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
                processRecord(record, context);
            }
            return "Successfully processed " + s3Event.getRecords().size() + " records";
        } catch (Exception e) {
            logger.error("Error processing S3 event", e);
            throw e;
        }
    }

    private void processRecord(S3EventNotification.S3EventNotificationRecord record, Context context) {
        try {
            logger.info("Processing record: {}", record);

            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getKey();

            logger.info("Processing image - Bucket: {}, Key: {}", srcBucket, srcKey);

            if (!isValidImageFile(srcKey)) {
                logger.info("Skipping non-image file: {}", srcKey);
                return;
            }

            DetectLabelsResponse recognitionLabelsResponse = detectLabels(srcBucket, srcKey);
            logger.info("Detected {} labels for image: {}", recognitionLabelsResponse.labels().size(), srcKey);

            PutItemResponse putLabelsResponse = putLabels(srcKey, recognitionLabelsResponse);
            logger.info("Successfully stored labels for image: {}", srcKey);

        } catch (Exception e) {
            logger.error("Error processing record for key: {}", record.getS3().getObject().getKey(), e);
        }
    }

    private boolean isValidImageFile(String key) {
        String lowerKey = key.toLowerCase();
        return lowerKey.endsWith(".jpg") || lowerKey.endsWith(".jpeg") || lowerKey.endsWith(".png") || lowerKey.endsWith(".gif") || lowerKey.endsWith(".bmp") || lowerKey.endsWith(".webp");
    }

    private DetectLabelsResponse detectLabels(String bucket, String key) {
        var detectLabelsRequest = DetectLabelsRequest.builder().image(Image.builder().s3Object(S3Object.builder().bucket(bucket).name(key).build()).build()).build();

        return rekognitionClient.detectLabels(detectLabelsRequest);
    }


    private PutItemResponse putLabels(String key, DetectLabelsResponse response) {
        Map<String, AttributeValue> item = Map.of(
                "imageId", AttributeValue.fromS(key),
                "labels", AttributeValue.fromSs(
                        response.labels().stream()
                                .map(Label::name)
                                .toList()
                ),
                "timestamp", AttributeValue.fromN(String.valueOf(System.currentTimeMillis() / 1000))
        );

        return dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(item)
                .build());
    }
}
