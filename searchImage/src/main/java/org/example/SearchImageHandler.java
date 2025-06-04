package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.util.*;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;


public class SearchImageHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Logger logger = LoggerFactory.getLogger(SearchImageHandler.class);

    private static final Region REGION = Region.EU_CENTRAL_1;
    private static final String TABLE_NAME = System.getenv("DYNAMODB_TABLE_NAME");
    private static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");

    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;

    public record Image(String imageName, String imageData, String contentType) {}

    public SearchImageHandler() {
        this.s3Client = S3Client.builder().region(REGION).build();
        this.dynamoDbClient = DynamoDbClient.builder().region(REGION).endpointDiscoveryEnabled(false).build();
        objectMapper = new ObjectMapper();
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        try {
            String query = extractQuery(input);
            if (isNull(query) || query.trim().isEmpty()) {
                return createErrorResponse(400, "Query parameter is required");
            }

            logger.info("Searching for images with label: {}", query);

            List<String> imageNames = searchImagesByLabel(query.toLowerCase());

            if (imageNames.isEmpty()) {
                logger.info("No matching images found");
                return createSuccessResponse(Collections.emptyList());
            }

            logger.info("Found {} matching images", imageNames.size());

            List<Image> images = loadImagesFromS3(imageNames);

            return createSuccessResponse(images);

        } catch (Exception e) {
            logger.info("Error processing request: {}", e.getMessage());
            return createErrorResponse(500, "Internal server error: " + e.getMessage());
        }
    }

    private String extractQuery(APIGatewayProxyRequestEvent input) {
        if (nonNull(input.getQueryStringParameters()) && input.getQueryStringParameters().containsKey("keyword")) {
            return input.getQueryStringParameters().get("keyword");
        }

        return null;
    }

    private List<String> searchImagesByLabel(String query) {
        List<String> imageIds = new ArrayList<>();
        logger.info("Searching query: {}", query);
        try {
            ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(TABLE_NAME)
                    .build();

            ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

            for (Map<String, AttributeValue> item : scanResponse.items()) {
                AttributeValue labelsAttribute = item.get("labels");
                logger.info("Searching for images with label: {}", labelsAttribute);
                if (nonNull(labelsAttribute) && nonNull(labelsAttribute.ss())) {
                    boolean hasMatchingLabel = labelsAttribute.ss().stream()
                            .anyMatch(label -> label.toLowerCase().contains(query));

                    logger.info("Found matching images with label: {}", hasMatchingLabel);
                    if (hasMatchingLabel) {
                        AttributeValue primaryKeyAttribute = item.get("imageId");
                        if (nonNull(primaryKeyAttribute) && nonNull(primaryKeyAttribute.s())) {
                            imageIds.add(primaryKeyAttribute.s());
                        }
                    }
                }
            }
        } catch (DynamoDbException e) {
            throw new RuntimeException("Error scanning DynamoDB table: " + e.getMessage(), e);
        }

        return imageIds;
    }

    private List<Image> loadImagesFromS3(List<String> imageNames) {
        return imageNames.stream()
                .map(this::loadImageFromS3)
                .filter(Objects::nonNull)
                .toList();
    }

    private Image loadImageFromS3(String imageName) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(imageName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);

            return new Image(
                    imageName,
                    Base64.getEncoder().encodeToString(objectBytes.asByteArray()),
                    nonNull(objectBytes.response().contentType()) ?
                            objectBytes.response().contentType() : "image/jpeg"
            );

        } catch (Exception e) {
            System.err.println("Error loading image " + imageName + ": " + e.getMessage());
            return null;
        }
    }

    private APIGatewayProxyResponseEvent createSuccessResponse(List<Image> images) {
        try {
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("success", true);
            responseBody.put("images", images);

            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(200)
                    .withHeaders(Map.of(
                            "Content-Type", "application/json",
                            "Access-Control-Allow-Origin", "*"
                    ))
                    .withBody(objectMapper.writeValueAsString(responseBody));

        } catch (Exception e) {
            return createErrorResponse(500, "Error creating response: " + e.getMessage());
        }
    }

    private APIGatewayProxyResponseEvent createErrorResponse(int statusCode, String message) {
        try {
            Map<String, Object> errorBody = new HashMap<>();
            errorBody.put("success", false);
            errorBody.put("error", message);

            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(statusCode)
                    .withHeaders(Map.of(
                            "Content-Type", "application/json",
                            "Access-Control-Allow-Origin", "*"
                    ))
                    .withBody(objectMapper.writeValueAsString(errorBody));

        } catch (Exception e) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(500)
                    .withBody("{\"success\":false,\"error\":\"Internal server error\"}");
        }
    }
}
