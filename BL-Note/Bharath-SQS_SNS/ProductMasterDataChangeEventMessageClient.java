package nz.com.reece.product.message.client;

import com.fasterxml.jackson.jr.ob.JSON;
import lombok.extern.slf4j.Slf4j;
import nz.com.reece.product.message.ProductMasterBuildEventRequest;
import nz.com.reece.product.message.ProductMasterDataEventRequest;
import nz.com.reece.product.model.ProductMasterDataChangeEventMapping;
import nz.com.reece.product.util.ProductMasterDataUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.Objects;

@Service
@Slf4j
public class ProductMasterDataChangeEventMessageClient {

    private final String productMasterDataChangeEventTopic;
    private final SnsClient snsClient;
    private final JSON json;
    private final ProductMasterDataUtil productMasterDataUtil;
    private static final String MESSAGE_GROUP_ID = "product-master-data";

    @Autowired
    public ProductMasterDataChangeEventMessageClient(
            @Value("${productMasterDataChangeEvent.topicName}") String productMasterDataChangeEventTopic,
            SnsClient snsClient, JSON json,
            ProductMasterDataUtil productMasterDataUtil) {
        this.productMasterDataChangeEventTopic = productMasterDataChangeEventTopic;
        this.snsClient = snsClient;
        this.json = json;
        this.productMasterDataUtil = productMasterDataUtil;
    }

    public int sendNotification(ProductMasterDataEventRequest productMasterDataEventRequest) {
        int httpStatusCode = 0;
        try {
            ProductMasterBuildEventRequest productMasterBuildEventRequest = (productMasterDataEventRequest);
            log.info("Publishing to topic {}, message: {} ", productMasterDataChangeEventTopic, json.asString(productMasterBuildEventRequest));
            PublishRequest request = PublishRequest.builder()
                    .message(json.asString(productMasterBuildEventRequest))
                    .messageGroupId(MESSAGE_GROUP_ID)
                    .messageDeduplicationId(productMasterDataEventRequest.getCorrelationId())
                    .topicArn(productMasterDataChangeEventTopic)
                    .messageAttributes(productMasterDataUtil.createMessageAttributes(productMasterDataEventRequest)).build();
            PublishResponse result = snsClient.publish(request);
            httpStatusCode = result.sdkHttpResponse().statusCode();
            log.info("Message successfully sent {}  wigetProductMasterBuildEventRequestth status: {} ", result.messageId(),  httpStatusCode);
            return httpStatusCode;
        } catch (Exception ex) {
            log.error("Failed to publish to the topic {}", ex.getMessage());
        }
        return httpStatusCode;
    }

    private ProductMasterBuildEventRequest getProductMasterBuildEventRequest(ProductMasterDataEventRequest productMasterDataEventRequest) {
        return  ProductMasterBuildEventRequest.builder()
                .id(productMasterDataEventRequest.getReferenceId())
                .status(getValue(productMasterDataEventRequest.getStatus()))
                .code(getValue(productMasterDataEventRequest.getCode()))
                .description(getValue(productMasterDataEventRequest.getDescription()))
                .parentId(productMasterDataEventRequest.getParentId())
                .parentCode(getValue(productMasterDataEventRequest.getParentCode()))
                .name(getValue(productMasterDataEventRequest.getName()))
                .image(getValue(productMasterDataEventRequest.getImage()))
                .type(getValue(productMasterDataEventRequest.getType()))
                .mapping(Objects.nonNull(productMasterDataEventRequest.getMapping())
                        ? productMasterDataEventRequest.getMapping()
                        : new ProductMasterDataChangeEventMapping(0, StringUtils.EMPTY))
                .build();
    }

    private String getValue(String value) {
        return StringUtils.isNotEmpty(value) ? value.trim() : StringUtils.EMPTY;
    }

}
