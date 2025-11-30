package nz.com.reece.product.listener;


import lombok.extern.slf4j.Slf4j;
import nz.com.reece.product.configuration.ProductMasterPollerConfiguration;
import nz.com.reece.product.service.ProductMasterEventService;
import nz.com.reece.product.transformer.ProductMasterDataMessageTransformer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

@Slf4j
@Component
@ConditionalOnProperty("listener.productMaster.polling.enabled")
public class ProductMasterEventPoller extends SqsEventPoller {
    private static final String POLLER_NAME = "product-master-data-sync(consumer)";
    private ProductMasterEventService productMasterService;
    private ProductMasterDataMessageTransformer productMasterMessageTransformer;

    protected ProductMasterEventPoller(SqsClient sqsClient, ProductMasterPollerConfiguration productMasterPollerConfiguration,
                                       ProductMasterEventService productMasterService,
                                       ProductMasterDataMessageTransformer productMasterMessageTransformer) {
        super(sqsClient, productMasterPollerConfiguration, POLLER_NAME);
        this.productMasterService = productMasterService;
        this.productMasterMessageTransformer = productMasterMessageTransformer;
    }


    @Override
    @Scheduled(fixedDelayString = "${listener.productMaster.pollingInterval}", initialDelayString = "${listener.productMaster.pollingInitialDelay}")
    public void pollMessages() {
        super.pollMessages();
    }

    @Override
    protected void processMessage(Message message) {
        log.info("Starting to process {} message id {} message {} with attributes {}",
                POLLER_NAME, message.messageId(), message.body(), message.messageAttributes());
        final var productMasterData = productMasterMessageTransformer.getProductMasterData(message);
        productMasterService.processEvent(productMasterData);
        deleteMessage(message);
    }

}
