package nz.com.reece.product.publisher;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@ConditionalOnProperty("productMasterDataChangeEvent.publishing.enabled")
public class ProductMasterDataChangeEventPublisher {

    private final ProductSectionEventsPublisher productSectionEventsPublisher;
    private final ProductCategoryEventsPublisher productCategoryEventsPublisher;
    private final ProductStatusEventsPublisher productStatusEventsPublisher;
    private final ProductUOMEventsPublisher productUOMEventsPublisher;
    private final ProductStatusControlEnabledEventsPublisher productStatusControlEnabledEventsPublisher;
    private final ProductDangerousGoodEventsPublisher productDangerousGoodEventsPublisher;
    private final ProductBrandsEventsPublisher productBrandEventsPublisher;

    @Value("${productMasterDataPublisherEvent.sql.maxEvents}")
    private int maxEvents;

    @Value("${productMasterDataChangeEvent.masterDataItems.enabled}")
    private List<String> masterDataItems;

    public ProductMasterDataChangeEventPublisher(
            ProductSectionEventsPublisher productSectionEventsPublisher,
            ProductCategoryEventsPublisher productCategoryEventsPublisher,
            ProductStatusEventsPublisher productStatusEventsPublisher,
            ProductUOMEventsPublisher productUOMEventsPublisher,
            ProductStatusControlEnabledEventsPublisher productStatusControlEnabledEventsPublisher,
            ProductDangerousGoodEventsPublisher productDangerousGoodEventsPublisher,
            ProductBrandsEventsPublisher productBrandEventsPublisher) {
        this.productSectionEventsPublisher = productSectionEventsPublisher;
        this.productCategoryEventsPublisher = productCategoryEventsPublisher;
        this.productStatusEventsPublisher = productStatusEventsPublisher;
        this.productUOMEventsPublisher = productUOMEventsPublisher;
        this.productStatusControlEnabledEventsPublisher = productStatusControlEnabledEventsPublisher;
        this.productDangerousGoodEventsPublisher = productDangerousGoodEventsPublisher;
        this.productBrandEventsPublisher = productBrandEventsPublisher;
    }

    @Scheduled(fixedDelayString = "${productMasterDataChangeEvent.publishingInterval}",
            initialDelayString = "${productMasterDataChangeEvent.publishingInitialDelay}")
    public void pollEvents() {
        for (String masterDataItem : masterDataItems) {
            switch (masterDataItem) {
                case "productSection":
                    productSectionEventsPublisher.fetchProductSectionEvents(maxEvents);
                    break;
                case "productCategory":
                    productCategoryEventsPublisher.fetchProductCategoryEvents(maxEvents);
                    break;
                case "productStatus":
                    productStatusEventsPublisher.fetchProductStatusEvents(maxEvents);
                    break;
                case "productUOM":
                    productUOMEventsPublisher.fetchProductUOMEvents(maxEvents);
                    break;
                case "productStatusControlEnabled":
                    productStatusControlEnabledEventsPublisher.fetchProductStatusControlEnabledEvents(maxEvents);
                    break;
                case "productDangerousGood":
                    productDangerousGoodEventsPublisher.fetchProductDangerousGoodEvents(maxEvents);
                    break;
                case "productBrands":
                    productBrandEventsPublisher.fetchProductBrandEvents(maxEvents);
                    break;
                default:
                    break;
            }
        }
    }

}
