package nz.com.reece.product.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@SuppressWarnings("PMD.TooManyFields")
public class ProductMasterDataChangeEvent implements Serializable {

    private static final long serialVersionUID = 3821845662123287381L;

    private long eventId;

    private long referenceId;

    private String code;

    private String entityType;

    private String eventType;

    private Integer status;

    private boolean active;

    private String description;

    private long parentId;

    private String parentCode;

    private String name;

    private String image;

    private String referenceCode;

    private String type;

    private String eventStatus;

    private ProductMasterDataChangeEventMapping mapping;

}
