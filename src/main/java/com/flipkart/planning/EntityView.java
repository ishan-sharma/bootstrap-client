package com.flipkart.planning;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.util.Map;

@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityView implements Serializable {

    @JsonProperty("entityId")
    private String entityId;

    @JsonProperty("entityVersion")
    private Long entityVersion;

    @JsonProperty("entityType")
    private String entityType;

    @JsonProperty("entitySubType")
    private String entitySubType;

    @JsonProperty("viewName")
    private String viewName;

    @JsonProperty("viewDefinitionVersion")
    private Long viewDefinitionVersion;

    @JsonProperty("viewVersion")
    private Long viewVersion;

    @JsonProperty("view")
    private Map<String, Object> view;
}
