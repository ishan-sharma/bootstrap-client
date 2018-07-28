package com.flipkart.planning;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.util.List;

@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZuluViewResponse implements Serializable {

    @JsonProperty("entityViews")
    private List<EntityView> entityViews;

    @JsonProperty("unavailableViews")
    private List<UnavailableView> unavailableViews;
}
