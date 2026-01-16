package refooding.api.domain.exchange.dto;


import lombok.AllArgsConstructor;
import lombok.Getter;
import refooding.api.domain.exchange.entity.Region;

import java.util.List;

public class RegionResponse{

    @Getter
    @AllArgsConstructor
    public static class ParentRegionResponse{
        private Long id;
        private String name;
        private List<ChildRegion> childRegions;

        public static ParentRegionResponse from(Region region){
            return new ParentRegionResponse(
                    region.getId(),
                    region.getName(),
                    region.getChildren().stream()
                            .map(ChildRegion::from)
                            .toList()
            );
        }
    }

    @Getter
    @AllArgsConstructor
    public static class ChildRegion{
        private Long id;
        private String name;

        public static ChildRegion from(Region region) {
            return new ChildRegion(region.getId(), region.getName());
        }
    }

}
