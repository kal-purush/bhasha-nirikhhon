package dev.orion.api.endpoint.body;

import dev.orion.entity.Activity;
import dev.orion.entity.GroupActivity;
import dev.orion.entity.User;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.*;
import java.util.stream.Collectors;

@Setter
@Getter
@NoArgsConstructor
public class StartActivityResponseBody {
    @Schema(required = true, example = "{70f4bb61-4c6e-4aac-bb1d-56e4247e16b3: [be638c24-34de-4771-8e18-7af05ab8962d]}")
    private final Map<UUID, List<String>> groups = new HashMap<>();

    @Schema(required = true, example = "372bf2a5-0da3-47bd-8c94-4a09d25d362a")
    private UUID activityUUID;

    public StartActivityResponseBody(Activity activity) {
        val groupActivities = activity.getGroupActivities();
        groupActivities.forEach(this::addGroup);
        this.activityUUID = activity.getUuid();
    }

    public void addGroup(GroupActivity groupActivity) {
        val userIdList = groupActivity.getParticipants().stream().map(User::getExternalId).collect(Collectors.toList());
        groups.put(groupActivity.getUuid(), userIdList);
    }
}