package huige233.transcend.event;

import net.minecraft.entity.Entity;
import net.minecraft.util.math.BlockPos;
import net.minecraftforge.event.entity.EntityEvent;
import net.minecraftforge.fml.common.eventhandler.Cancelable;

import javax.annotation.Nonnull;

@Cancelable
public class TeleportEntityEvent extends EntityEvent {
    private @Nonnull BlockPos targetPos;

    private int dimension;


    /**
     * Fired before an entity teleports to the given location.
     *
     * @param entity
     *          The entity teleporting
     * @param pos
     *          The target coord
     */
    public TeleportEntityEvent(@Nonnull Entity entity, @Nonnull BlockPos pos, int dimension) {
        super(entity);
        this.targetPos = pos;
        this.setDimension(dimension);
    }

    public @Nonnull BlockPos getTarget() {
        return targetPos;
    }

    public void setTargetPos(@Nonnull BlockPos target) {
        this.targetPos = target;
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }
}