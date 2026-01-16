package mod.ckenja.relocate.content.funnel;

import com.simibubi.create.foundation.blockEntity.SmartBlockEntity;
import com.simibubi.create.foundation.blockEntity.behaviour.filtering.FilteringBehaviour;
import com.simibubi.create.foundation.blockEntity.behaviour.inventory.InvManipulationBehaviour;
import net.minecraft.world.item.ItemStack;

import java.util.function.Predicate;

public class RelocateInvManipulationBehaviour extends InvManipulationBehaviour {
    public RelocateInvManipulationBehaviour(SmartBlockEntity be, InterfaceProvider target) {
        super(be, target);
    }

    @Override
    protected Predicate<ItemStack> getFilterTest(Predicate<ItemStack> customFilter) {
        Predicate<ItemStack> test = customFilter;
        FilteringBehaviour filter = blockEntity.getBehaviour(FilteringBehaviour.TYPE);
        if (filter != null)
            test = customFilter.and(itemStack -> !filter.test(itemStack));
        return test;
    }
}