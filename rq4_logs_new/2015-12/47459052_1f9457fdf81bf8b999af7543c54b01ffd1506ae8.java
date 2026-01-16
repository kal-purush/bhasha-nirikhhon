package data.scripts.campaign.submarkets;

import com.fs.starfarer.api.Global;
import com.fs.starfarer.api.campaign.*;
import com.fs.starfarer.api.campaign.CargoAPI.CargoItemType;
import com.fs.starfarer.api.campaign.econ.CommodityOnMarketAPI;
import com.fs.starfarer.api.campaign.econ.SubmarketAPI;
import com.fs.starfarer.api.combat.ShipAPI.HullSize;
import com.fs.starfarer.api.fleet.FleetMemberAPI;
import com.fs.starfarer.api.fleet.FleetMemberType;
import com.fs.starfarer.api.impl.campaign.ids.Commodities;
import com.fs.starfarer.api.impl.campaign.ids.Factions;
import com.fs.starfarer.api.impl.campaign.ids.ShipRoles;
import com.fs.starfarer.api.impl.campaign.submarkets.BaseSubmarketPlugin;
import com.fs.starfarer.api.loading.WeaponSpecAPI;
import com.fs.starfarer.api.util.Highlights;
import com.fs.starfarer.api.util.Misc;
import com.fs.starfarer.api.util.WeightedRandomPicker;

public class FS_SubmarketPlugin extends BaseSubmarketPlugin {

    private final RepLevel minStanding = RepLevel.WELCOMING;

    @Override
    public String getIllegalTransferText(CargoStackAPI stack, TransferAction action) {
        RepLevel req = getRequiredLevelAssumingLegal(stack, action);

        if (req != null) {
            return "Requires: " + submarket.getFaction().getDisplayName() + " - " + req.getDisplayName().toLowerCase();
        }

        return "Illegal to trade in " + stack.getDisplayName() + " here";
    }

    @Override
    public String getIllegalTransferText(FleetMemberAPI member, TransferAction action) {
        RepLevel req = getRequiredLevelAssumingLegal(member, action);

        if (req != null) {
            return "Req: " + submarket.getFaction().getDisplayName() + " - " + req.getDisplayName().toLowerCase();
        }

        return "Illegal to buy or sell here";
    }

    @Override
    public String getName() {
        return submarket.getFaction().getDisplayName();
    }

    @Override
    public OnClickAction getOnClickAction(CoreUIAPI ui) {
        return OnClickAction.OPEN_SUBMARKET;
    }

    @Override
    public String getTooltipAppendix(CoreUIAPI ui) {
        if (!isEnabled(ui)) {
            return "Requires: " + submarket.getFaction().getDisplayName() + " - " + minStanding.getDisplayName().toLowerCase();
        }
        return null;
    }

    @Override
    public Highlights getTooltipAppendixHighlights(CoreUIAPI ui) {
        String appendix = getTooltipAppendix(ui);
        if (appendix == null) {
            return null;
        }

        Highlights h = new Highlights();
        h.setText(appendix);
        h.setColors(Misc.getNegativeHighlightColor());
        return h;
    }

    @Override
    public void init(SubmarketAPI submarket) {
        super.init(submarket);
    }

    @Override
    public boolean isEnabled(CoreUIAPI ui) {
        RepLevel level = submarket.getFaction().getRelationshipLevel(Global.getSector().getFaction(Factions.PLAYER));
        return level.isAtWorst(minStanding);
    }

    @Override
    public boolean isIllegalOnSubmarket(String commodityId, TransferAction action) {
        boolean illegal = submarket.getFaction().isIllegal(commodityId);
        RepLevel req = getRequiredLevelAssumingLegal(commodityId, action);

        if (req == null) {
            return illegal;
        }

        RepLevel level = submarket.getFaction().getRelationshipLevel(Global.getSector().getFaction(Factions.PLAYER));
        boolean legal = level.isAtWorst(req);
        return !legal;
    }

    @Override
    public boolean isIllegalOnSubmarket(CargoStackAPI stack, TransferAction action) {
        if (stack.isCommodityStack()) {
            return isIllegalOnSubmarket((String) stack.getData(), action);
        }

        RepLevel req = getRequiredLevelAssumingLegal(stack, action);
        if (req == null) {
            return false;
        }

        RepLevel level = submarket.getFaction().getRelationshipLevel(Global.getSector().getFaction(Factions.PLAYER));

        boolean legal = level.isAtWorst(req);
        return !legal;
    }

    @Override
    public boolean isIllegalOnSubmarket(FleetMemberAPI member, TransferAction action) {
        RepLevel req = getRequiredLevelAssumingLegal(member, action);
        if (req == null) {
            return false;
        }

        RepLevel level = submarket.getFaction().getRelationshipLevel(Global.getSector().getFaction(Factions.PLAYER));

        boolean legal = level.isAtWorst(req);
        return !legal;
    }

    @Override
    public void updateCargoPrePlayerInteraction() {
        if (!okToUpdateCargo()) {
            return;
        }
        sinceLastCargoUpdate = 0f;

        CargoAPI cargo = getCargo();
        float stability = market.getStabilityValue();

        float availableFraction = 0.2f + 0.02f * stability;

        for (CommodityOnMarketAPI com : market.getAllCommodities()) {
            String id = com.getId();

            if (market.isIllegal(id)) {
                continue;
            }

            float desired = computeStockpileAvailableForSale(com) * availableFraction;
            desired = (int) desired;

            float current = cargo.getQuantity(CargoItemType.RESOURCES, id);

            if (desired > current) {
                cargo.addItems(CargoItemType.RESOURCES, id, (int) (desired - current));
            } else if (current > desired) {
                cargo.removeItems(CargoItemType.RESOURCES, id, (int) (current - desired));
            }
        }

        pruneWeapons(0.5f);
        addWeaponsBasedOnMarketSize(8, 4, null);

        addShips();
        cargo.sort();
    }

    private void addShips()
    {
        int marketSize = this.market.getSize();

        pruneShips(0.75F);

        WeightedRandomPicker<String> rolePicker = new WeightedRandomPicker<>();
        rolePicker.add(ShipRoles.COMBAT_SMALL, 0f);
        rolePicker.add(ShipRoles.COMBAT_MEDIUM, 0f);
        rolePicker.add(ShipRoles.COMBAT_LARGE, 3f);
        rolePicker.add(ShipRoles.COMBAT_CAPITAL, 1f);
        rolePicker.add(ShipRoles.INTERCEPTOR, 10f);
        rolePicker.add(ShipRoles.FIGHTER, 10f);
        rolePicker.add(ShipRoles.BOMBER, 10f);

        addShipsForRoles(2 + marketSize * 2, rolePicker, null);
        if (marketSize >= 4)
        {
            boolean hasCapital = false;
            for (FleetMemberAPI member : getCargo().getMothballedShips().getMembersListCopy()) {
                if (member.isCapital())
                {
                    hasCapital = true;
                    break;
                }
            }
            if (!hasCapital)
            {
                rolePicker.clear();
                rolePicker.add(ShipRoles.COMBAT_CAPITAL, 1.0F);
                addShipsForRoles(getCargo().getMothballedShips().getMembersListCopy().size() + 1, rolePicker, null);
            }
        }
    }
    private RepLevel getRequiredLevelAssumingLegal(CargoStackAPI stack, TransferAction action) {
        if (stack.isWeaponStack()) {
            if (action == TransferAction.PLAYER_BUY) {
                WeaponSpecAPI spec = stack.getWeaponSpecIfWeapon();
                if (!spec.getWeaponId().startsWith("tem_")) {
                    return RepLevel.VENGEFUL;
                }
                int tier = spec.getTier();
                switch (tier) {
                    case 3: return RepLevel.FRIENDLY;
                    case 4: return RepLevel.COOPERATIVE;
                    default: return RepLevel.COOPERATIVE;
                }
            }
            return RepLevel.VENGEFUL;
        }

        if (!stack.isCommodityStack()) {
            return null;
        }
        return getRequiredLevelAssumingLegal((String) stack.getData(), action);
    }

    private RepLevel getRequiredLevelAssumingLegal(String commodityId, TransferAction action) {
        if (action == TransferAction.PLAYER_SELL) {
            return RepLevel.VENGEFUL;
        }

        if (submarket.getFaction().isIllegal(commodityId)) {
            return null;
        }

        CommodityOnMarketAPI com = market.getCommodityData(commodityId);
        if (com.isPersonnel()) {
            return RepLevel.COOPERATIVE;
        }
        if (com.getId().contentEquals(Commodities.HAND_WEAPONS) || com.getId().contentEquals(Commodities.RARE_METALS) || com.getId().contentEquals(
                Commodities.RARE_ORE)) {
            return RepLevel.FRIENDLY;
        }
        return RepLevel.WELCOMING;
    }

    private RepLevel getRequiredLevelAssumingLegal(FleetMemberAPI member, TransferAction action) {
        if (action == TransferAction.PLAYER_BUY) {
            int fp = member.getFleetPointCost();
            HullSize size = member.getHullSpec().getHullSize();

            if (size == HullSize.FIGHTER || size == HullSize.FRIGATE) {
                return RepLevel.FRIENDLY;
            } else {
                return RepLevel.COOPERATIVE;
            }
        }
        return null;
    }
}