package latmod.minescape.enums;

public enum Pickaxe

{
	BRONZE("bronze_pickaxe", 1, 5, 1, 0),
	IRON("iron_pickaxe", 1, 20, 10, 0),
	STEEL("steel_pickaxe", 6, 35, 20, 0),
	MITHRIL("mithril_pickaxe", 21, 55, 30, 0),
	ADAMANT("adamant_pickaxe", 31, 75, 40, 0),
	RUNE("rune_pickaxe", 41, 90, 50, 0),
	INFERNO_ADZE("inferno_adze", 41, 0, 1, 92),
	SACRED("sacred_clay_pickaxe", 50, 0, 1, 0),
	VOLATILE("volatile_clay_pickaxe", 50, 0, 1, 0),
	DRAGON("dragon_pickaxe", 61, 0, 60, 0),
	CRYSTAL("crystal_pickaxe", 71, 0, 70, 0),
	IMCANDO("imcando_pickaxe", 81, 0, 80, 0),
	;
	
	public static final Pickaxe[] VALUES = values();
	
	public final String ID;
	
	/* Required level to craft pickaxe */
	public final int smithing_level;
	/*Required level to use pickaxe*/
	public final int mining_level;
	/*Required level to equip*/
	public final int attack_level;
	public final int firemaking_level;
	Pickaxe(String s, int m, int l, int a, int f)
	{
		ID = s;
		mining_level = m;
		smithing_level = l;
		attack_level = a;
		firemaking_level = f;
	}
}

