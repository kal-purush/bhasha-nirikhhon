package forestMoon.client.guicontainer;

import org.lwjgl.opengl.GL11;

import forestMoon.ExtendedPlayerProperties;
import forestMoon.client.entity.TileEntityChest;
import forestMoon.client.gui.MyGuiButton;
import forestMoon.container.PlayerShopingContainer;
import forestMoon.packet.PacketHandler;
import forestMoon.packet.player.MessagePlayerPropertieToServer;
import forestMoon.packet.shoping.MessageShopingSyncToServer;
import forestMoon.packet.shoping.MessageSpawnItemstack;
import net.minecraft.client.gui.GuiButton;
import net.minecraft.client.gui.inventory.GuiContainer;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.item.ItemStack;
import net.minecraft.util.ResourceLocation;
import net.minecraft.util.StatCollector;

public class PlayerShopingGuiContainer extends GuiContainer{

	private EntityPlayer player;
	private TileEntityChest tileEntity;
	private String shopName;
	private String itemName ="";
	private ExtendedPlayerProperties properties;
	private int moneyX,moneyY;
	private static PlayerShopingContainer container;
	private static final ResourceLocation GUITEXTURE = new ResourceLocation("forestmoon:textures/gui/container/shopGui_2.png");



	public PlayerShopingGuiContainer(EntityPlayer player, TileEntityChest tileEntity) {
		super(container = new PlayerShopingContainer(tileEntity, player));
		this.player = player;
		this.tileEntity = tileEntity;
		ySize = 235;
		shopName = StatCollector.translateToLocalFormatted(StatCollector.translateToLocal("shop_name"), tileEntity.getAdminName());

		PacketHandler.INSTANCE.sendToServer(new MessageShopingSyncToServer(tileEntity.xCoord, tileEntity.yCoord, tileEntity.zCoord));


		new ExtendedPlayerProperties();
		properties = ExtendedPlayerProperties.get(player);
	}

	public void initGui() {
		super.initGui();

		System.out.println("\nguileft :" + guiLeft + " width:"+width +" xSize:"+xSize + "\n guitop"+guiTop + " height" + height+" ySize:" + ySize);
		moneyX = xSize - 62;
		moneyY = this.ySize - 148;

		this.buttonList.add(new MyGuiButton(0, guiLeft + xSize - 50, guiTop+ySize- 165, 40,15,StatCollector.translateToLocal("buy_button")));
	}

	@Override
	protected void drawGuiContainerForegroundLayer(int p_146979_1_, int p_146979_2_) {

		fontRendererObj.drawString(shopName, 8, 4, 4210752);
		fontRendererObj.drawString(itemName, 8, 74, 4210752);
		fontRendererObj.drawString(StatCollector.translateToLocal(StatCollector.translateToLocal("money") + properties.getMoney()), moneyX, moneyY, 4210752);
		itemName = container.slotItemToString(container.getLastSlotNumber());

		super.drawGuiContainerForegroundLayer(p_146979_1_, p_146979_2_);

		if(tileEntity.isAdminFlag()) {
			player.closeScreen();
		}
	}

	@Override
	protected void drawGuiContainerBackgroundLayer(float p_146976_1_, int p_146976_2_, int p_146976_3_) {
		GL11.glColor4f(1.0F, 1.0F, 1.0F, 1.0F);
		mc.getTextureManager().bindTexture(GUITEXTURE);
		int k = (width - xSize) / 2;
		int l = (height - ySize) / 2;
		this.drawTexturedModalRect(k, l, 0, 0, xSize, ySize);
	}
	//ボタン押下時
	@Override
	protected void actionPerformed(GuiButton button) {
		ItemStack itemStack;
		int price;

		if(button.id == 0) {
			if(!itemName.equals("") && tileEntity.getStackInSlot(container.getLastSlotNumber()).stackSize > 0 && !tileEntity.isAdminFlag()) {
				price = tileEntity.getSellPrice(container.getLastSlotNumber());
				itemStack = tileEntity.getStackInSlot(container.getLastSlotNumber());
				buy(itemStack, price);
			}
		}
	}
	//販売処理
	private void buy(ItemStack itemStack,int price){
		long money = properties.getMoney();

		if(price <= money ){
			double x = player.lastTickPosX;
			double y = player.lastTickPosY;
			double z = player.lastTickPosZ;

			PacketHandler.INSTANCE.sendToServer(new MessageSpawnItemstack(itemStack, x, y, z,1));

			properties.changeMoney(price  * -1);
			tileEntity.buy(container.getLastSlotNumber(), 1);
			PacketHandler.INSTANCE.sendToServer(new MessagePlayerPropertieToServer(player));
		}

	}

	@Override
	protected void mouseClicked(int p_73864_1_, int p_73864_2_, int p_73864_3_) {
		super.mouseClicked(p_73864_1_, p_73864_2_, p_73864_3_);
		System.out.println("x:" + p_73864_1_+" y:" + p_73864_2_ + " ?:"+p_73864_3_);
	}
}