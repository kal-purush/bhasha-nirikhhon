package forestMoon.client.guicontainer;

import org.lwjgl.opengl.GL11;

import cpw.mods.fml.relauncher.Side;
import cpw.mods.fml.relauncher.SideOnly;
import forestMoon.ExtendedPlayerProperties;
import forestMoon.container.PlayerShopAdminContainer;
import forestMoon.packet.PacketHandler;
import forestMoon.packet.player.MessagePlayerPropertieToServer;
import forestMoon.packet.shoping.MessagePlayerShopSyncToServer;
import forestMoon.packet.shoping.MessageShopSettingFlagToServer;
import forestMoon.tileEntity.TileEntityShop;
import net.minecraft.client.gui.GuiButton;
import net.minecraft.client.gui.GuiTextField;
import net.minecraft.client.gui.inventory.GuiContainer;
import net.minecraft.entity.player.EntityPlayer;
import net.minecraft.util.ResourceLocation;
import net.minecraft.util.StatCollector;

@SideOnly(Side.CLIENT)
public class PlayerShopAdminGuiContainer extends GuiContainer {
	private GuiTextField priceTextField;
	private int x, y, z, moneyX, moneyY;
	private GuiButton doneBtn, changeBtn, salengGetBtn;
	private TileEntityShop tileEntity;
	private boolean shopSettingMode = false;//false:itemMode true:priceMode
	private EntityPlayer player;
	private boolean shopSettingFlag = false;//ショップを設定中かのフラグ
	private ExtendedPlayerProperties properties;
	private static PlayerShopAdminContainer container;
	private String itemName = "";
	private String mode = "";

	public enum ButtonId {
		doneBtn(0), changeBtn(1), salengGetBtn(2);

		private final int id;

		private ButtonId(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}
	}

	private static final ResourceLocation GUITEXTURE = new ResourceLocation(
			"forestmoon:textures/gui/container/shopGui_2.png");

	public PlayerShopAdminGuiContainer(EntityPlayer player, TileEntityShop tileEntitiy) {
		super(container = new PlayerShopAdminContainer(player, tileEntitiy));
		this.tileEntity = tileEntitiy;
		this.player = player;
		ySize = 235;

		x = tileEntity.xCoord;
		y = tileEntity.yCoord;
		z = tileEntity.zCoord;

		new ExtendedPlayerProperties();
		properties = ExtendedPlayerProperties.get(player);
		PacketHandler.INSTANCE.sendToServer(new MessagePlayerShopSyncToServer(x, y, z));
//		PacketHandler.INSTANCE.sendToServer(new MessageShopChangeFlag(true, x, y, z));
	}

	public void initGui() {
		super.initGui();
		tileEntity.setSlotClickFlag(true);
		if (tileEntity.getAdminName().equals("NONE")) {
			tileEntity.setAdminName(player.getCommandSenderName());
			PacketHandler.INSTANCE.sendToServer(new MessagePlayerShopSyncToServer(x, y, z, player.getCommandSenderName(),
					tileEntity.getSellPrices(), tileEntity.getEarnings(), tileEntity.getItemStacks()));
		} else if (tileEntity.getAdminName().equals(player.getCommandSenderName())) {
			shopSettingFlag = true;
		}
		if (shopSettingFlag) {
			this.buttonList.add(this.doneBtn = new GuiButton(ButtonId.doneBtn.id, this.width / 2 + 100,
					this.height / 2 - 60, 105, 20, StatCollector.translateToLocal("doneButton")));
			this.buttonList.add(this.changeBtn = new GuiButton(ButtonId.changeBtn.id, this.width / 2 + 100,
					this.height / 2 - 30, 105, 20, StatCollector.translateToLocal("changeButton")));
			this.buttonList.add(this.salengGetBtn = new GuiButton(ButtonId.salengGetBtn.id, this.width / 2 + 100,
					this.height / 2, 105, 20, StatCollector.translateToLocal("salengGetButton")));

			this.doneBtn.enabled = false;

			priceTextField = new GuiTextField(this.fontRendererObj, 190, 20, 100, 20);
			priceTextField.setFocused(true);
			priceTextField.setText("");
			priceTextField.setMaxStringLength(8);
		}

		moneyX = xSize - 62;
		moneyY = this.ySize - 148;

		PacketHandler.INSTANCE.sendToServer(new MessageShopSettingFlagToServer(x, y, z, true));

	}

	@Override
	protected void drawGuiContainerForegroundLayer(int p_146979_1_, int p_146979_2_) {
		fontRendererObj.drawString(mode, xSize + 18, 2, 16777215);
		fontRendererObj.drawString(StatCollector.translateToLocalFormatted(StatCollector.translateToLocal("sales"),
				tileEntity.getEarnings()), 8, 84, 4210752);
		fontRendererObj.drawString(StatCollector.translateToLocalFormatted("shop_name", tileEntity.getAdminName()), 8,
				6, 4210752);
		fontRendererObj.drawString(
				StatCollector.translateToLocal(StatCollector.translateToLocal("money") + properties.getMoney()), moneyX,
				moneyY, 4210752);
		fontRendererObj.drawString(itemName, 8, 72, 4210752);
		if (shopSettingFlag) {
			priceTextField.setEnabled(shopSettingMode);
			priceTextField.drawTextBox();

			// テキストフィールドに数値が入ってる場合にボタンを有効化
			this.doneBtn.enabled = this.priceTextField.getText().trim().length() > 0;
		}

		if (shopSettingMode) {
			mode = StatCollector.translateToLocal("priceChange");
		} else {
			mode = StatCollector.translateToLocal("itemChange");
		}
		itemName = container.slotItemToString(container.getLastSlotNumber());

		super.drawGuiContainerForegroundLayer(p_146979_1_, p_146979_2_);
	}

	@Override
	protected void drawGuiContainerBackgroundLayer(float tick, int mouseX, int mouseY) {
		GL11.glColor4f(1.0F, 1.0F, 1.0F, 1.0F);
		mc.getTextureManager().bindTexture(GUITEXTURE);
		int k = (width - xSize) / 2;
		int l = (height - ySize) / 2;
		this.drawTexturedModalRect(k, l, 0, 0, xSize, ySize);
	}

	// マウスでクリックした時のアクション
	public void mouseClickd(int i, int j, int k) {
		System.out.println("i:" + i + "/j:" + j);
		if (shopSettingMode) {
			if (0 <= i && i <= xSize && 0 <= j && j <= 50) {
			} else {
				super.mouseClicked(i, i, k);
			}
			if (shopSettingFlag) {
				priceTextField.mouseClicked(i, j, k);
			}
		} else {
			super.mouseClicked(i, j, k);
		}

	}

	// キーをタイピングした時のアクション
	protected void keyTyped(char p_73869_1_, int p_73869_2_) {
		super.keyTyped(p_73869_1_, p_73869_2_);
		if (p_73869_2_ == 1) {
			mc.displayGuiScreen(null);
		}
		if (shopSettingFlag) {
			priceTextField.textboxKeyTyped(p_73869_1_, p_73869_2_);
		}
	}

	// ボタンを押した時のアクション
	public void actionPerformed(GuiButton guibutton) {
		if (shopSettingFlag) {
			if (guibutton.id == ButtonId.doneBtn.id) {
				if (shopSettingMode == true) {
					// 価格設定の処理
					try {
						int price = Integer.parseInt(priceTextField.getText());
						tileEntity.setSellPrice(container.getLastSlotNumber(), price);
						tileEntity.sendServer();
					} catch (Exception e) {
					}

				}
			}
			if (guibutton.id == ButtonId.changeBtn.id) {
				// 商品管理と在庫処理のbool切り替え
				shopSettingMode = !shopSettingMode;
				container.setClickFlag(!container.getClickFlag());

			}
			if (guibutton.id == ButtonId.salengGetBtn.id) {
				properties.changeMoney(tileEntity.getEarnings());
				tileEntity.setEarnings(0);
				PacketHandler.INSTANCE.sendToServer(new MessagePlayerPropertieToServer(player));
				PacketHandler.INSTANCE.sendToServer(
						new MessagePlayerShopSyncToServer(tileEntity.xCoord, tileEntity.yCoord, tileEntity.zCoord,
								tileEntity.getAdminName(), tileEntity.getSellPrices(), 0, tileEntity.getItemStacks()));
				// PacketHandler.INSTANCE.sendToServer(new
				// MessageEarningsSyncToServer(tileEntity.xCoord,
				// tileEntity.yCoord, tileEntity.zCoord,
				// tileEntity.getEarnings()));
			}
		}
	}

	@Override
	public void onGuiClosed() {
		super.onGuiClosed();
//		PacketHandler.INSTANCE.sendToServer(
//				new MessageShopChangeFlag(false, tileEntity.xCoord, tileEntity.yCoord, tileEntity.zCoord));
		PacketHandler.INSTANCE.sendToServer(
				new MessageShopSettingFlagToServer(tileEntity.xCoord, tileEntity.yCoord, tileEntity.zCoord, false));
	}
}