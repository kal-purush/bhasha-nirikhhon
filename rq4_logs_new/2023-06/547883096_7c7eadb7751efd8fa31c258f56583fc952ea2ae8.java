package com.ofek2608.deep_pocket.api.implementable;

import com.mojang.blaze3d.platform.Window;
import com.mojang.blaze3d.vertex.PoseStack;
import com.ofek2608.deep_pocket.api.utils.GuiUtils;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.Font;

import java.util.List;

public interface HoverBox {
	void render(PoseStack stack, int color);
	int getWidth();
	int getHeight();
	
	static void render(HoverBox box, PoseStack stack, int color, int mx, int my) {
		Minecraft minecraft = Minecraft.getInstance();
		Window window = minecraft.getWindow();
		int boxWidth = box.getWidth();
		int boxHeight = box.getHeight();
		int windowWidth = window.getGuiScaledWidth();
//		int windowHeight = window.getGuiScaledHeight();
		
		int boxX = mx + boxWidth + 8 < windowWidth - 8 ? mx + 8 : mx - boxWidth - 8;
		int boxY = my - boxHeight - 8 > 8 ? my - boxHeight - 8 : my + 8;
		
		GuiUtils.setShaderColor(color);
		GuiUtils.renderOutline(boxX, boxX + boxWidth, boxY, boxY + boxHeight);
		stack.pushPose();
		stack.translate(boxX, boxY, 1000);
		box.render(stack, color);
		stack.popPose();
	}
	
	class Text implements HoverBox {
		protected String value;
		protected Font font;
		
		public Text(String value, Font font) {
      this.value = value;
			this.font = font;
    }
		
		public Text(String value) {
			this(value, Minecraft.getInstance().font);
		}
		
		@Override
		public void render(PoseStack stack, int color) {
			font.draw(stack, value, 0, 0, 0xFFFFFF);
		}
		
		@Override
		public int getWidth() {
			return font.width(value);
		}
		
		@Override
		public int getHeight() {
			return font.lineHeight;
		}
	}
	
	class Row implements HoverBox {
		protected Align align;
		protected int spacing;
		protected List<HoverBox> elements;
		
		public Row(Align align, int spacing, List<HoverBox> elements) {
			this.align = align;
			this.spacing = spacing;
      this.elements = elements;
		}
		
		@Override
		public void render(PoseStack stack, int color) {
			int width = getWidth();
			int height = getHeight();
			stack.pushPose();
			for (HoverBox element : elements) {
				switch (align) {
					case TOP, LEFT -> element.render(stack, color);
					case MIDDLE -> {
						stack.pushPose();
						stack.translate(0, (height - element.getHeight()) * 0.5f, 0);
						element.render(stack, color);
						stack.popPose();
					}
					case BOTTOM -> {
						stack.pushPose();
						stack.translate(0, height - element.getHeight(), 0);
						element.render(stack, color);
						stack.popPose();
					}
					case CENTER -> {
						stack.pushPose();
						stack.translate((width - element.getWidth()) * 0.5f, 0, 0);
						element.render(stack, color);
						stack.popPose();
					}
					case RIGHT -> {
						stack.pushPose();
						stack.translate(width - element.getWidth(), 0, 0);
						element.render(stack, color);
						stack.popPose();
					}
				}
				switch (align) {
					case TOP, MIDDLE, BOTTOM -> stack.translate(element.getWidth() + spacing, 0, 0);
					case LEFT, CENTER, RIGHT -> stack.translate(0, element.getHeight() + spacing, 0);
				}
			}
			stack.popPose();
		}
		
		@Override
		public int getWidth() {
			int result = 0;
			if (align.ordinal() < 3) {
				for (HoverBox element : elements) {
					result += element.getWidth();
				}
				result += spacing * (elements.size() - 1);
			} else {
				for (HoverBox element : elements) {
					result = Math.max(element.getWidth(), result);
				}
			}
			return result;
		}
		
		@Override
		public int getHeight() {
			int result = 0;
			if (align.ordinal() >= 3) {
				for (HoverBox element : elements) {
					result += element.getHeight();
				}
				result += spacing * (elements.size() - 1);
			} else {
				for (HoverBox element : elements) {
					result = Math.max(element.getHeight(), result);
				}
			}
			return result;
		}
		
		
		public enum Align {
			//Vertical align
			TOP,
			MIDDLE,
			BOTTOM,
			//Horizontal align
			LEFT,
      CENTER,
      RIGHT,
		}
	}
}