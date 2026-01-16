package de.tr7zw.tas;

import java.awt.Robot;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import net.minecraft.client.Minecraft;
import net.minecraft.client.settings.GameSettings;
import net.minecraft.client.settings.KeyBinding;
import net.minecraft.util.MovementInput;
import net.minecraft.util.MovementInputFromOptions;
import net.minecraft.util.text.TextComponentString;
import net.minecraft.util.text.TextFormatting;
/**
 * Newer version of TASInput.java<br>
 * Executes the input directly after reading the line in the file
 * @author ScribbleLP
 *
 */
public class Playback extends MovementInputFromOptions{

	private Minecraft mc = Minecraft.getMinecraft();
	private boolean forward;
	private boolean backward;
	private boolean left;
	private boolean right;
	private boolean Jump;
	private boolean Sneak;
	private boolean sprint;
	private boolean leftclick;
	private boolean rightclick;
	private float pitch;
	private float yaw;
	private int hotbarslot;
	private int calcstate=0;
	private String[] args;
	public static int frame=0;

	
	
	public Playback(String[] Helloargs) {
		super(Minecraft.getMinecraft().gameSettings);
		args=Helloargs;
	}
	
	public void sendMessage(String msg){
		try{
			Minecraft.getMinecraft().ingameGUI.getChatGUI().printChatMessage(new TextComponentString(msg));
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public Float recalcYaw(float Yaw){
		while(Yaw>=180)Yaw-=360;
		while(Yaw<-180)Yaw+=360;
		return Yaw;
		
	}
	private float uncalc(float yaw){
		if(recalcYaw(mc.player.rotationYaw)>=0&&(recalcYaw(mc.player.rotationYaw)-yaw)>180){
			calcstate++;
		}
		if(recalcYaw(mc.player.rotationYaw)<0&&(recalcYaw(mc.player.rotationYaw)-yaw)<-180){
			calcstate--;
		}
		return yaw+(360*calcstate);
	}
	

	public void readingFile(String[] args, int stopAt){
		File file = new File(Minecraft.getMinecraft().mcDataDir, "saves" + File.separator + 
				"tasfiles"+ File.separator + args[0] + ".tas");
		try{
			BufferedReader Buff = new BufferedReader(new FileReader(file));
			String s;
			int line=0;
			while (true){
				if((s=Buff.readLine()).equalsIgnoreCase("END")){
					break;
				}
				if(s.startsWith("#")||s.startsWith("/")){
					continue;
				}
				else if(line==stopAt){
					handleData(s.split(";"));
					Buff.close();
					return;
				}
				line++;
			}
			sendMessage("Finished Playback");
			TASInput.donePlaying=true;
			Buff.close();
			return;
		} catch (NullPointerException ex){
				sendMessage(TextFormatting.RED+"Error while reading the file. Couldn't find an END");
				TASInput.donePlaying=true;
				ex.printStackTrace();
				
			
		} catch (IOException e) {
			TASInput.donePlaying=true;
			e.printStackTrace();
		}
	}

	private void handleData(String[] field) {
		forward=field[1].equalsIgnoreCase("W");
		backward=field[2].equalsIgnoreCase("S");
		left=field[3].equalsIgnoreCase("A");
		right=field[4].equalsIgnoreCase("D");
		Jump=field[5].equalsIgnoreCase("Space");
		Sneak=field[6].equalsIgnoreCase("Shift");
		sprint=field[7].equalsIgnoreCase("Ctrl");
		pitch=Float.parseFloat(field[8]);
		yaw=Float.parseFloat(field[9]);
		leftclick=field[10].equalsIgnoreCase("LK");
		rightclick=field[11].equalsIgnoreCase("RK");
		hotbarslot=Integer.parseInt(field[12]);
	}
	

	@Override
	public void updatePlayerMoveState() {
		if(!TASInput.donePlaying){
			readingFile(args, frame++);
		}
		if (TASInput.donePlaying){
			super.updatePlayerMoveState();
			return;
		}
		
		KeyBinding.setKeyBindState(-100, leftclick);			//Read Leftclick from File
		KeyBinding.setKeyBindState(-99, rightclick);			//Read RightClick from File
		KeyBinding.setKeyBindState(29, sprint);					//Read Sprint Key from File
		mc.player.inventory.currentItem=hotbarslot;				//Read Inventory Slot from File etc...
		
		this.moveStrafe = 0.0F;
		this.moveForward = 0.0F;

		if (forward)
		{
		++this.moveForward;
		this.forwardKeyDown = true;
		}
		else
		{
			this.forwardKeyDown = false;
		}

		if (backward)
		{
			--this.moveForward;
			this.backKeyDown = true;
		}
		else
		{
			this.backKeyDown = false;
		}

		if (left)
		{
			++this.moveStrafe;
			this.leftKeyDown = true;
		}
		else
		{
			this.leftKeyDown = false;
		}
	
		if (right)
		{
		--this.moveStrafe;
		this.rightKeyDown = true;
		}
		else
		{
			this.rightKeyDown = false;
		}
		this.jump=Jump;
		this.sneak=Sneak;
	
		if (this.sneak)
		{
			this.moveStrafe = (float)((double)this.moveStrafe * 0.3D);
			this.moveForward = (float)((double)this.moveForward * 0.3D);
		}
		mc.player.rotationPitch = pitch;
		mc.player.rotationYaw = uncalc(yaw);
	}
}

