package com.SoulSandHands;


import net.minecraft.client.particle.IAnimatedSprite;
import net.minecraft.client.particle.IParticleFactory;
import net.minecraft.client.particle.IParticleRenderType;
import net.minecraft.client.particle.Particle;
import net.minecraft.client.particle.SpriteTexturedParticle;
import net.minecraft.client.world.ClientWorld;
import net.minecraft.particles.BasicParticleType;
import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.api.distmarker.OnlyIn;

@OnlyIn(Dist.CLIENT)
public class SoulHandsParticle extends SpriteTexturedParticle{
	protected SoulHandsParticle(ClientWorld world, double x, double y, double z, double motionX, double motionY,double motionZ) {
		super(world, x, y, z, motionX, motionY, motionZ);
		this.setSize(width*2, height*2);
		// TODO Auto-generated constructor stub
	}

	//protected SoulHandsParticle(ClientWorld world, double x, double y, double z, double motionX, double motionY, double motionZ, float scale) {
	//	super(world, x, y, z, 0.1F, 0.1F, 0.1F, motionX, motionY, motionZ, scale, spriteWithAge, 1F, 8, -0.004D, true);
	//}
	
	@OnlyIn(Dist.CLIENT)
	public static class Factory implements IParticleFactory<BasicParticleType> {
		private final IAnimatedSprite spriteSet;

      public Factory(IAnimatedSprite spriteSet) {
         this.spriteSet = spriteSet;
      }

		

		@Override
		public Particle makeParticle(BasicParticleType typeIn, ClientWorld worldIn, double x, double y, double z,double xSpeed, double ySpeed, double zSpeed) {
			// TODO Auto-generated method stub
			SoulHandsParticle particle = new SoulHandsParticle(worldIn,  x,  y, z,0, 0, 0);
			particle.selectSpriteRandomly(spriteSet);
			particle.setColor(1.0F, 1.0F, 1.0F);
			return particle;
		}
	}
	
	
	@Override
	public void tick() {
	      this.prevPosX = this.posX;
	      this.prevPosY = this.posY;
	      this.prevPosZ = this.posZ;
	      if (this.age++ >= this.maxAge) {
	         this.setExpired();
	      } else {
	    	  this.motionY = -Math.pow(this.age,-1.1)/10;
	         this.move(0, this.motionY, 0);
	         if (this.posY == this.prevPosY) {
	        	 
	            this.motionX *= 1.1D;
	            this.motionZ *= 1.1D;
	         }

	         this.motionX *= (double)0.96F;
	         this.motionY *= (double)0.96F;
	         this.motionZ *= (double)0.96F;
	         if (this.onGround) {
	            this.motionX *= (double)0.7F;
	            this.motionZ *= (double)0.7F;
	         }

	      }
	   }

	
	@Override
	public IParticleRenderType getRenderType() {
		// TODO Auto-generated method stub
		return IParticleRenderType.PARTICLE_SHEET_TRANSLUCENT;
	}
}