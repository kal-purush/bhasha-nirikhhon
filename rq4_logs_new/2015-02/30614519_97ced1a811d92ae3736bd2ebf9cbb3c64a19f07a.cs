using System;
using System.Collections.Generic;
using System.Text;
using Engine;
using Engine.FileSystem;
using Engine.MathEx;
using Engine.Utils;
using Engine.Renderer;
using Engine.PhysicsSystem;
using Engine.SoundSystem;
using Engine.UISystem;
using Engine.EntitySystem;
using Engine.MapSystem;
using ProjectCommon;
using ProjectEntities;

namespace Maps_RTSMap_LogicSystem_LogicSystemScripts
{
	public class Beginning : Engine.EntitySystem.LogicSystem.LogicEntityObject
	{
		Engine.EntitySystem.World __ownerEntity;
		
		public Beginning( Engine.EntitySystem.World ownerEntity )
			: base( ownerEntity )
		{
			this.__ownerEntity = ownerEntity;
			ownerEntity.Create += delegate( Engine.EntitySystem.Entity __entity ) { if( Engine.EntitySystem.LogicSystemManager.Instance != null )Create(  ); };
		}
		
		public Engine.EntitySystem.World Owner
		{
			get { return __ownerEntity; }
		}
		
		
		public void Create()
		{
			Engine.EntitySystem.LogicClass __class = Engine.EntitySystem.LogicSystemManager.Instance.MapClassManager.GetByName( "Beginning" );
			Engine.EntitySystem.LogicSystem.LogicDesignerMethod __method = (Engine.EntitySystem.LogicSystem.LogicDesignerMethod)__class.GetMethodByName( "Create" );
			__method.Execute( this, new object[ 0 ]{  } );
		}

	}
}