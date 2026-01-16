using System;
using System.Collections.Generic;
using Mononoke.Core;

namespace Mononoke.Layers
{
	public class Layer
	{
		public int Depth { get; set; }


		private List<Entity> _entities;

		public IEnumerable<Entity> Entities => _entities;


		private HashSet<Scene> _scenes;
		
		
		public Layer(int depth = 0)
		{
			Depth = depth;
			_entities = new List<Entity>();
		}


		public void Attach(Scene scene)
		{
			if (_scenes.Contains(scene))
			{
				Console.WriteLine("The layer is already attached to this scene");
				return;
			}

			_scenes.Add(scene);
			// TODO binding and depth stuff
		}
		
		public void Detach(Scene scene)
		{
			
		}
		
		
		
		internal void Add(Entity entity)
		{
			_entities.Add(entity);
		}
		
		
		internal bool Remove(Entity entity)
		{
			return _entities.Remove(entity);
		}


		public bool Contains(Entity entity)
		{
			return _entities.Contains(entity);
		}
	}
}