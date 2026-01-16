using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Assets.scripts.Actor;

namespace Assets.scripts.Upgrade
{
	public class Stash
	{
		public Character Owner { get; private set; }
		public int Capacity { get; set; }

		private List<InventoryItem> items;
		public List<InventoryItem> Items
		{
			get { return items; }
		}

		public Stash(Character owner, int capacity)
		{
			this.Owner = owner;
			this.Capacity = capacity;
			items = new List<InventoryItem>();
		}

		public void LoadUpgrades()
		{
		}

		public bool HasInInventory(InventoryItem u)
		{
			return items.Contains(u);
		}

		public bool CanAdd(InventoryItem u)
		{
			if (items.Count >= Capacity)
				return false;

			return true;
		}

		public void AddItem(InventoryItem u)
		{
			if (!CanAdd(u))
				return;

			items.Add(u);
		}

		public void RemoveItem(InventoryItem u)
		{
			items.Remove(u);
		}

		public InventoryItem GetItem(int order)
		{
			try
			{
				InventoryItem u = items[order];
				return u;
			}
			catch (Exception)
			{
			}

			return null;
		}

		public InventoryItem GetUpgrade(Type type)
		{
			foreach (InventoryItem u in items)
			{
				if (u.GetType().Equals(type))
					return u;
			}
			return null;
		}
	}
}