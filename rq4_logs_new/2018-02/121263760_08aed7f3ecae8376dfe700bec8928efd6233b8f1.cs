using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace OneRoomRPGJam
{
	public class StateMachine
	{
		List<State> stateList;
		int currentState; 

		public StateMachine()
		{
			stateList = new List<State>(); 
		}
		public void Init()
		{
			if (stateList.Count > 0)
			{
				stateList[currentState].Init(); 
			}
		}
		public void Update(GameTime gameTime)
		{
			stateList[currentState].Update(gameTime); 
		}
		public void Render(SpriteBatch spriteBatch)
		{
			stateList[currentState].Render(spriteBatch); 
		}
		public void ChangeState(int index)
		{
			stateList[currentState].OnExit();
			currentState = index;
			stateList[currentState].OnEnter(); 
		}
		public void AddState(State state)
		{
			stateList.Add(state);
		}
	}
}