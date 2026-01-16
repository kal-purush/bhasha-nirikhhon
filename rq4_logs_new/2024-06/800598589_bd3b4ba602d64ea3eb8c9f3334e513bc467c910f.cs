using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Godot;

public partial class Supervisor : Node
{
	[Export]
	public Node AgentsRootNode;
	
	[Export]
	public Environment Environment;

	[Export]
	public int InitialAgentCount = 10;

	[Export] 
	public bool UseLogicAgents = false;
	
	private PackedScene packedTrainAgent = ResourceLoader.Load<PackedScene>("res://src/scenes/trainAgent.tscn");
	private PackedScene packedLogicAgent = ResourceLoader.Load<PackedScene>("res://src/scenes/logicAgent.tscn");
	
	public override void _Ready()
	{
		for (int i = 0; i < this.InitialAgentCount; i++)
		{
			this.SpawnAgent();
		}
	}

	public override void _PhysicsProcess(double delta)
	{
		if (!this.UseLogicAgents)
		{
			this.SendData();
			this.ReceiveData();
		}
	}

	private void SpawnAgent()
	{
		Node2D agentInstance = (Node2D)(this.UseLogicAgents ? this.packedLogicAgent : this.packedTrainAgent).Instantiate();
		this.AgentsRootNode.AddChild(agentInstance);
		Vector2 spawnOffset = new Vector2(
			(float)(new Random().NextDouble() * this.Environment.Size.X),
			(float)(new Random().NextDouble() * this.Environment.Size.Y)
		);
		agentInstance.GlobalPosition = this.Environment.GlobalPosition + spawnOffset;
		Agent agent = (Agent)agentInstance;
		EntityManager.Get().RegisterAgent(agent);
	}

	private void SendData()
	{
		List<AgentData> data = new List<AgentData>();
		foreach (var (_, agent_) in EntityManager.Get().Agents)
		{
			TrainAgent agent = (TrainAgent)agent_;
			data.Add(agent.NormalizedData);
		}
		
		byte[] rawData = JsonConvert.SerializeObject(data).ToUtf8Buffer();
		PipeHandler.Get().Send(rawData);
	}

	private void ReceiveData()
	{
		byte[] data = PipeHandler.Get().Receive();
		string jsonString = Encoding.UTF8.GetString(data);
		List<AgentAction> agentActions = JsonConvert.DeserializeObject<List<AgentAction>>(jsonString);
		this.SaveActions(agentActions);
	}

	private void SaveActions(List<AgentAction> actions)
	{
		foreach (AgentAction action in actions)
		{
			TrainAgent agent = (TrainAgent)EntityManager.Get().Agent(action.Id);
			agent.Action = action;
		}
	}

	public void Reset()
	{
		
	}
}