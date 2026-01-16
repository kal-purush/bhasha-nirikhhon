using System.Collections.Generic;
using UnityEngine;

public class Command {
	public string Method { get; set; }
	public Dictionary<string, string> Arguments { get; set; }

	public Command(string method) {
		this.Method = method;
		this.Arguments = new Dictionary<string, string>();
	}

	public void AddArgument(string name, string value) {
		this.Arguments.Add(name, value);
	}

	public string GetJSON() {
		return JsonUtility.ToJson(this);
	}

	public void FromResponse(string json) {
		JsonUtility.FromJson<Command>(json);
	}
}