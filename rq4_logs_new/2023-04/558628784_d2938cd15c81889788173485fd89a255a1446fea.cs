using System.Threading.Tasks;
using UnityEngine;

public abstract class AssetLoaderBase : MonoBehaviour
{
	public static AssetLoaderBase Instance { get; private set; }

	public virtual GameObject LoadResource(string path)
	{
		return Resources.Load<GameObject>(path);
	}

	public virtual async Task<GameObject> LoadResourceAsync(string path)
	{
		//var req = Resources.LoadAsync<GameObject>(path);
		//await req;
		//return req.asset as GameObject;
		return LoadResource(path);
	}

	protected virtual void Awake()
	{
		Instance = this;
	}

	protected virtual void OnDestroy()
	{
		if (Instance == this)
			Instance = null;
	}

	public static GameObject Load(string path)
	{
		if (Instance != null)
			return Instance.LoadResource(path);

		return Resources.Load<GameObject>(path);
	}

	public static async Task<GameObject> LoadAsync(string path)
	{
		if (Instance != null)
			return await Instance.LoadResourceAsync(path);

		//var req = Resources.LoadAsync<GameObject>(path);
		//await req;
		//return req.asset as GameObject;
		return Load(path);
	}
}