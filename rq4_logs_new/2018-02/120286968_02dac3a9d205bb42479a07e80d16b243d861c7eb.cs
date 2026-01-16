using UnityEngine.EventSystems;
using System.Collections;
using System;
using UnityEngine;
using UnityEngine.SceneManagement;
public class GameManager : MonoBehaviour, IPointerDownHandler, IPointerClickHandler,
    IPointerUpHandler, IPointerExitHandler, IPointerEnterHandler,
    IBeginDragHandler, IDragHandler, IEndDragHandler{
    
    
    // Use this for initialization

    public SManager sceneManager;                       // Reference to SManager for each scene
    public static bool mousepressed = false;
    [HideInInspector]
    public enum Scenes                                      // Place all scene names here in order
    {
        Init,
        Intro,
        Scene01,
        Scene02,
        Scene03
    }
    public Scenes currentScene;
    public static GameManager Instance
    {
        get { return GameManager.instance; }
    }
    // access to the singleton
    private static GameManager instance;
    // this is called after Awake() OR after the script is recompiled (Recompile > Disable > Enable)
    private void Init()
    {
        // Assign our current scene on one-time init so we can support starting game from any scene during testing
        currentScene = (Scenes)Enum.Parse(typeof(Scenes), SceneManager.GetActiveScene().name);
    }

    protected virtual void OnEnable()
    {
        SceneManager.sceneLoaded += OnSceneLoaded;
        if (instance == null)
        {
            instance = this;

            Init();
        }
        else if (instance != this)
        {
            Debug.LogWarning("GAME MANAGER: WARNING - THERE IS ALREADY AN INSTANCE OF GAME MANAGER RUNNING - DESTROYING THIS ONE.");
            Destroy(this.gameObject);
            return;
        }
    }
    void OnDisable()
    {
        SceneManager.sceneLoaded -= OnSceneLoaded;
    }
    // Called each time a new scene is loaded
    void OnSceneLoaded(Scene scene, LoadSceneMode mode){
        Debug.Log("LEVEL WAS LOADED: " + SceneManager.GetActiveScene().name);
        //AndroidBroadcastIntentHandler.BroadcastJSONData("scene", SceneManager.GetActiveScene().name);

        //LoadUI();
        LoadSceneManager();
    }
    private void LoadSceneManager()
    {
        // Grab the current SManager GameObject (if it exists)
        GameObject sceneManagerGO = GameObject.Find("SManager");

        if (sceneManagerGO != null)
        {
            sceneManager = sceneManagerGO.GetComponent<SManager>();

            if (sceneManager != null)
            {
                //sceneManager.Init(this);
            }
        }
    }

    void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}
    public void OnBeginDrag(PointerEventData eventData)
    {
        Debug.Log("Drag Begin");
    }

    public void OnDrag(PointerEventData eventData)
    {
        Debug.Log("Dragging");
    }

    public void OnEndDrag(PointerEventData eventData)
    {
        Debug.Log("Drag Ended");
    }

    public void OnPointerClick(PointerEventData eventData)
    {
        Debug.Log("Clicked: " + eventData.pointerCurrentRaycast.gameObject.name);
        
    }

    public void OnPointerDown(PointerEventData eventData)
    {
        // gm.deep();
        mousepressed = true;
        Debug.Log("Mouse Down: " + eventData.pointerCurrentRaycast.gameObject.name);
        //if (sceneManager != null)
        //{
            Debug.Log("hello"+  eventData.pointerCurrentRaycast.gameObject.name);
        sceneManager.OnMouseDown(eventData.pointerCurrentRaycast.gameObject);
        

        //}

    }

    public void OnPointerEnter(PointerEventData eventData)
    {
        Debug.Log("Mouse Enter");
    }

    public void OnPointerExit(PointerEventData eventData)
    {
        Debug.Log("Mouse Exit");
    }

    public void OnPointerUp(PointerEventData eventData)
    {
        mousepressed = false;
       // Time.timeScale = 1;

        sceneManager.OnMouseUp(eventData.pointerCurrentRaycast.gameObject);

        //gm.disableanim();
    }

}