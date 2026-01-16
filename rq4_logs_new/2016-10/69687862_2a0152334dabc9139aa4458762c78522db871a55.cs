using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using System;

public class PanelTimer : MonoBehaviour
{

	public Text timerTxt;
	public Text clockTxt;
	public Text stateTxt;

	public Button startTime;
	public Button pauseTime;
	public Button resumeTime;

	public float totalSeconds = 5f;

	private Timer _timer;
	private TimeSpan _timeSpan;


	void Start ()
	{

		this._timer = this.gameObject.AddComponent<Timer> ();
		this._timer.OnTimerComplete += OnTimerCompleteHandler;
		this._timer.OnTimerUpdate += OnTimerUpdateHandler;
		this._timer.OnTimerPause += OnTimerPauseHandler;
		this._timer.SetUpTimer (this.totalSeconds);

		//t.StartTimer();

		//Listeners Botones Varias Maneras..
		//this.startTime.onClick.AddListener(this._timer.StartTimer);
		//this.startTime.onClick.AddListener(delegate(){this._timer.StartTimer(); this.stateTxt.text = "Update";});

		this.startTime.onClick.AddListener (this.onClickStartHandler);
		this.pauseTime.onClick.AddListener (this.onClickPauseHandler);
		this.resumeTime.onClick.AddListener (this.onClickResumeHandler);
	}

	//Callback botones
	private void onClickStartHandler ()
	{
		this._timer.StartTimer ();
		this.stateTxt.text = "Update";
	}

	private void onClickPauseHandler ()
	{
		this._timer.PauseTimer ();
		this.stateTxt.text = "Pause";
	}

	private void onClickResumeHandler ()
	{
		this._timer.ResumeTimer ();
		this.stateTxt.text = "Update";
	}

	//Handlers Timer
	private void OnTimerUpdateHandler (float time)
	{
		this.timerTxt.text = time.ToString ();
		this.clockTxt.text = ClockText (time);
	}

	private void OnTimerPauseHandler (float time)
	{
		this.stateTxt.text = "Pause";
	}

	private void OnTimerCompleteHandler (float time)
	{
		//this.timerTxt.text = "Ya terminé... Yahoo!";
		this.stateTxt.text = "Complete";
		//this._timer.OnTimerComplete -= OnTimerCompleteHandler;
		//this._timer.OnTimerUpdate -= OnTimerUpdateHandler;
		//this._timer.OnTimerPause -= OnTimerPauseHandler;	
	}

	//Formato Reloj apartir de milisegundos
	private string ClockText (float time)
	{	
		
		float seconds = this.totalSeconds - Mathf.FloorToInt(time);
		return  TimeSpan.FromSeconds (seconds).ToString ();

	}

}