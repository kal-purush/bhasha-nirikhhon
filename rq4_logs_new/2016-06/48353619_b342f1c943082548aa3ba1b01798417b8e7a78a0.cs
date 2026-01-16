using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class LevelSelectManager : MonoBehaviour {

	public Text currentLevelTitle;
	public Transform container; //Holds buttons

	private Vector2 touchStart;
	private Vector2 touchEnd;
	private float touchStartTime;
	private float touchEndTime;

	private float buttonSpacing = 250.0f;
	private float clampSpeed = 0.3f;
	private float minSwipeDistance = 10;
	private float maxSwipeTime = 1.0f;

	private float containerWidth;
	private float slideAmount;
	private int selectedLevelIndex;
	private int prevSelectedLevelIndex = -1;
	private bool finishedClamp = true;

	//Fade in/out button variables
	public Color darkColor;
	public Color lightColor;
	private Vector3 largeScale = new Vector3 (1.0f, 1.0f, 1);
	private Vector3 smallScale = new Vector3 (0.7f, 0.7f, 1);

	void Start() {
		selectedLevelIndex = 0;
		currentLevelTitle.text = container.transform.GetChild (selectedLevelIndex).GetComponent<LevelButton> ().levelName;
		int numLevels = container.transform.childCount;

		//Space level buttons equidistant from each other
		for (int i = 1; i < numLevels; i++) {
			Transform prevButton = container.GetChild (i - 1);
			container.GetChild(i).transform.localPosition = new Vector3 (prevButton.transform.localPosition.x + buttonSpacing, 0, 0);
		}
		containerWidth = container.GetChild(numLevels - 1).transform.localPosition.x - container.GetChild(0).transform.localPosition.x;
	}

	void Update() {
		//****ON CLICK****
		if (Input.GetMouseButtonDown(0)) {
			touchStart = Input.mousePosition;
			touchStartTime = Time.time;
		}
		if (Input.GetMouseButton(0)) {
			touchEnd = Input.mousePosition;
			slideAmount = touchEnd.x - touchStart.x;
			//if not beyond container bounds, slide
			if ((slideAmount < 0 && container.localPosition.x > -containerWidth) || (slideAmount > 0 && container.localPosition.x < 0)) {
				SlideContainer();
			}
		}

		if (Input.GetMouseButtonUp(0)) {
			touchEndTime = Time.time;
			touchEnd = Input.mousePosition;
			float clampPosition = 100;

			//Registered as tap
			if (finishedClamp && touchStart == touchEnd) {
				if (touchStart.x >= (Screen.width / 2 + container.transform.GetChild(0).GetComponent<RectTransform>().sizeDelta.x / 2)) {
					clampPosition = Mathf.Ceil ((container.transform.localPosition.x - buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe next
				} else if (touchStart.x <= (Screen.width / 2 - container.transform.GetChild(0).GetComponent<RectTransform>().sizeDelta.x / 2)) {
					clampPosition = Mathf.Floor ((container.transform.localPosition.x + buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe prev
				}
				if (clampPosition != 100) {
					clampPosition = Mathf.Clamp (clampPosition, -containerWidth, 0);
					if (finishedClamp) {
						StartCoroutine (ClampContainer (clampPosition, nameValue => currentLevelTitle.text = nameValue, finishedValue => finishedClamp = finishedValue));
					}
				}
				//Registered as a swipe
			} else {
				if (Mathf.Abs (touchEnd.x - touchStart.x) > minSwipeDistance && touchEndTime - touchStartTime < maxSwipeTime) {
					if (touchEnd.x - touchStart.x > 0) {
						clampPosition = Mathf.Floor ((container.transform.localPosition.x + buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe prev
					} else {
						clampPosition = Mathf.Ceil ((container.transform.localPosition.x - buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe next
					}
				} else {
					clampPosition = Mathf.Round (container.transform.localPosition.x / buttonSpacing) * buttonSpacing;
				}
				clampPosition = Mathf.Clamp (clampPosition, -containerWidth, 0);
				if (finishedClamp) {
					StartCoroutine (ClampContainer (clampPosition, nameValue => currentLevelTitle.text = nameValue, finishedValue => finishedClamp = finishedValue));
				}
			}
		}
		//****END ON CLICK****

		//On touch
		foreach (Touch touch in Input.touches) {
			if (touch.phase == TouchPhase.Began) {
				touchStart = touch.position;
				touchStartTime = Time.time;
			}

			if (touch.phase == TouchPhase.Moved) {
				touchEnd = touch.position;
				slideAmount = touchEnd.x - touchStart.x;
				//if not beyond container bounds, slide
				if ((slideAmount < 0 && container.localPosition.x > -containerWidth) || (slideAmount > 0 && container.localPosition.x < 0)) {
					SlideContainer();
				}
			}

			if (touch.phase == TouchPhase.Ended) {
				touchEndTime = Time.time;
				touchEnd = touch.position;
				float clampPosition = 100;

				//Registered as tap
				if (finishedClamp && touchStart == touchEnd) {
					if (touchStart.x >= (Screen.width / 2 + container.transform.GetChild(0).GetComponent<RectTransform>().sizeDelta.x / 2)) {
						clampPosition = Mathf.Ceil ((container.transform.localPosition.x - buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe next
					} else if (touchStart.x <= (Screen.width / 2 - container.transform.GetChild(0).GetComponent<RectTransform>().sizeDelta.x / 2)) {
						clampPosition = Mathf.Floor ((container.transform.localPosition.x + buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe prev
					}
					if (clampPosition != 100) {
						clampPosition = Mathf.Clamp (clampPosition, -containerWidth, 0);
						if (finishedClamp) {
							StartCoroutine (ClampContainer (clampPosition, nameValue => currentLevelTitle.text = nameValue, finishedValue => finishedClamp = finishedValue));
						}
					}
				//Registered as a swipe
				} else {
					if (Mathf.Abs (touchEnd.x - touchStart.x) > minSwipeDistance && touchEndTime - touchStartTime < maxSwipeTime) {
						if (touchEnd.x - touchStart.x > 0) {
							clampPosition = Mathf.Floor ((container.transform.localPosition.x + buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe prev
						} else {
							clampPosition = Mathf.Ceil ((container.transform.localPosition.x - buttonSpacing) / buttonSpacing) * buttonSpacing; //swipe next
						}
					} else {
						clampPosition = Mathf.Round (container.transform.localPosition.x / buttonSpacing) * buttonSpacing;
					}
					clampPosition = Mathf.Clamp (clampPosition, -containerWidth, 0);
					if (finishedClamp) {
						StartCoroutine (ClampContainer (clampPosition, nameValue => currentLevelTitle.text = nameValue, finishedValue => finishedClamp = finishedValue));
					}
				}
			}
			//Only consider the first touch
			break;
		}

		//Fade, zoom in new level
		container.transform.GetChild (selectedLevelIndex).GetComponent<Image> ().color = Color.Lerp (darkColor, lightColor, 
			1 - Mathf.Abs((container.transform.localPosition.x + container.transform.GetChild (selectedLevelIndex).transform.localPosition.x)) / (0.5f *buttonSpacing));
		
		container.transform.GetChild (selectedLevelIndex).transform.localScale = Vector3.Lerp (smallScale, largeScale, 
			1 - Mathf.Abs((container.transform.localPosition.x + container.transform.GetChild (selectedLevelIndex).transform.localPosition.x)) / (0.5f *buttonSpacing));

		container.transform.GetChild (selectedLevelIndex).GetComponent<Button> ().enabled = true;

		//Fade, zoom out old level
		if (prevSelectedLevelIndex != -1) {
			container.transform.GetChild (prevSelectedLevelIndex).GetComponent<Image> ().color = Color.Lerp (darkColor, lightColor, 
				1 - Mathf.Abs ((container.transform.localPosition.x + container.transform.GetChild (prevSelectedLevelIndex).transform.localPosition.x)) / (0.5f * buttonSpacing));

			container.transform.GetChild (prevSelectedLevelIndex).transform.localScale = Vector3.Lerp (smallScale, largeScale, 
				1 - Mathf.Abs ((container.transform.localPosition.x + container.transform.GetChild (prevSelectedLevelIndex).transform.localPosition.x)) / (0.5f * buttonSpacing));
	
			container.transform.GetChild (prevSelectedLevelIndex).GetComponent<Button> ().enabled = false;
		}
	}

	//Slide container as finger slides
	private void SlideContainer() {
		container.transform.Translate(new Vector3 (slideAmount, 0, 0));
		Mathf.Clamp (container.transform.position.x, -containerWidth, 0);
		touchStart = touchEnd;
		int nextIndex = Mathf.Clamp(Mathf.RoundToInt(-container.transform.localPosition.x / buttonSpacing), 0, container.transform.childCount - 1);
		if (nextIndex != selectedLevelIndex) {
			prevSelectedLevelIndex = selectedLevelIndex;
		}
		selectedLevelIndex = nextIndex;
		currentLevelTitle.text = (container.transform.GetChild (selectedLevelIndex).GetComponent<LevelButton> ().levelName);
	}

	//Clamp container to nearest levelButton position
	private IEnumerator ClampContainer(float clampPosition, System.Action<string> levelName, System.Action<bool> finished) {
		float elapsedTime = 0;
		float startX = container.transform.localPosition.x;
		int nextIndex;
		//Lerp container to next position
		while (container.transform.localPosition.x != clampPosition) {
			container.transform.localPosition = new Vector3 (Mathf.Lerp (startX, clampPosition, elapsedTime / clampSpeed), container.transform.localPosition.y, 0);
			elapsedTime += Time.deltaTime;
			nextIndex = Mathf.Clamp(Mathf.RoundToInt(-container.transform.localPosition.x / buttonSpacing), 0, container.transform.childCount - 1);
			if (nextIndex != selectedLevelIndex) {
				prevSelectedLevelIndex = selectedLevelIndex;
			}
			selectedLevelIndex = nextIndex;
			levelName(container.transform.GetChild (selectedLevelIndex).GetComponent<LevelButton> ().levelName);
			finished (false);
			yield return null;
		}
		finished (true);
	}
}