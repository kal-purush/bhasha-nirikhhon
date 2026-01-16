//Written by Ruan "Ru" Klinkert 

//Class for generating 3D graphs(Bar, Point, Line and Pie) and mapping onto a target

using UnityEngine;
using System.Collections;
using Vuforia;

public class ModelInstantiator : MonoBehaviour {
	public TrackableBehaviour theTrackable;

	private GameObject trackableGameObject;

	//Default colors
	Color[] colors = new Color[]{ 
		new Color32((byte)91,(byte)155,(byte)213,(byte)255),
		new Color32((byte)237,(byte)125,(byte)49,(byte)255),
		new Color32((byte)165,(byte)165,(byte)165,(byte)255),
		new Color32((byte)255,(byte)192,(byte)0,(byte)255),
		new Color32((byte)68,(byte)114,(byte)196,(byte)255),
		new Color32((byte)112,(byte)173,(byte)71,(byte)255)
	};
	int current_colour = 0;

	private bool mSwapModel = false;
	//Reference to model
	GameObject graph = null;

	void Start () {
		Input.simulateMouseWithTouches = true;

		if (theTrackable == null) {
			Debug.Log ("Warning: Trackable not set !!");
		} else {
			SetModel();
		}
	}
		
	//Text to be displayed in GUI
	string guitext = "";

	#region GUI
	void OnGUI() {
		//Display text on GUI
		if (guitext != "") {
			GUIStyle style = new GUIStyle ();
			style.alignment = TextAnchor.UpperCenter;
			int fontsize = 48;

			Vector2 size;

			//Get font size so that text does not exceed frame width
			do {

				style.fontSize = fontsize;
				size = style.CalcSize(new GUIContent(guitext));
				fontsize -= 2;

			}
			while (size.x > Screen.width);

			style.normal.textColor = Color.white;


			GUI.Label (new Rect (0, 0, Screen.width, 150), guitext, style);
		}

		//Swap button
		if (GUI.Button (new Rect (50, 150, 250, 100), "Swap")) {
			mSwapModel = true;
		}
			
	}
	#endregion

	private Vector3 defaultScale = Vector3.one;
	private bool swiping = false;
	private Vector2 lastPosition;

	#region Update
	void Update () {
		if (mSwapModel && theTrackable != null) {
			SetModel ();
			mSwapModel = false;
		} 
			
		///////////////////////
		///  Detect touch  ///
		//////////////////////


		if (graph != null) {
			if (theTrackable.CurrentStatus == TrackableBehaviour.Status.TRACKED) {


				//If two fingers on screen
				if (Input.touchCount == 2) {
					// Store both touches.
					Touch touchZero = Input.GetTouch (0);
					Touch touchOne = Input.GetTouch (1);

					// Find the position in the previous frame of each touch.
					Vector2 touchZeroPrevPos = touchZero.position - touchZero.deltaPosition;
					Vector2 touchOnePrevPos = touchOne.position - touchOne.deltaPosition;

					// Find the magnitude of the vector (the distance) between the touches in each frame.
					float prevTouchDeltaMag = (touchZeroPrevPos - touchOnePrevPos).magnitude;
					float touchDeltaMag = (touchZero.position - touchOne.position).magnitude;

					// Find the difference in the distances between each frame.
					float deltaMagnitudeDiff = prevTouchDeltaMag - touchDeltaMag;

					if (deltaMagnitudeDiff < 0) { //If fingers are moving apart -> grow
						graph.transform.localScale *= 1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f);
					} else { //If fingers are moving closer -> shrink
						if ((graph.transform.localScale.x / (1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f))) >= defaultScale.x) {
							//Shrink
							graph.transform.localScale /= 1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f);

							float newWidth = (graph.transform.localScale.x / defaultScale.x);
							float heightRatio = graph.transform.Find ("Graph Base").transform.lossyScale.z / graph.transform.Find ("Graph Base").transform.lossyScale.x;
							float newHeight = (graph.transform.localScale.y / defaultScale.y) * heightRatio;

							//Right edge
							if (graph.transform.localPosition.x >= (newWidth / 2)) {
								graph.transform.localPosition = new Vector3 (
									graph.transform.localPosition.x / (1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f)),
									graph.transform.localPosition.y,
									graph.transform.localPosition.z);
							}

							//Left edge
							if (graph.transform.localPosition.x <= -(newWidth / 2)) {
								graph.transform.localPosition = new Vector3 (
									graph.transform.localPosition.x / (1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f)),
									graph.transform.localPosition.y,
									graph.transform.localPosition.z);
							}

							//Top edge
							if (graph.transform.localPosition.z >= (newHeight / 2)) {
								graph.transform.localPosition = new Vector3 (
									graph.transform.localPosition.x,
									graph.transform.localPosition.y,
									graph.transform.localPosition.z / (1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f)));
							}

							//Bottom edge
							if (graph.transform.localPosition.z <= -(newHeight / 2)) {
								graph.transform.localPosition = new Vector3 (
									graph.transform.localPosition.x,
									graph.transform.localPosition.y,
									graph.transform.localPosition.z / (1f + (Mathf.Abs (deltaMagnitudeDiff) / 10f)));
							}

						}
					}

			
					//If single finger
				} else if (Input.touchCount == 1) {

					Touch touch = Input.GetTouch (0);

					if (touch.deltaPosition.sqrMagnitude != 0){
						if (swiping == false){ //If finger just pressed
							swiping = true;
							lastPosition = touch.position;
							return;
						}
						else{ //If finger dragging
							Vector2 direction = Input.GetTouch(0).position - lastPosition;

							float swipeSpeed = 5.0f * (defaultScale.x / graph.transform.localScale.x);

							//Check whether a greater distance was moved across the x-axis or y-axis
							if (Mathf.Abs(direction.x) > Mathf.Abs(direction.y)){
								//X movement greater than Y movement
								float swipeValue = touch.position.x - lastPosition.x;
								float newWidth = (graph.transform.localScale.x / defaultScale.x);

								if (direction.x > 0) { //If moving right - finger moved from lower x value to higher x value
									if (graph.transform.localPosition.x + ((Mathf.Abs (swipeValue / Screen.width)) / swipeSpeed) <= (newWidth / 2)) {
										graph.transform.localPosition = new Vector3 (
											graph.transform.localPosition.x + ((Mathf.Abs (swipeValue / Screen.width)) / swipeSpeed),
											graph.transform.localPosition.y,
											graph.transform.localPosition.z);
									} else {
										graph.transform.localPosition = new Vector3 (
											(newWidth / 2),
											graph.transform.localPosition.y,
											graph.transform.localPosition.z);
									}

								} else { //If moving left - finger moved from higher x value to lower x value
									if (graph.transform.localPosition.x - ((Mathf.Abs (swipeValue / Screen.width)) / swipeSpeed) >= -(newWidth / 2)) {
										graph.transform.localPosition = new Vector3 (
											graph.transform.localPosition.x - ((Mathf.Abs (swipeValue / Screen.width)) / swipeSpeed),
											graph.transform.localPosition.y,
											graph.transform.localPosition.z);
									} else {
										graph.transform.localPosition = new Vector3 (
											-(newWidth / 2),
											graph.transform.localPosition.y,
											graph.transform.localPosition.z);
									}
								}
							}
							else{
								//Y movement greater than X movement
								float swipeValue = touch.position.y - lastPosition.y;
								//Adjust for fact that target and model are not perfect squares
								//	- Dimensions of target can be ignored because only the center is required and target would always be bound to 1f*1f square
								float heightRatio = graph.transform.Find ("Graph Base").transform.lossyScale.z / graph.transform.Find ("Graph Base").transform.lossyScale.x;
								float newHeight = (graph.transform.localScale.y / defaultScale.y) * heightRatio;

								if (direction.y > 0) {  //If moving up - finger moved from lower y value to higher y value
									if (graph.transform.localPosition.z + ((Mathf.Abs (swipeValue / Screen.height)) / swipeSpeed) <= (newHeight / 2)) {
										graph.transform.localPosition = new Vector3 (
											graph.transform.localPosition.x,
											graph.transform.localPosition.y,
											graph.transform.localPosition.z + ((Mathf.Abs (swipeValue / Screen.height)) / swipeSpeed));
									} else {
										graph.transform.localPosition = new Vector3 (
											graph.transform.localPosition.x,
											graph.transform.localPosition.y,
											(newHeight / 2));
									}

								} else {  //If moving down - finger moved from higher y value to lower y value
									if (graph.transform.localPosition.z - ((Mathf.Abs (swipeValue / Screen.height)) / swipeSpeed) >= -(newHeight / 2)) {
										graph.transform.localPosition = new Vector3 (
											graph.transform.localPosition.x,
											graph.transform.localPosition.y,
											graph.transform.localPosition.z - ((Mathf.Abs (swipeValue / Screen.height)) / swipeSpeed));
									} else {
										graph.transform.localPosition = new Vector3 (
											graph.transform.localPosition.x,
											graph.transform.localPosition.y,
											-(newHeight / 2));
									}
								}

							}
						}
					}
					else{
						swiping = false;
					}
				}
			}
		}
			

	}
	#endregion

	int model = 0;

	#region Set model
	private void SetModel() {

		trackableGameObject = theTrackable.gameObject;

		//////////////////
		// Create Model //
		//////////////////

		int series_count = (int)Random.Range(1,5);
		int category_count = (int)Random.Range(1,20);

		float[,] values = new float[series_count, category_count];

		for (int s = 0; s < series_count; s++){
			for (int c = 0; c < category_count; c++)
				values [s, c] = Random.Range (1, 100);
		}

		string[] categories = { "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert","John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert", "John", "Jane","Peter","Mary","Robert" };
		string[] series = { "Apples", "Pears", "Bananas" };

		current_colour = 0;

		//Trackable object = object that graph has to be attached to
		//Title = Title of graph
		//Values = Two dimentional array of floats containing all graph values
		//Categories = string array with names of each category (x-axis)
		//Series =  string array with names of each series (z-axis)
		//Categories_title = title for categories
		//Series_title =  title for series
		//Series_Count = number of series
		//Category_Count = number of categories

		trackableGameObject.transform.position = new Vector3 (0f, 0f, 0f);
		trackableGameObject.transform.eulerAngles = new Vector3 (0f, 0f, 0f);


		foreach (Transform child in trackableGameObject.transform)
			GameObject.Destroy (child.gameObject);

		if (model == 0) {
			graph = generateBarGraph ("Graph", values, categories, series, "Employees", "", series_count, category_count);
		} else if (model == 1) {
			graph = generateLineGraph ("Graph", values, categories, series, "Employees", "", series_count, category_count);
		} else if (model == 2) {
			graph = generatePointGraph ("Graph", values, categories, series, "Employees", "", series_count, category_count);
		} else if (model == 3) {
			graph = generatePieChart ("Graph", values, categories, series, "Employees", "", series_count, category_count);
		}

		guitext = "Randomly generated graph " + model.ToString ();
		if (trackableGameObject != null)
			graph.transform.parent = trackableGameObject.transform;

		defaultScale = graph.transform.localScale;

		model++;
		if (model > 3)
			model = 0;
	}
	#endregion


	#region Draw Pie Chart
	GameObject generatePieChart(
		string title, 
		float[,] values, 
		string[] categories,
		string[] series, 
		string category_title,
		string series_title,
		int series_count, 
		int category_count
	){
		float totalWidth = 25f;
		float ringWidth = totalWidth/series_count;
		float height = 2f;
		int nbTotalSlice;
		int minimumSlices = 50;
		float spacing = ringWidth / 10f;
		float line_size = 0.2f;

		float text_label_scale = 1f; //Scale of text label
		float value_label_scale = 1f; //Scale of value label

		//Calculate label scales
		GameObject temp_Object = new GameObject ();
		TextMesh temp_Mesh = temp_Object.AddComponent<TextMesh> ();
		temp_Mesh.fontSize = 200;
		temp_Mesh.characterSize = 1;

		for (int c = 0; c < categories.Length; c++) {

			//Get size of each values' label
			temp_Mesh.text = categories[c];
			float temp_scale = ((totalWidth/4f) / CalculateTextScale (temp_Mesh));

			if (text_label_scale > temp_scale)
				text_label_scale = temp_scale;
		}

		for (int s = 0; s < series_count; s++) {
			for (int c = 0; c < category_count; c++) {

				//Get size of each values' label
				temp_Mesh.text = values [s, c].ToString();
				float temp_scale = ((ringWidth/2f) / CalculateTextScale (temp_Mesh));

				if (value_label_scale > temp_scale)
					value_label_scale = temp_scale;
			}
		}
		Destroy (temp_Object);

		GameObject pieChart = new GameObject ("Graph Base");

		Color32[] pieceColour = new Color32[category_count];
		for (int c = 0; c < category_count; c++) {
			pieceColour[c] = getColor ();
		}

		for (int s = 0; s < series_count; s++) {

			//Shape of ring
			float bottomRadiusOuter = ((s + 1) * ringWidth) - spacing; 
			float topRadiusOuter = ((s + 1) * ringWidth) - spacing;
			float bottomRadiusInner = (s * ringWidth); 
			float topRadiusInner = (s * ringWidth);
			float bottom = 0f;


			float totalValue = 0f;
			float alreadyPass = 0f;

			float smallest = values [0, 0];

			for (int c = 0; c < category_count; c++) {
				if (smallest <= 0)
					smallest = values [s, c];
				else if (values [s, c] < smallest && values [s, c] > 0)
					smallest = values [s, c];
				totalValue += values [s, c];
			}

			//Calculate minimum amount of slices that could host all values
			nbTotalSlice = Mathf.RoundToInt(totalValue/smallest);
			if (nbTotalSlice < minimumSlices)
				nbTotalSlice *= Mathf.RoundToInt (minimumSlices / nbTotalSlice);
			int totalPieces = nbTotalSlice;
			nbTotalSlice = 0;
			for (int c = 0; c < category_count; c++) {
				nbTotalSlice += Mathf.CeilToInt (totalPieces / totalValue * values [s, c]);
				//print (nbTotalSlice + " " + totalPieces + " " + totalValue + " " + values [s, c]);
			}

			for (int c = 0; c < category_count; c++) {

				Material material = new Material (Shader.Find ("Diffuse"));
				material.SetTextureScale ("_MainTex", new Vector2 (100, 100));
				material.color = pieceColour[c];

				int slices = Mathf.CeilToInt (totalPieces / totalValue * values [s, c]);

				//Each slices is drawn individually
				//A category gets a piece of the circle, that piece is made up of a number of slices
				for (int sliceCount = 0; sliceCount < slices; sliceCount++) {

					GameObject pieSlice = new GameObject ("pieSlice" + c.ToString());
					pieSlice.transform.parent = pieChart.transform;
					pieSlice.AddComponent<MeshFilter> ();
					pieSlice.AddComponent<MeshRenderer> ();

					pieSlice.GetComponent<MeshRenderer> ().material = material;

					Mesh mesh = new Mesh ();
					pieSlice.GetComponent<MeshFilter> ().mesh = mesh;
					mesh.Clear ();

					int nbSides = 2;
					int nbVerticesCap = nbSides * 2 + 2;
					int nbVerticesSides = nbSides * 2 + 2;

					//////////////////
					//   Vertices  //
					/////////////////

					// bottom + top + sides
					Vector3[] vertices = new Vector3[nbVerticesCap * 2 + nbVerticesSides * 2];
					int vert = 0;
					float _2pi = Mathf.PI * 2f;

					// Bottom cap
					int sideCounter = 0;
					while (vert < nbVerticesCap) {
						sideCounter = sideCounter == nbSides ? 0 : sideCounter;

						//Divide to get proper angle
						float r1 = ((float)(sideCounter++) / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);

						float cos = Mathf.Cos (r1);
						float sin = Mathf.Sin (r1);

						vertices [vert] = new Vector3 (cos * (bottomRadiusInner * .5f), bottom, sin * (bottomRadiusInner * .5f)); //Inner point of line
						vertices [vert + 1] = new Vector3 (cos * (bottomRadiusOuter * .5f), bottom, sin * (bottomRadiusOuter * .5f)); //Outer point of line
						vert += 2;
					}

					// Top cap
					sideCounter = 0;
					while (vert < nbVerticesCap * 2) {
						sideCounter = sideCounter == nbSides ? 0 : sideCounter;

						//Divide to get proper angle
						float r1 = ((float)(sideCounter++) / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);

						float cos = Mathf.Cos (r1);
						float sin = Mathf.Sin (r1);

						vertices [vert] = new Vector3 (cos * (topRadiusInner * .5f), bottom + height, sin * (topRadiusInner * .5f)); //Inner point of line
						vertices [vert + 1] = new Vector3 (cos * (topRadiusOuter * .5f), bottom + height, sin * (topRadiusOuter * .5f)); //Outer point of line
						vert += 2;
					}

					// Sides (out)
					sideCounter = 0;
					while (vert < nbVerticesCap * 2 + nbVerticesSides) {
						sideCounter = sideCounter == nbSides ? 0 : sideCounter;

						//Divide to get proper angle
						float r1 = ((float)(sideCounter++) / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);

						float cos = Mathf.Cos (r1);
						float sin = Mathf.Sin (r1);

						vertices [vert] = new Vector3 (cos * (topRadiusOuter * .5f), bottom + height, sin * (topRadiusOuter * .5f));
						vertices [vert + 1] = new Vector3 (cos * (bottomRadiusOuter * .5f), bottom, sin * (bottomRadiusOuter * .5f));
						vert += 2;

					}

					// Sides (in)
					sideCounter = 0;
					while (vert < vertices.Length) {
						sideCounter = sideCounter == nbSides ? 0 : sideCounter;

						float r1 = ((float)(sideCounter++) / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);
						float cos = Mathf.Cos (r1);
						float sin = Mathf.Sin (r1);

						vertices [vert] = new Vector3 (cos * (topRadiusInner * .5f), bottom + height, sin * (topRadiusInner * .5f));
						vertices [vert + 1] = new Vector3 (cos * (bottomRadiusInner * .5f), bottom, sin * (bottomRadiusInner * .5f));
						vert += 2;
					}


					//////////////////
					//   Normales  //
					/////////////////

					// bottom + top + sides
					Vector3[] normales = new Vector3[vertices.Length];
					vert = 0;

					// Bottom cap
					while (vert < nbVerticesCap) {
						normales [vert++] = Vector3.down;
					}

					// Top cap
					while (vert < nbVerticesCap * 2) {
						normales [vert++] = Vector3.up;
					}

					// Sides (out)
					sideCounter = 0;
					while (vert < nbVerticesCap * 2 + nbVerticesSides) {
						sideCounter = sideCounter == nbSides ? 0 : sideCounter;

						float r1 = ((float)(sideCounter++) / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);

						normales [vert] = new Vector3 (Mathf.Cos (r1), 0f, Mathf.Sin (r1));
						normales [vert + 1] = normales [vert];
						vert += 2;
					}

					// Sides (in)
					sideCounter = 0;
					while (vert < vertices.Length) {
						sideCounter = sideCounter == nbSides ? 0 : sideCounter;

						float r1 = ((float)(sideCounter++) / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);

						normales [vert] = -(new Vector3 (Mathf.Cos (r1), 0f, Mathf.Sin (r1)));
						normales [vert + 1] = normales [vert];
						vert += 2;
					}


					//////////////////
					//      UVs    //
					/////////////////

					Vector2[] uvs = new Vector2[vertices.Length];

					vert = 0;
					// Bottom cap
					sideCounter = 0;
					while (vert < nbVerticesCap) {
						float t = (float)(sideCounter++) / nbSides;
						uvs [vert++] = new Vector2 (0f, t);
						uvs [vert++] = new Vector2 (1f, t);
					}

					// Top cap
					sideCounter = 0;
					while (vert < nbVerticesCap * 2) {
						float t = (float)(sideCounter++) / nbSides;
						uvs [vert++] = new Vector2 (0f, t);
						uvs [vert++] = new Vector2 (1f, t);
					}

					// Sides (out)
					sideCounter = 0;
					while (vert < nbVerticesCap * 2 + nbVerticesSides) {
						float t = (float)(sideCounter++) / nbSides;
						uvs [vert++] = new Vector2 (t, 0f);
						uvs [vert++] = new Vector2 (t, 1f);
					}

					// Sides (in)
					sideCounter = 0;
					while (vert < vertices.Length) {
						float t = (float)(sideCounter++) / nbSides;
						uvs [vert++] = new Vector2 (t, 0f);
						uvs [vert++] = new Vector2 (t, 1f);
					}

					//////////////////
					//  Triangles  //
					/////////////////

					int nbFace = nbSides * 4;
					int nbTriangles = nbFace * 2;
					int nbIndexes = nbTriangles * 3;
					int[] triangles = new int[nbIndexes];

					// Bottom cap
					int i = 0;
					sideCounter = 0;
					while (sideCounter < nbSides) {
						int current = sideCounter * 2;
						int next = sideCounter * 2 + 2;

						triangles [i++] = next + 1;
						triangles [i++] = next;
						triangles [i++] = current;

						triangles [i++] = current + 1;
						triangles [i++] = next + 1;
						triangles [i++] = current;

						sideCounter++;
					}

					// Top cap
					while (sideCounter < nbSides * 2) {
						int current = sideCounter * 2 + 2;
						int next = sideCounter * 2 + 4;

						triangles [i++] = current;
						triangles [i++] = next;
						triangles [i++] = next + 1;

						triangles [i++] = current;
						triangles [i++] = next + 1;
						triangles [i++] = current + 1;

						sideCounter++;
					}

					// Sides (out)
					while (sideCounter < nbSides * 3) {
						int current = sideCounter * 2 + 4;
						int next = sideCounter * 2 + 6;

						triangles [i++] = current;
						triangles [i++] = next;
						triangles [i++] = next + 1;


						triangles [i++] = current;
						triangles [i++] = next + 1;
						triangles [i++] = current + 1;

						sideCounter++;
					}

					// Sides (in)
					while (sideCounter < nbSides * 4) {
						int current = sideCounter * 2 + 6;
						int next = sideCounter * 2 + 8;

						triangles [i++] = next + 1;
						triangles [i++] = next;
						triangles [i++] = current;

						triangles [i++] = current + 1;
						triangles [i++] = next + 1;
						triangles [i++] = current;

						sideCounter++;
					}

					mesh.vertices = vertices;
					mesh.normals = normales;
					mesh.uv = uvs;
					mesh.triangles = triangles;

					mesh.RecalculateBounds ();
					mesh.Optimize ();

					//If in middle of piece
					if (sliceCount == (int)(slices / 2)) {

						float multiplier = (float)(((float)slices / 2) - Mathf.FloorToInt ((float)slices / 2));
						float r1 = (multiplier / nbTotalSlice * _2pi) + ((float)(alreadyPass) / nbTotalSlice * _2pi);

						float cos = Mathf.Cos (r1);
						float sin = Mathf.Sin (r1);

						//Add label to middle of piece
						Vector2 inner = new Vector2(
							cos * (topRadiusInner * .5f), 
							sin * (topRadiusInner * .5f)
						);
						Vector2 outer = new Vector2(
							cos * (topRadiusOuter * .5f),
							sin * (topRadiusOuter * .5f)
						);

						float x_Center;
						float z_Center;

						if (inner.x > outer.x) {
							x_Center = ((inner.x - outer.x) / 2f) + outer.x;
						} else {
							x_Center = ((outer.x - inner.x) / 2f) + inner.x;
						}
						if (inner.y > outer.y) { //y represents z - Vector2 uses x and y
							z_Center = ((inner.y - outer.y) / 2f) + outer.y;
						} else {
							z_Center = ((outer.y - inner.y) / 2f) + inner.y;
						}

						GameObject fontObj = new GameObject();
						fontObj.transform.parent = pieChart.transform;
						fontObj.name = s + ":" + c + " value label";

						TextMesh tm = fontObj.AddComponent<TextMesh>();
						fontObj.GetComponent<Renderer>().material.color = Color.white;
						tm.font = (Resources.Load("Courier") as Font);
						tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
						if (totalValue == 100f) 
							tm.text = values [s, c].ToString() + " %";
						else tm.text = values [s, c].ToString();
						tm.fontSize = 200;
						tm.characterSize = 1;

						//Set scale relative to parent
						fontObj.transform.localScale = new Vector3 (
							value_label_scale,
							value_label_scale,
							1f
						);

						//Set height of label to top of bar (0.5 of the bar's relative height)
						fontObj.transform.localPosition = new Vector3 (
							x_Center,
							bottom + height + 0.1f,
							z_Center
						);

						//Anchor text to middle of bar
						tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

						//Rotate text to point upwards
						fontObj.transform.Rotate (90, 0, 0);



						//Add labels to outermost ring
						if (s == series_count - 1) {

							if (c < categories.Length) {

								//The height of the bar
								float bar_height = height / 4f;

								//Create bar
								GameObject bar_cube = GameObject.CreatePrimitive (PrimitiveType.Cube);
								bar_cube.GetComponent<MeshRenderer> ().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;

								bar_cube.name = "Label: " + categories [c];

								//Add as child of base
								bar_cube.transform.parent = pieChart.transform;

								Vector3 start = new Vector3 (
									               cos * ((((series_count + 1) * ringWidth) - 1f) * .5f), 
									               ((bar_height / 2) + (height + (c * height))), 
									               sin * ((((series_count + 1) * ringWidth) - 1f) * .5f)
								               );
								Vector3 finish = new Vector3 (
									                cos * (topRadiusOuter * .5f),
									                (bottom + height) / 2.0f, 
									                sin * (topRadiusOuter * .5f)
								                );


								//Set position of bar relative to parent
								bar_cube.transform.localPosition = start;

								bar_cube.transform.localRotation = Quaternion.identity;

								//Set scale relative to parent
								bar_cube.transform.localScale = new Vector3 (
									totalWidth / 4f, 
									bar_height, 
									totalWidth / 8f
								);

								//Set color of bar
								bar_cube.GetComponent<Renderer> ().material.color = pieceColour [c];

								bar_cube.SetActive (true);

								//Add text to top of bar
								fontObj = new GameObject ();
								fontObj.transform.parent = bar_cube.transform;
								fontObj.name = bar_cube.name + " text";

								tm = fontObj.AddComponent<TextMesh> ();
								fontObj.GetComponent<Renderer> ().material.color = Color.white;
								tm.font = (Resources.Load ("Courier") as Font);
								tm.GetComponent<Renderer> ().material.shader = Shader.Find ("zTextShader");
								tm.text = categories [c];
								tm.fontSize = 200;
								tm.characterSize = 1;

								//Set scale relative to parent
								fontObj.transform.localScale = new Vector3 (
									text_label_scale,
									text_label_scale,
									1f
								);

								//Set height of label to top of bar (0.5 of the bar's relative height)
								fontObj.transform.localPosition = new Vector3 (
									0f,
									0.51f,
									0f
								);

								//Anchor text to middle of bar
								tm.GetComponent<TextMesh> ().anchor = TextAnchor.MiddleCenter;

								//Rotate text to point upwards
								fontObj.transform.Rotate (90, 0, 0);

								//Draw line to graph

								float x_diff;
								float x_actual;
								if (finish.x > start.x) {
									x_diff = ((finish.x - start.x) / 2);
									x_actual = x_diff + start.x;
								} else {
									x_diff = ((start.x - finish.x) / 2);
									x_actual = x_diff + finish.x;
								}
								float y_diff;
								float y_actual;
								if (finish.y > start.y) {
									y_diff = ((finish.y - start.y) / 2);
									y_actual = y_diff + start.y;
								} else {
									y_diff = ((start.y - finish.y) / 2);
									y_actual = y_diff + finish.y;
								}
								float z_diff;
								float z_actual;
								if (finish.z > start.z) {
									z_diff = ((finish.z - start.z) / 2);
									z_actual = z_diff + start.z;
								} else {
									z_diff = ((start.z - finish.z) / 2);
									z_actual = z_diff + finish.z;
								}

								Vector3 cylinderPosition = new Vector3 (
									                          x_actual,
									                          y_actual,
									                          z_actual
								                          );

								GameObject line = GameObject.CreatePrimitive (PrimitiveType.Cylinder);
								line.GetComponent<MeshRenderer> ().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;
								line.transform.parent = pieChart.transform;

								//Set position of point relative to parent
								line.transform.localPosition = cylinderPosition;

								line.transform.localRotation = Quaternion.identity;

								float scale = Mathf.Sqrt (y_diff * y_diff + x_diff * x_diff + z_diff * z_diff);

								//Set scale relative to parent
								line.transform.localScale = new Vector3 (
									line_size, 
									scale, //Length
									line_size
								);

								//Set color of point
								line.GetComponent<Renderer> ().material = material;

								line.transform.rotation = Quaternion.FromToRotation (Vector3.up, finish - start);

								line.SetActive (true);
							}
						}
					}

					alreadyPass += (nbSides - 1);
				}

			}
		}

		pieChart.transform.localPosition = new Vector3 (0f, 0f, 0f);
		pieChart.transform.localRotation = Quaternion.identity;
		pieChart.transform.localScale = new Vector3 (1f / totalWidth / 2f, 1f / totalWidth / 2f, 1f / totalWidth / 2f);

		return pieChart;
	}
	#endregion


	#region Draw Line Graph
	GameObject generateLineGraph(
		string title, 
		float[,] values, 
		string[] categories,
		string[] series, 
		string category_title,
		string series_title,
		int series_count, 
		int category_count
	){
		float spacing = 1f;  //Space between each point
		float base_height = 2f; //Height of the base
		float point_size = 2f;//Width and height of each point
		float point_container_size = 5f; //Width and height of point's personal space
		float line_size = 0.5f; //z and y of line

		//Calculate x and z lengths
		float x_width = category_count * (point_container_size + spacing) + point_container_size;
		float z_width = series_count * (point_container_size + spacing) + point_container_size;

		GameObject container = createBase (title, categories, series, category_title, series_title, series_count, category_count, x_width, z_width, point_container_size, base_height, spacing);

		float value_label_scale = 1f; //Scale of value label

		//Calculate label scales
		GameObject temp_Object = new GameObject ();
		TextMesh temp_Mesh = temp_Object.AddComponent<TextMesh> ();
		temp_Mesh.fontSize = 200;
		temp_Mesh.characterSize = 1;

		float highest = 0;
		float lowest = values [0, 0];

		for (int s = 0; s < series_count; s++) {
			for (int c = 0; c < category_count; c++) {

				//Get highest and lowest values in data collection
				if (values [s, c] > highest)
					highest = values [s, c];
				if (values [s, c] < lowest)
					lowest = values [s, c];

				//Get size of each values' label
				temp_Mesh.text = values [s, c].ToString();
				float temp_scale = (point_size / CalculateTextScale (temp_Mesh)) * (2f);

				if (value_label_scale > temp_scale)
					value_label_scale = temp_scale;
			}
		}

		Destroy (temp_Object);

		float range = highest - lowest;
		int tickCount = Mathf.RoundToInt (x_width/2f);
		float tickValue = generateTickInterval (range, tickCount);
		float lowestBound = tickValue * (Mathf.Round (lowest / tickValue));
		while (lowestBound >= lowest)
			lowestBound -= tickValue;

		lowestBound -= tickValue;

		if (lowest == highest)
			lowestBound -= (highest / 2);

		Color32 point_color;

		//Each series is drawn behind each other
		for (int s = 0; s < series_count; s++) {

			//Generate new color for each series
			point_color = getColor ();
			Vector3[] positions = new Vector3[category_count];

			for (int c = 0; c < category_count; c++) {

				try {
					//Create point
					GameObject point = GameObject.CreatePrimitive (PrimitiveType.Sphere);
					point.GetComponent<MeshRenderer>().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;

					//The height of the point
					float point_height = (values [s, c] - lowestBound) / tickValue;

					point.name = "Point: " + s.ToString () + ',' + c.ToString ();

					//Add as child of base
					point.transform.parent = container.transform;

					//Set position of point relative to parent
					point.transform.localPosition = new Vector3 (

						//X : X-Position of point relative to parent
						((c * (point_container_size + spacing) + point_container_size) - (x_width / 2) + (point_container_size / 2)),

						//Y : height of the center of the point relative to the center of the base
						((point_height / 2) + (base_height / 2)),

						//Z : Z-Position of point relative to parent
						((s * (point_container_size + spacing) + point_container_size) - (z_width / 2) + (point_container_size / 2))
					);

					positions[c] = point.transform.localPosition;

					point.transform.localRotation = Quaternion.identity;

					//Set scale relative to parent
					point.transform.localScale = new Vector3 (
						point_size, 
						point_size, 
						point_size
					);

					//Set color of point
					point.GetComponent<Renderer> ().material.color = point_color;

					point.SetActive (true);


					//Add text to top of point
					GameObject fontObj = new GameObject();
					fontObj.transform.parent = point.transform;
					fontObj.name = point.name + " value label";

					TextMesh tm = fontObj.AddComponent<TextMesh>();
					fontObj.GetComponent<Renderer>().material.color = Color.black;
					tm.font = (Resources.Load("Courier") as Font);
					tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
					tm.text = values [s, c].ToString();
					tm.fontSize = 200;
					tm.characterSize = 1;

					//Set scale relative to parent
					fontObj.transform.localScale = new Vector3 (
						value_label_scale,
						value_label_scale,
						1f
					);

					//Set height of label to top of bar (0.5 of the bar's relative height)
					fontObj.transform.localPosition = new Vector3 (
						0f,
						0f,
						-0.5f
					);

					//Anchor text to middle of bar
					tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

					//Add line to next point
					if( c > 0) {

						float x_diff = ((positions [c].x - positions [c - 1].x) / 2);
						float x_actual = x_diff + positions [c - 1].x;
						float y_diff;
						float y_actual;
						if(positions [c].y > positions [c - 1].y){
							y_diff = ((positions [c].y - positions [c - 1].y) / 2);
							y_actual = y_diff + positions [c - 1].y;
						}
						else{
							y_diff = ((positions [c - 1].y - positions [c].y) / 2);
							y_actual = y_diff + positions [c].y;
						}

						Vector3 cylinderPosition = new Vector3 (

							x_actual,
							//X : X-Position of point relative to parent
							//(1f/point_size) * (-0.5f - (((c * (point_container_size + spacing) + point_container_size) - (x_width / 2) + (point_container_size + spacing / 2))/2f)),

							//Y : height of the center of the point relative to the center of the base
							y_actual,

							//Z : Z-Position of point relative to parent
							((s * (point_container_size + spacing) + point_container_size) - (z_width / 2) + (point_container_size / 2))
						);

						GameObject line = GameObject.CreatePrimitive (PrimitiveType.Cylinder);
						line.GetComponent<MeshRenderer>().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;
						line.transform.parent = container.transform;

						//Set position of point relative to parent
						line.transform.localPosition = cylinderPosition;

						line.transform.localRotation = Quaternion.identity;

						float scale = Mathf.Sqrt (y_diff * y_diff + x_diff * x_diff);

						//Set scale relative to parent
						line.transform.localScale = new Vector3 (
							line_size, 
							scale, //Length
							line_size
						);

						//Set color of point
						line.GetComponent<Renderer> ().material.color = point_color;

						line.transform.rotation = Quaternion.FromToRotation(Vector3.up, positions [c] - positions [c - 1]);

						line.SetActive (true);
					}





				} catch {
				}

				//Point created
			}

			//End of series
		}

		return container;

	}
	#endregion

	#region Draw Point Graph
	GameObject generatePointGraph(
		string title, 
		float[,] values, 
		string[] categories,
		string[] series, 
		string category_title,
		string series_title,
		int series_count, 
		int category_count
	){
		float spacing = 1f;  //Space between each point
		float base_height = 2f; //Height of the base
		float point_size = 2f;//Width and height of each point
		float point_container_size = 5f;

		//Calculate x and z lengths
		float x_width = category_count * (point_container_size + spacing) + point_container_size;
		float z_width = series_count * (point_container_size + spacing) + point_container_size;

		GameObject container = createBase (title, categories, series, category_title, series_title, series_count, category_count, x_width, z_width, point_container_size, base_height, spacing);

		float value_label_scale = 1f; //Scale of value label

		//Calculate label scales
		GameObject temp_Object = new GameObject ();
		TextMesh temp_Mesh = temp_Object.AddComponent<TextMesh> ();
		temp_Mesh.fontSize = 200;
		temp_Mesh.characterSize = 1;

		float highest = 0;
		float lowest = values [0, 0];

		for (int s = 0; s < series_count; s++) {
			for (int c = 0; c < category_count; c++) {

				//Get highest and lowest values in data collection
				if (values [s, c] > highest)
					highest = values [s, c];
				if (values [s, c] < lowest)
					lowest = values [s, c];

				//Get size of each values' label
				temp_Mesh.text = values [s, c].ToString();
				float temp_scale = (point_size / CalculateTextScale (temp_Mesh)) * (2f);

				if (value_label_scale > temp_scale)
					value_label_scale = temp_scale;
			}
		}

		Destroy (temp_Object);

		float range = highest - lowest;
		int tickCount = Mathf.RoundToInt (x_width/2f);
		float tickValue = generateTickInterval (range, tickCount);
		float lowestBound = tickValue * (Mathf.Round (lowest / tickValue));
		while (lowestBound >= lowest)
			lowestBound -= tickValue;

		lowestBound -= tickValue;

		if (lowest == highest)
			lowestBound -= (highest / 2);

		Color32 point_color;

		//Each series is drawn behind each other
		for (int s = 0; s < series_count; s++) {

			//Generate new color for each series
			point_color = getColor ();

			for (int c = 0; c < category_count; c++) {

				try {
					//Create point
					GameObject point = GameObject.CreatePrimitive (PrimitiveType.Sphere);
					point.GetComponent<MeshRenderer>().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;

					//The height of the point
					float point_height = (values [s, c] - lowestBound) / tickValue;

					point.name = "Point: " + s.ToString () + ',' + c.ToString ();

					//Add as child of base
					point.transform.parent = container.transform;

					//Set position of point relative to parent
					point.transform.localPosition = new Vector3 (

						//X : X-Position of point relative to parent
						((c * (point_container_size + spacing) + point_container_size) - (x_width / 2) + (point_container_size / 2)),

						//Y : height of the center of the point relative to the center of the base
						((point_height / 2) + (base_height / 2)),

						//Z : Z-Position of point relative to parent
						((s * (point_container_size + spacing) + point_container_size) - (z_width / 2) + (point_container_size / 2))
					);

					point.transform.localRotation = Quaternion.identity;

					//Set scale relative to parent
					point.transform.localScale = new Vector3 (
						point_size, 
						point_size, 
						point_size
					);

					//Set color of point
					point.GetComponent<Renderer> ().material.color = point_color;

					point.SetActive (true);


					//Add text to top of point
					GameObject fontObj = new GameObject();
					fontObj.transform.parent = point.transform;
					fontObj.name = point.name + " value label";

					TextMesh tm = fontObj.AddComponent<TextMesh>();
					fontObj.GetComponent<Renderer>().material.color = Color.black;
					tm.font = (Resources.Load("Courier") as Font);
					tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
					tm.text = values [s, c].ToString();
					tm.fontSize = 200;
					tm.characterSize = 1;

					//Set scale relative to parent
					fontObj.transform.localScale = new Vector3 (
						value_label_scale,
						value_label_scale,
						1f
					);

					//Set height of label to top of point (0.5 of the point's relative height)
					fontObj.transform.localPosition = new Vector3 (
						0f,
						0.5f,
						0f
					);

					//Anchor text to middle of bar
					tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

					//Rotate text to point upwards
					fontObj.transform.Rotate (90, 0, 0);


				} catch {
				}

				//Point created
			}


			//End of series
		}

		return container;

	}
	#endregion

	#region Draw Bar Graph
	GameObject generateBarGraph(
		string title, 
		float[,] values, 
		string[] categories,
		string[] series, 
		string category_title,
		string series_title,
		int series_count, 
		int category_count
	){

		float bar_size = 5f; //Width and height of each bar
		float spacing = 2f;  //Space between each bar
		float base_height = 2f; //Height of the base

		//Calculate x and z lengths
		float x_width = category_count * (bar_size + spacing) + bar_size;
		float z_width = series_count * (bar_size + spacing) + bar_size;

		GameObject container = createBase (title, categories, series, category_title, series_title, series_count, category_count, x_width, z_width, bar_size, base_height, spacing);

		float value_label_scale = 1f; //Scale of value label

		//Calculate label scales
		GameObject temp_Object = new GameObject ();
		TextMesh temp_Mesh = temp_Object.AddComponent<TextMesh> ();
		temp_Mesh.fontSize = 200;
		temp_Mesh.characterSize = 1;

		//Calculate the scale of the value label
		// - Set all labels' scale to scale of largest value
		float highest = 0;
		float lowest = values [0, 0];

		for (int s = 0; s < series_count; s++) {
			for (int c = 0; c < category_count; c++) {

				//Get highest and lowest values in data collection
				if (values [s, c] > highest)
					highest = values [s, c];
				if (values [s, c] < lowest)
					lowest = values [s, c];

				//Get size of each values' label
				temp_Mesh.text = values [s, c].ToString();
				float temp_scale = (bar_size / CalculateTextScale (temp_Mesh)) * (1f + (1f / bar_size));

				if (value_label_scale > temp_scale)
					value_label_scale = temp_scale;
			}
		}

		Destroy (temp_Object);

		//Calculate scale of the graph
		float range = highest - lowest;
		int tickCount = Mathf.RoundToInt (x_width/2f);
		float tickValue = generateTickInterval (range, tickCount);
		float lowestBound = tickValue * (Mathf.Round (lowest / tickValue));
		while (lowestBound >= lowest)
			lowestBound -= tickValue;

		if (lowest == highest)
			lowestBound -= (highest / 2);

		Color32 bar_color;

		//Each series is drawn behind each other
		for (int s = 0; s < series_count; s++) {

			//Generate new color for each series
			bar_color = getColor ();

			for (int c = 0; c < category_count; c++) {

				try{

					//The height of the bar
					float bar_height = (values [s, c] - lowestBound) / tickValue;

					//Create bar
					GameObject bar_cube = GameObject.CreatePrimitive (PrimitiveType.Cube);
					bar_cube.GetComponent<MeshRenderer>().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;
					bar_cube.name = "Bar: " +  s.ToString() + ',' + c.ToString();

					//Add as child of base
					bar_cube.transform.parent = container.transform;

					//Set position of bar relative to parent
					bar_cube.transform.localPosition = new Vector3 (

						//X : X-Position of bar relative to parent
						((c * (bar_size + spacing) + bar_size) - (x_width / 2) + (bar_size / 2)),

						//Y : height of the center of the bar relative to the center of the base
						((bar_height / 2) + (base_height / 2)),

						//Z : Z-Position of bar relative to parent
						((s * (bar_size + spacing) + bar_size) - (z_width / 2) + (bar_size / 2))
					);

					bar_cube.transform.localRotation = Quaternion.identity;

					//Set scale relative to parent
					bar_cube.transform.localScale = new Vector3 (
						bar_size, 
						bar_height, 
						bar_size
					);

					//Set color of bar
					bar_cube.GetComponent<Renderer> ().material.color = bar_color;

					bar_cube.SetActive (true);

					//Add text to top of bar
					GameObject fontObj = new GameObject();
					fontObj.transform.parent = bar_cube.transform;
					fontObj.name = bar_cube.name + " value label";

					TextMesh tm = fontObj.AddComponent<TextMesh>();
					fontObj.GetComponent<Renderer>().material.color = Color.white;
					tm.font = (Resources.Load("Courier") as Font);
					tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
					tm.text = values [s, c].ToString();
					tm.fontSize = 200;
					tm.characterSize = 1;

					//Set scale relative to parent
					fontObj.transform.localScale = new Vector3 (
						value_label_scale,
						value_label_scale,
						1f
					);

					//Set height of label to top of bar (0.5 of the bar's relative height)
					fontObj.transform.localPosition = new Vector3 (
						0f,
						0.51f,
						0f
					);

					//Anchor text to middle of bar
					tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

					//Rotate text to point upwards
					fontObj.transform.Rotate (90, 0, 0);

					//Bar created
				}
				catch{
				}
			}

			//End of series
		}

		return container;
	}
	#endregion


	#region Draw Base
	//Draw and return base and container
	GameObject createBase(
		string title, 
		string[] categories, 
		string[] series, 
		string category_title,
		string series_title,
		int series_count,
		int category_count, 
		float x_width,
		float z_width,
		float bar_size,
		float base_height,
		float spacing
	){
		float caption_label_scale = 1f; //Scale of caption label

		//Calculate label scales
		GameObject temp_Object = new GameObject ();
		TextMesh temp_Mesh = temp_Object.AddComponent<TextMesh> ();
		temp_Mesh.fontSize = 200;
		temp_Mesh.characterSize = 1;

		//Determine scale of caption labels
		//Check category labels
		for (int c = 0; c < categories.Length; c++) {
			temp_Mesh.text = categories[c];
			float temp_scale = (bar_size / CalculateTextScale (temp_Mesh)) * (2f);

			if (caption_label_scale > temp_scale)
				caption_label_scale = temp_scale;
		}

		//Check series labels
		for (int s = 0; s < series.Length; s++) {
			temp_Mesh.text = series[s];
			float temp_scale = (bar_size / CalculateTextScale (temp_Mesh)) * (2f);

			if (caption_label_scale > temp_scale)
				caption_label_scale = temp_scale;
		}


		Destroy (temp_Object);



		//Create container
		// - All objects would be relative to container
		GameObject container = new GameObject ("Container");

		container.transform.localPosition = new Vector3 (0f, 0f, 0f);
		container.transform.localRotation = Quaternion.identity;
		container.transform.localScale = new Vector3 (1f/x_width, 1f/x_width, 1f/x_width);

		container.SetActive (true);


		//Create base
		GameObject base_cube = GameObject.CreatePrimitive(PrimitiveType.Cube);
		base_cube.GetComponent<MeshRenderer>().shadowCastingMode = UnityEngine.Rendering.ShadowCastingMode.Off;
		base_cube.name = "Graph Base"; //DO NOT CHANGE - swiping procedure requires component

		base_cube.transform.parent = container.transform;

		base_cube.transform.localPosition = new Vector3(0f,0f,0f);
		base_cube.transform.localRotation = Quaternion.identity;
		base_cube.transform.localScale = new Vector3 (x_width, base_height, z_width);

		base_cube.GetComponent<Renderer>().material.color = Color.white;
		base_cube.SetActive(true);

		//Draw labels
		//Draw category title if not null
		if (category_title != null && category_title != "") {
			GameObject titleObj = new GameObject("Category title");

			//Add as child of container
			titleObj.transform.parent = container.transform;

			//Set position of plane relative to parent
			titleObj.transform.localPosition = new Vector3 (
				//X : X-Position of plane relative to parent
				0f,
				//Y : Y-Position of plane relative to parent
				base_height/2,
				//Z : Z-Position of plane relative to parent
				((bar_size / 2) - (z_width / 2) - (bar_size / 4))
			);

			//Set scale relative to parent
			titleObj.transform.localScale = new Vector3 (
				(bar_size), 
				1f,
				(bar_size) / 2f
			);
				
			titleObj.SetActive(true);


			//Add text to label
			GameObject fontObj = new GameObject();
			fontObj.transform.parent = titleObj.transform;
			fontObj.name = titleObj.name + " text";

			TextMesh tm = fontObj.AddComponent<TextMesh>();
			fontObj.GetComponent<Renderer>().material.color = Color.black;
			tm.font = (Resources.Load("Courier") as Font);
			tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
			tm.text = category_title;
			tm.fontSize = 200;
			tm.characterSize = 1;

			//Set scale relative to parent
			fontObj.transform.localScale = new Vector3 (
				caption_label_scale,
				caption_label_scale,
				1f
			);

			fontObj.transform.localPosition = new Vector3 (
				0f,
				0.01f,
				0f
			);

			tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

			//Rotate text to point upwards
			fontObj.transform.Rotate (90, 0, 0);
		}

		//Categories
		int c_length = 0;
		if (categories == null) {
			c_length = 0;
		} else {
			if (category_count < categories.Length) {
				c_length = category_count;
			} else {
				c_length = categories.Length;
			}
		}

		for (int c = 0; c < c_length; c++) {
			GameObject label = new GameObject("Category " + c.ToString());

			//Add as child of container
			label.transform.parent = container.transform;

			if (category_title != null && category_title != "") {
				//Set position of plane relative to parent
				label.transform.localPosition = new Vector3 (
					//X : X-Position of plane relative to parent
					((c * (bar_size + spacing) + bar_size) - (x_width / 2) + (bar_size / 2)),
					//Y : Y-Position of plane relative to parent
					base_height/2,
					//Z : Z-Position of plane relative to parent
					((bar_size / 2) - (z_width / 2) + (bar_size / 4))
				);

				//Set scale relative to parent
				label.transform.localScale = new Vector3 (
					(bar_size), 
					1f,
					(bar_size) / 2f
				);
			} else {
				//Set position of plane relative to parent
				label.transform.localPosition = new Vector3 (
					//X : X-Position of plane relative to parent
					((c * (bar_size + spacing) + bar_size) - (x_width / 2) + (bar_size / 2)),
					//Y : Y-Position of plane relative to parent
					base_height/2,
					//Z : Z-Position of plane relative to parent
					((bar_size / 2) - (z_width / 2))
				);

				//Set scale relative to parent
				label.transform.localScale = new Vector3 (
					(bar_size), 
					1f,
					(bar_size)
				);
			}
				
			label.SetActive(true);


			//Add text to label
			GameObject fontObj = new GameObject();
			fontObj.transform.parent = label.transform;
			fontObj.name = label.name + " text";

			TextMesh tm = fontObj.AddComponent<TextMesh>();
			fontObj.GetComponent<Renderer>().material.color = Color.black;
			tm.font = (Resources.Load("Courier") as Font);
			tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
			tm.text = categories[c];
			tm.fontSize = 200;
			tm.characterSize = 1;

			//Set scale relative to parent
			fontObj.transform.localScale = new Vector3 (
				caption_label_scale,
				caption_label_scale,
				1f
			);

			fontObj.transform.localPosition = new Vector3 (
				0f,
				0.01f,
				0f
			);

			tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

			//Rotate text to point upwards
			fontObj.transform.Rotate (90, 0, 0);
		}

		//Draw series title if not null
		if (series_title != null && series_title != "") {
			GameObject titleObj = new GameObject("Series title");

			//Add as child of container
			titleObj.transform.parent = container.transform;

			//Set position of plane relative to parent
			titleObj.transform.localPosition = new Vector3 (
				//X : X-Position of plane relative to parent
				((bar_size / 2) - (x_width / 2) - (bar_size / 4)),
				//Y : Y-Position of plane relative to parent
				base_height/2,
				//Z : Z-Position of plane relative to parent
				0f
			);

			//Set scale relative to parent
			titleObj.transform.localScale = new Vector3 (
				(bar_size) / 2f, 
				1f,
				(bar_size)
			);
				
			titleObj.SetActive(true);

			//Add text to label
			GameObject fontObj = new GameObject();
			fontObj.transform.parent = titleObj.transform;
			fontObj.name = titleObj.name + " text";

			TextMesh tm = fontObj.AddComponent<TextMesh>();
			fontObj.GetComponent<Renderer>().material.color = Color.black;
			tm.font = (Resources.Load("Courier") as Font);
			tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
			tm.text = series_title;
			tm.fontSize = 200;
			tm.characterSize = 1;

			//Set scale relative to parent
			fontObj.transform.localScale = new Vector3 (
				caption_label_scale,
				caption_label_scale,
				1f
			);

			fontObj.transform.localPosition = new Vector3 (
				0f,
				0.01f,
				0f
			);

			tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

			//Rotate text to point upwards
			fontObj.transform.Rotate (90, 90, 0);
		}

		//Series
		int s_length = 0;
		if (series == null) {
			s_length = 0;
		} else {
			if (series_count < series.Length) {
				s_length = series_count;
			} else {
				s_length = series.Length;
			}
		}

		for (int s = 0; s < s_length; s++) {
			GameObject label = new GameObject("Series " + s.ToString());

			//Add as child of conainer
			label.transform.parent = container.transform;

			if (series_title != null && series_title != "") {
				//Set position of plane relative to parent
				label.transform.localPosition = new Vector3 (
					//X : X-Position of plane relative to parent
					((bar_size / 2) - (x_width / 2) + (bar_size/4)),
					//Y : Y-Position of plane relative to parent
					base_height/2,
					//Z : Z-Position of plane relative to parent
					((s * (bar_size + spacing) + bar_size) - (z_width / 2) + (bar_size / 2))
				);

				//Set scale relative to parent
				label.transform.localScale = new Vector3 (
					(bar_size) / 2f, 
					1f,
					(bar_size)
				);
			} else {
				//Set position of plane relative to parent
				label.transform.localPosition = new Vector3 (
					//X : X-Position of plane relative to parent
					((bar_size / 2) - (x_width / 2)),
					//Y : Y-Position of plane relative to parent
					base_height/2,
					//Z : Z-Position of plane relative to parent
					((s * (bar_size + spacing) + bar_size) - (z_width / 2) + (bar_size / 2))
				);

				//Set scale relative to parent
				label.transform.localScale = new Vector3 (
					(bar_size), 
					1f,
					(bar_size)
				);
			}
				
			label.SetActive(true);


			//Add text to label
			GameObject fontObj = new GameObject();
			fontObj.transform.parent = label.transform;
			fontObj.name = label.name + " text";

			TextMesh tm = fontObj.AddComponent<TextMesh>();
			fontObj.GetComponent<Renderer>().material.color = Color.black;
			tm.font = (Resources.Load("Courier") as Font);
			tm.GetComponent<Renderer> ().material.shader = Shader.Find("zTextShader");
			tm.text = series[s];
			tm.fontSize = 200;
			tm.characterSize = 1;

			//Set scale relative to parent
			fontObj.transform.localScale = new Vector3 (
				caption_label_scale,
				caption_label_scale,
				1f
			);

			fontObj.transform.localPosition = new Vector3 (
				0f,
				0.01f,
				0f
			);

			tm.GetComponent<TextMesh>().anchor = TextAnchor.MiddleCenter;

			//Rotate text to point upwards
			fontObj.transform.Rotate (90, 90, 0);
		}

		/*
		//Draw title
		if (title != null && title != "") {
			GameObject titleObj = new GameObject("Title");

			//Add as child of container
			titleObj.transform.parent = container.transform;

			//Set position of plane relative to parent
			titleObj.transform.localPosition = new Vector3 (
				//X : X-Position of plane relative to parent
				0f,
				//Y : Y-Position of plane relative to parent
				0f,
				//Z : Z-Position of plane relative to parent
				(-0.1f - (z_width / 2))
			);

			//Set scale relative to parent
			titleObj.transform.localScale = new Vector3 (
				(x_width), 
				1f,
				0.1f
			);

			titleObj.transform.Rotate (90, 0, 0);

			titleObj.GetComponent<Renderer> ().material.color = Color.white;
			titleObj.SetActive (true);


			//Add text to label
			GameObject titleTextObj = new GameObject ();
			titleTextObj.transform.parent = titleObj.transform;
			titleTextObj.name = titleObj.name + " text";

			TextMesh tm_title = titleTextObj.AddComponent<TextMesh> ();
			titleTextObj.GetComponent<Renderer> ().material.color = Color.black;
			tm_title.font = (Resources.Load ("Courier") as Font);
			tm_title.GetComponent<Renderer> ().material.shader = Shader.Find ("zTextShader");
			tm_title.text = title;
			tm_title.fontSize = 200;
			tm_title.characterSize = 1;

			float titleScale = 0f;

			if (((x_width / ((float)CalculateLengthOfMessage (tm_title)))) < ((base_height / ((float)CalculateHeightOfMessage (tm_title))) * 4f)) {
				titleScale = (x_width / ((float)CalculateLengthOfMessage (tm_title)));
			} else {
				titleScale = (base_height / ((float)CalculateHeightOfMessage (tm_title))) * 4f;
			}

			//Set scale relative to parent
			titleTextObj.transform.localScale = new Vector3 (
				titleScale,
				titleScale,
				1f
			);

			titleTextObj.transform.localPosition = new Vector3 (
				0f,
				0f,
				0f
			);

			tm_title.GetComponent<TextMesh> ().anchor = TextAnchor.MiddleCenter;
		}*/

		return container;

	}
	#endregion

	#region Tools
	//Returns size of largest textmesh length (horizontal or vertical)
	// - To generate scale of text
	float CalculateTextScale(TextMesh tm)
	{
		if (CalculateLengthOfMessage(tm) >= CalculateHeightOfMessage(tm)) {
			return ((float)CalculateLengthOfMessage(tm));
		} else {
			return ((float)CalculateHeightOfMessage(tm));
		}
	}

	//Calculates horizontal length of text
	int CalculateLengthOfMessage(TextMesh tm)
	{
		int totalLength = 0;

		Font myFont = tm.font; 
		CharacterInfo characterInfo = new CharacterInfo();

		char[] arr = tm.text.ToCharArray();

		int tempLength = 0;
		foreach(char c in arr)
		{
			if (c.CompareTo ('\n') == 0) {
				if (tempLength > totalLength)
					totalLength = tempLength;
				tempLength = 0;
			} else {
				myFont.GetCharacterInfo (c, out characterInfo, tm.fontSize);  

				tempLength += characterInfo.advance;
			}
		}
		if (tempLength > totalLength)
			totalLength = tempLength;

		return totalLength;
	}

	//Calculates vertical value of text
	int CalculateHeightOfMessage(TextMesh tm)
	{
		int totalHeight = 0;

		Font myFont = tm.font; 
		CharacterInfo characterInfo = new CharacterInfo();

		char[] arr = tm.text.ToCharArray();
		bool first = true;
		bool addHeight = true;

		foreach(char c in arr)
		{
			if (first) {
				if (c.CompareTo ('\n') != 0) {
					myFont.GetCharacterInfo (c, out characterInfo, tm.fontSize);  
					totalHeight += characterInfo.glyphHeight;
				}
				first = false;
			} else {
				if (c.CompareTo ('\n') == 0) {
					addHeight = true;
				} else {
					if (addHeight) {
						myFont.GetCharacterInfo (c, out characterInfo, tm.fontSize);  
						totalHeight += characterInfo.glyphHeight;
						addHeight = false;
					}
				}
			}
		}

		return totalHeight;
	}

	//If all default colors have not been assigned return next in list else generate random colour
	Color32 getColor(){
		Color32 color;

		if (current_colour < colors.Length) {
			color = colors [current_colour];
			current_colour++;
		}
		else {
			color = new Color32((byte)Random.Range(0,256),(byte)Random.Range(0,256),(byte)Random.Range(0,256),(byte)255);
		}

		return color;
	}

	//Calculate graph intervals
	float generateTickInterval(float range, int tickCount){

		if (range == 0.0f)
			return 1f;
		float unroundedTickSize = range/(tickCount - 1);
		float x = Mathf.Ceil (Mathf.Log10 (unroundedTickSize) - 1);
		float pow10 = Mathf.Pow (10, x);
		float roundedTickRange = Mathf.Ceil (unroundedTickSize / pow10) * pow10;
		return roundedTickRange;
	}
	#endregion

}