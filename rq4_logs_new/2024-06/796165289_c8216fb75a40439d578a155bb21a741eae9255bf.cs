using UnityEngine;
using UnityEngine.UI;
using TMPro;
using System.Collections.Generic;
using UnityEngine.SceneManagement;

public class PhotoelectricEffect : MonoBehaviour
{
    public SpriteRenderer lightSprite;
    public Slider wavelengthSlider;
    public Slider intensitySlider;
    public Slider voltageSlider;
    public TMP_Text currentText;
    public TMP_Text wavelengthText;
    public TMP_Text intensityText;
    public TMP_Text voltageText;
    public TMP_Text textA; // New TMP_Text to be exchanged
    public TMP_Text textB; // New TMP_Text to be exchanged
    public ParticleSystem electronEmitter;
    public Vector3 newPosition; // Manually enter the new position in the Inspector
    public GameObject objectToMove; // Specify which GameObject to move
    private float workFunction = 2.75f; // Work function of Sodium in eV
    private const float planckConstant = 4.135667696e-15f; // Planck's constant in eV·s
    private const float speedOfLight = 299792458f; // Speed of light in m/s
    private const float electronCharge = 1.602176634e-19f; // Charge of an electron in coulombs
    private Vector3 originalPosition; // Store the original position of the GameObject
    private string originalTextA; // Declare at class level to store original textA value
    private string originalTextB; // Declare at class level to store original textB value
    public TMP_Dropdown metalDropdown;
    public TMP_Text workFunctionText; // Declare the TMP public variable
    private Color currentColor;
    private struct MetalProperties
    {
        public float workFunction; // in eV
        public float thresholdWavelength; // in nm, for example

        public MetalProperties(float workFunction, float thresholdWavelength)
        {
            this.workFunction = workFunction;
            this.thresholdWavelength = thresholdWavelength;
        }
    }

    private Dictionary<string, MetalProperties> metals = new Dictionary<string, MetalProperties>
    {
        {"Sodium", new MetalProperties(2.75f, 451f)},
        {"Potassium", new MetalProperties(2.30f, 539f)},
        {"Cesium", new MetalProperties(2.14f, 579f)},
        {"Rubidium", new MetalProperties(2.26f, 549f)},
        {"calcium", new MetalProperties(2.87f, 432)}
    };
    void Awake()
    {
        Screen.orientation = ScreenOrientation.LandscapeLeft;
        // Check if the back button was pressed
        if (Input.GetKeyDown(KeyCode.Escape))
        {
            // Load the "Main Alpha Functionality Pages" scene
            SceneManager.LoadScene("Main Alpha Functionality Pages");
        }
    }

    void Start()
    {
        wavelengthSlider.onValueChanged.AddListener(OnWavelengthChanged);
        intensitySlider.onValueChanged.AddListener(OnIntensityChanged);
        voltageSlider.onValueChanged.AddListener(OnVoltageChanged);
        UpdateLightColor();
        UpdateCurrent();
        UpdateElectronEmission();
        // Store the original position of the GameObject
        if (objectToMove != null)
        {
            originalPosition = objectToMove.transform.position;
        }
        // Assuming Start() is already present, add these lines to store original texts
            originalTextA = textA.text;
            originalTextB = textB.text;
            metalDropdown.onValueChanged.AddListener(delegate {
            MetalChanged(metalDropdown.value);
        });
    }
    void MetalChanged(int index)
    {
        string selectedMetal = metalDropdown.options[index].text;
        if (metals.ContainsKey(selectedMetal))
        {
            MetalProperties properties = metals[selectedMetal];
            workFunction = properties.workFunction;
            // Update other properties like threshold wavelength as needed

            // Update the simulation to reflect the new metal properties
            UpdateCurrent();
            UpdateElectronEmission();
            // Any other updates that depend on the metal's properties
        }
    }
    void OnWavelengthChanged(float value)
    {
        wavelengthText.text = $"{value} nm";
        UpdateLightColor();
        UpdateCurrent();
        UpdateElectronEmission();
    }

    void OnIntensityChanged(float value)
    {
        intensityText.text = $"{value}%";
        UpdateLightIntensity();
        UpdateElectronEmission();
    }

    void OnVoltageChanged(float value)
    {
        voltageText.text = $"{value} V";
        if (value < 0)
        {
            // Set textA to '-' and textB to '+'
            textA.text = "-";
            textB.text = "+";

            // Change the specified GameObject's position to the new position
            if (objectToMove != null)
            {
                objectToMove.transform.position = newPosition;
            }
        }
        else
        {
            // Restore original values of textA and textB
            textA.text = originalTextA;
            textB.text = originalTextB;

            // If voltage is above zero, move the GameObject back to its original position
            if (objectToMove != null)
            {
                objectToMove.transform.position = originalPosition;
            }
        }
        UpdateCurrent();
        UpdateElectronEmission();
    }

    void UpdateLightColor()
    {
        float wavelength = wavelengthSlider.value;
        currentColor = WavelengthToColor(wavelength);
        lightSprite.color = new Color(currentColor.r, currentColor.g, currentColor.b, lightSprite.color.a); // Preserve current alpha
    }

    void UpdateLightIntensity()
    {
        float intensity = intensitySlider.value / 100f;
        lightSprite.color = new Color(currentColor.r, currentColor.g, currentColor.b, intensity);
    }

    void UpdateCurrent()
    {
        float wavelength = wavelengthSlider.value * 1e-9f; // Convert nm to meters
        Debug.Log($"Wavelength: {wavelength} m");
        float voltage = voltageSlider.value;
        Debug.Log($"Voltage: {voltage} V");
        float frequency = speedOfLight / wavelength;
        float photonEnergy = planckConstant * frequency; // Converted to eV
        Debug.Log($"Photon Energy: {photonEnergy} eV");
        float kineticEnergy = photonEnergy - workFunction;
        Debug.Log($"Kinetic Energy: {kineticEnergy} eV");

        float current = 0f;
        if (kineticEnergy > 0)
        {
            float stoppingVoltage = kineticEnergy;
            Debug.Log($"Stopping Voltage: {stoppingVoltage} V");
            if (voltage >= stoppingVoltage)
            {
                current = (intensitySlider.value / 100f) * 100f; // Example scaling
                Debug.Log($"Current: {current} nA");
            }
        }
        currentText.text = $"Current: {current:F2} nA";
    }

    void UpdateElectronEmission()
    {
        var emission = electronEmitter.emission;
        var main = electronEmitter.main;
        float wavelength = wavelengthSlider.value;
        float intensity = intensitySlider.value;

        // Retrieve the selected metal's work function
        float workFunction = metals[metalDropdown.options[metalDropdown.value].text].workFunction;

        // Update the TMP with the work function value
        workFunctionText.text = $"{workFunction} eV";
        
        // Check if the wavelength exceeds the threshold wavelength and stop emission if true
        if (wavelength > metals[metalDropdown.options[metalDropdown.value].text].thresholdWavelength)
        {
            emission.rateOverTime = 0;
            return;
        }
    
        emission.rateOverTime = intensity;
    
        float frequency = speedOfLight / (wavelength * 1e-9f);
        float photonEnergy = planckConstant * frequency;
        float kineticEnergy = photonEnergy - workFunction;
        float stoppingPotential = kineticEnergy / electronCharge;
    
        if (voltageSlider.value > stoppingPotential)
        {
            main.startSpeed = -Mathf.Abs(main.startSpeed.constant); // Reverse particle direction
        }
        else
        {
            main.startSpeed = Mathf.Abs(main.startSpeed.constant); // Ensure correct direction
        }
    }

    Color WavelengthToColor(float wavelength)
    {
        float R, G, B;
        if (wavelength >= 380 && wavelength < 440)
        {
            R = -(wavelength - 440) / (440 - 380);
            G = 0.0f;
            B = 1.0f;
        }
        else if (wavelength >= 440 && wavelength < 490)
        {
            R = 0.0f;
            G = (wavelength - 440) / (490 - 440);
            B = 1.0f;
        }
        else if (wavelength >= 490 && wavelength < 510)
        {
            R = 0.0f;
            G = 1.0f;
            B = -(wavelength - 510) / (510 - 490);
        }
        else if (wavelength >= 510 && wavelength < 580)
        {
            R = (wavelength - 510) / (580 - 510);
            G = 1.0f;
            B = 0.0f;
        }
        else if (wavelength >= 580 && wavelength < 645)
        {
            R = 1.0f;
            G = -(wavelength - 645) / (645 - 580);
            B = 0.0f;
        }
        else if (wavelength >= 645 && wavelength <= 700)
        {
            R = 1.0f;
            G = 0.0f;
            B = 0.0f;
        }
        else
        {
            R = 0.0f;
            G = 0.0f;
            B = 0.0f;
        }

        R = Mathf.Clamp(R, 0, 1);
        G = Mathf.Clamp(G, 0, 1);
        B = Mathf.Clamp(B, 0, 1);

        return new Color(R, G, B, lightSprite.color.a);
    }
}