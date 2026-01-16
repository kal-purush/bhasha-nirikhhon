using DG.Tweening;
using Sim;
using UnityEngine;

public class Roof : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private float fadeDuration = .3f;
    
    private new Renderer renderer;

    private Material customMaterial;

    private Color originColor;

    private void Awake() {
        this.renderer = GetComponent<Renderer>();
        this.customMaterial = new Material(this.renderer.sharedMaterial);
        this.originColor = this.customMaterial.color;
        this.renderer.sharedMaterial = this.customMaterial;
    }

    private void OnTriggerEnter(Collider other) {
        if (other.CompareTag("Player") && other.gameObject == PlayerController.Local.gameObject) {
            this.Hide();
        }
    }

    private void OnTriggerExit(Collider other) {
        if (other.CompareTag("Player") && other.gameObject == PlayerController.Local.gameObject) {
            this.Show();
        }
    }

    private void Show() {
        this.customMaterial.DOComplete();
        this.customMaterial.DOColor(this.originColor, this.fadeDuration);
    }

    private void Hide() {
        this.customMaterial.DOComplete();
        this.customMaterial.DOColor(new Color(0, 0, 0, 0), this.fadeDuration);
    }
}