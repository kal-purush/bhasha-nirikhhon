using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(SpriteRenderer))]
public class AnimatedSprites : MonoBehaviour
{
    public SpriteRenderer spriteRenderer { get; private set; }
    public Sprite[] sprites;
    public float animationTime = 0.25f;
    public int animationIndex { get; private set; }
    public bool loop = true;
    private void Awake()
    {
        this.spriteRenderer = GetComponent<SpriteRenderer>();
    }
    private void Start()
    {
        InvokeRepeating(nameof(Advance), this.animationTime, this.animationTime);
    }
    private void Advance()
    {
        if (!this.spriteRenderer.enabled)
        {
            return;
        }
        this.animationIndex++;
        if (this.animationIndex >= this.sprites.Length && loop)
        {
            this.animationIndex = 0;
        }
        if (this.animationIndex >= 0 && this.animationIndex < this.sprites.Length)
        {
            this.spriteRenderer.sprite = this.sprites[this.animationIndex];
        }
    }
    public void Restart()
    {
        this.animationIndex = -1;
        Advance();
    }
}