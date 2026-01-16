using System;
using DG.Tweening;
using UnityEngine;
using UnityEngine.UI;

public class TweenUtils : MonoBehaviour
{
    private const float SCALE_OUT_VALUE = 0.75f;
    private const float DEFAULT_DURATION = 0.3f;

    /// <summary>
    /// Fade in main canvas and scale in rect content
    /// </summary>
    /// <param name="canvas">Background canvas (usually with blur material), use for fade in</param>
    /// <param name="content">Content rect of view, use for scale in</param>
    /// <param name="callback">callback event after FadeIn() tween complete</param>
    public static void Show(CanvasGroup canvas, Transform content, Action callback = null)
    {
        DOTween.Complete(canvas);
        DOTween.Complete(content);

        FadeIn(canvas);
        ScaleIn(content, callback);
    }

    /// <summary>
    /// Fade out main canvas and scale out rect content
    /// </summary>
    /// <param name="canvas">Background canvas (usually with blur material), use for fade out</param>
    /// <param name="content">Content rect of view, use for scale out</param>
    /// <param name="callback">callback event after FadeOut() tween complete</param>
    public static void Hide(CanvasGroup canvas, Transform content, Action callback = null, bool forceDelete = true)
    {
        DOTween.Complete(canvas);
        DOTween.Complete(content);

        FadeOut(canvas, null, forceDelete);
        ScaleOut(content, callback);
    }

    /// <summary>
    /// Scale out object 2D
    /// </summary>
    /// <param name="content">Content rect object, use for scale out</param>
    /// <param name="callback">callback event after ScaleOut tween complete</param>
    public static void ScaleOut(Transform content, Action callback = null, float duration = DEFAULT_DURATION)
    {
        content.transform.localScale = Vector3.one;
        content.DOScale(Vector3.one * SCALE_OUT_VALUE, duration).SetEase(Ease.OutExpo).OnComplete(() => { callback?.Invoke(); });
    }

    /// <summary>
    /// Scale in object 2D
    /// </summary>
    /// <param name="content">Content rect object, use for scale in</param>
    /// <param name="callback">callback event after ScaleIn tween complete</param>
    public static void ScaleIn(Transform content, Action callback = null, float duration = DEFAULT_DURATION)
    {
        content.transform.localScale = Vector3.one * SCALE_OUT_VALUE;
        content.DOScale(Vector3.one, duration).SetEase(Ease.OutExpo).OnComplete(() => { callback?.Invoke(); });
    }

    /// <summary>
    /// Fade in canvasGroup
    /// </summary>
    /// <param name="canvas">CanvasGroup rect object, use for fade in</param>
    /// <param name="callback">callback event after FadeIn tween complete</param>
    public static void FadeIn(CanvasGroup canvas, Action callback = null, float delay = 0f, float duration = DEFAULT_DURATION)
    {
        canvas?.DOFade(1f, duration).SetEase(Ease.OutExpo).SetDelay(delay)
            .OnStart(() => {
                canvas.alpha = 0f;
                canvas.gameObject.SetActive(true);
                canvas.blocksRaycasts = false;
            })
            .OnComplete(() => {
                canvas.blocksRaycasts = true;
                callback?.Invoke();
            });
    }

    /// <summary>
    /// Fade out canvasGroup
    /// </summary>
    /// <param name="canvas">CanvasGroup rect object, use for fade out</param>
    /// <param name="callback">callback event after FadeOut tween complete</param>
    public static void FadeOut(CanvasGroup canvas, Action callback = null, bool forceDelete = true, float delay = 0f, float duration = DEFAULT_DURATION)
    {
        canvas?.DOFade(0f, duration).SetEase(Ease.OutExpo).SetDelay(delay)
            .OnStart(() => {
                canvas.alpha = 1f;
                canvas.gameObject.SetActive(true);
                canvas.blocksRaycasts = false;
            })
            .OnComplete(() =>
            {
                callback?.Invoke();
                canvas.gameObject.SetActive(false);
                if (forceDelete)
                {
                    Destroy(canvas.gameObject);
                }
            });
    }

    /// <summary>
    /// Shaking position object's Transform
    /// </summary>
    /// <param name="tf">object's Transform, use for shaking position</param>
    public static void DOShakeError(Transform tf)
    {
        DOTween.Complete(tf);
        tf.DOShakePosition(0.6f, new Vector3(12f, 0, 0), 30);
    }

    /// <summary>
    /// Scroll horizontally to a target position
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="target">Target Transform to scroll to</param>
    /// <param name="callback">Callback event after scroll complete</param>
    /// <param name="duration">Duration of the scroll animation</param>
    /// <param name="offsetX">Offset for the target position</param>
    /// <param name="skipAnimation">Whether to skip the animation</param>
    public static void ScrollHorizontalTo(ScrollRect scrollRect, Transform target, Action callback = null, float duration = DEFAULT_DURATION, float offsetX = 0f, bool skipAnimation = true)
    {
        Canvas.ForceUpdateCanvases();

        RectTransform contentPanel = scrollRect.content as RectTransform;

        float posX = ((Vector2)scrollRect.transform.InverseTransformPoint(contentPanel.position)).x
            - ((Vector2)scrollRect.transform.InverseTransformPoint(target.position)).x
            - offsetX;

        if (skipAnimation)
        {
            scrollRect.content.anchoredPosition = new Vector2(posX, scrollRect.content.anchoredPosition.y);
            callback?.Invoke();
        }
        else
        {
            ScrollRect.MovementType scrMovementType = scrollRect.movementType;
            scrollRect.movementType = ScrollRect.MovementType.Clamped;

            DOTween.Complete(contentPanel);
            contentPanel.DOAnchorPosX(
                posX,
                duration
            ).OnComplete(() => {
                DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
                {
                    scrollRect.movementType = scrMovementType;
                    callback?.Invoke();
                });
            });
        }
    }

    /// <summary>
    /// Scroll vertically to a target position
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="target">Target RectTransform to scroll to</param>
    /// <param name="callback">Callback event after scroll complete</param>
    /// <param name="duration">Duration of the scroll animation</param>
    /// <param name="offsetY">Offset for the target position</param>
    /// <param name="skipAnimation">Whether to skip the animation</param>
    public static void ScrollVerticalTo(ScrollRect scrollRect, RectTransform target, Action callback = null, float duration = DEFAULT_DURATION, float offsetY = 48f, bool skipAnimation = true)
    {
        Canvas.ForceUpdateCanvases();

        RectTransform contentPanel = scrollRect.content as RectTransform;

        float posY = ((Vector2)scrollRect.transform.InverseTransformPoint(contentPanel.position)).y
            - ((Vector2)scrollRect.transform.InverseTransformPoint(target.position)).y
            - offsetY;

        if (skipAnimation)
        {
            scrollRect.content.anchoredPosition = new Vector2(scrollRect.content.anchoredPosition.x, posY);
            callback?.Invoke();
        }
        else
        {
            ScrollRect.MovementType scrMovementType = scrollRect.movementType;
            scrollRect.movementType = ScrollRect.MovementType.Clamped;

            DOTween.Complete(contentPanel);
            contentPanel.DOAnchorPosY(
                posY,
                duration
            ).OnComplete(() =>
            {
                DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
                {
                    scrollRect.movementType = scrMovementType;
                    callback?.Invoke();
                });
            });
        }
    }

    /// <summary>
    /// Scroll to a target position
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="target">Target RectTransform to scroll to</param>
    /// <param name="callback">Callback event after scroll complete</param>
    /// <param name="forceUpdate">Whether to force update</param>
    /// <param name="delay">Delay before scrolling</param>
    public static void ScrollTo(ScrollRect scrollRect, RectTransform target, Action callback = null, bool forceUpdate = false, float delay = 0f)
    {
        Canvas.ForceUpdateCanvases();

        ScrollRect.MovementType scrMovementType = scrollRect.movementType;
        scrollRect.movementType = ScrollRect.MovementType.Clamped;

        DOTween.Complete(scrollRect.content);
        DOVirtual.DelayedCall(delay, () =>
        {
            if (forceUpdate)
            {
                scrollRect.content.anchoredPosition = (Vector2)scrollRect.transform.InverseTransformPoint(scrollRect.content.position)
                    - (Vector2)scrollRect.transform.InverseTransformPoint(target.position);
                scrollRect.movementType = scrMovementType;
                callback?.Invoke();
            }
            else
            {
                scrollRect.content.DOAnchorPos(
                    (Vector2)scrollRect.transform.InverseTransformPoint(scrollRect.content.position)
                    - (Vector2)scrollRect.transform.InverseTransformPoint(target.position),
                    DEFAULT_DURATION
                ).OnComplete(() =>
                {
                    DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
                    {
                        scrollRect.movementType = scrMovementType;
                        callback?.Invoke();
                    });
                });
            }
        });
    }

    /// <summary>
    /// Scroll to the top of the content
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="callback">Callback event after scroll complete</param>
    public static void ScrollToTop(ScrollRect scrollRect, Action callback = null)
    {
        Canvas.ForceUpdateCanvases();

        ScrollRect.MovementType scrMovementType = scrollRect.movementType;
        scrollRect.movementType = ScrollRect.MovementType.Clamped;

        scrollRect.content.DOAnchorPosY(0f, 0.1f).OnComplete(() =>
        {
            DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
            {
                scrollRect.movementType = scrMovementType;
                callback?.Invoke();
            });
        });
    }

    /// <summary>
    /// Scroll to the bottom of the content
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="callback">Callback event after scroll complete</param>
    public static void ScrollToBottom(ScrollRect scrollRect, Action callback = null)
    {
        Canvas.ForceUpdateCanvases();

        ScrollRect.MovementType scrMovementType = scrollRect.movementType;
        scrollRect.movementType = ScrollRect.MovementType.Clamped;

        scrollRect.content.DOAnchorPosY(1f, 0.1f).OnComplete(() =>
        {
            DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
            {
                scrollRect.movementType = scrMovementType;
                callback?.Invoke();
            });
        });
    }

    /// <summary>
    /// Scroll to the left of the content
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="callback">Callback event after scroll complete</param>
    public static void ScrollToLeft(ScrollRect scrollRect, Action callback = null)
    {
        Canvas.ForceUpdateCanvases();

        ScrollRect.MovementType scrMovementType = scrollRect.movementType;
        scrollRect.movementType = ScrollRect.MovementType.Clamped;

        scrollRect.content.DOAnchorPosX(0.1f, 0f).OnComplete(() =>
        {
            DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
            {
                scrollRect.movementType = scrMovementType;
                callback?.Invoke();
            });
        });
    }

    /// <summary>
    /// Scroll to the right of the content
    /// </summary>
    /// <param name="scrollRect">ScrollRect to scroll</param>
    /// <param name="callback">Callback event after scroll complete</param>
    public static void ScrollToRight(ScrollRect scrollRect, Action callback = null)
    {
        Canvas.ForceUpdateCanvases();

        ScrollRect.MovementType scrMovementType = scrollRect.movementType;
        scrollRect.movementType = ScrollRect.MovementType.Clamped;

        scrollRect.content.DOAnchorPosX(0.1f, 1f).OnComplete(() =>
        {
            DOVirtual.DelayedCall(DEFAULT_DURATION, () =>
            {
                scrollRect.movementType = scrMovementType;
                callback?.Invoke();
            });
        });
    }

    /// <summary>
    /// Add fading in effect after instantiating object
    /// </summary>
    /// <param name="goItem">GameObject to instantiate</param>
    /// <param name="transformIndex">Index of the transform to set</param>
    /// <param name="callback">Callback event after instantiate complete</param>
    /// <param name="duration">Duration of the fading effect</param>
    public static void CreateItem(GameObject goItem, int transformIndex, Action callback = null, float duration = 1f)
    {
        if (goItem == null)
        {
            callback?.Invoke();
            return;
        }

        CanvasGroup cvgItem = goItem.GetOrAddComponent<CanvasGroup>();
        goItem.transform.SetSiblingIndex(transformIndex);
        ScaleIn(goItem.transform, duration: duration);
        FadeIn(cvgItem, callback: callback, duration: duration);
    }

    /// <summary>
    /// Add fading out effect before destroying object
    /// </summary>
    /// <param name="goItem">GameObject to destroy</param>
    /// <param name="callback">Callback event after destroy complete</param>
    /// <param name="duration">Duration of the fading effect</param>
    public static void DestroyItem(GameObject goItem, Action callback = null, float duration = DEFAULT_DURATION)
    {
        if (goItem == null)
        {
            callback?.Invoke();
            return;
        }

        CanvasGroup cvgItem = goItem.GetOrAddComponent<CanvasGroup>();
        ScaleOut(goItem.transform, duration: duration);
        FadeOut(cvgItem, callback: callback, duration: duration);
    }

    /// <summary>
    /// Slide and destroy the object
    /// </summary>
    /// <param name="goItem">GameObject to slide and destroy</param>
    /// <param name="slideToRight">Slide direction</param>
    /// <param name="callback">Callback event after destroy complete</param>
    public static void SlidingAndDestroy(GameObject goItem, bool slideToRight, Action callback)
    {
        if (goItem == null)
        {
            callback?.Invoke();
            return;
        }

        CanvasGroup cvgItem = goItem.GetOrAddComponent<CanvasGroup>();
        SlidingX(goItem: goItem, slideToRight: slideToRight);
        FadeOut(canvas: cvgItem, callback: callback);
    }

    /// <summary>
    /// Slide object horizontally
    /// </summary>
    /// <param name="goItem">GameObject to slide</param>
    /// <param name="slideToRight">Slide direction</param>
    /// <param name="callback">Callback event after slide complete</param>
    public static void SlidingX(GameObject goItem, bool slideToRight = true, Action callback = null)
    {
        goItem.transform.DOLocalMoveX(goItem.transform.localPosition.x + (slideToRight ? 300f : -300f), DEFAULT_DURATION)
            .OnComplete(() => callback?.Invoke());
    }
}