using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using System;
using DG.Tweening;
using UnityEngine.UI;
using UnityGameFramework.Runtime;

public class CardItem:MonoBehaviour, IBeginDragHandler, IDragHandler, IEndDragHandler
    , IComparable<CardItem>
    , IPointerUpHandler, IPointerEnterHandler, IPointerClickHandler {
    public GameObject front;
    public GameObject back;
    public GameObject landlordSign;
    public int CardID {
        get;
        private set;
    }
    public bool isSelected {
        get {
            return Mathf.Abs( transform.localPosition.y - ( OriginY + Raise ) ) < 2;
        }
    }

    public const float OriginY = -273;
    private const float Raise = 22;
    private const float OUTCARD_TWEEN_POSY=-190;
    private static bool canMultiSelect;
    private static List<CardItem> selectedList = new List<CardItem>( );
    private static List<CardItem> preSelectedList = new List<CardItem>( );

    private CanvasGroup _group;
    private CanvasGroup Group {
        get {
            if( _group == null )
                _group = GetComponent<CanvasGroup>( );
            return _group;
        }
    }

    private Image[] _imgs;
    private Image[] Imgs {
        get {
            if( _imgs == null )
                _imgs = GetComponentsInChildren<Image>( true );
            return _imgs;
        }
    }

    public void SetFrontActive( bool active ) {
        front.SetActive( active );
        back.SetActive( !active );
    }

    public void DisableInput( ) {
        for( int i = 0; i < Imgs.Length; i++ ) {
            Imgs[i].raycastTarget = false;
        }
    }

    public void SetGrayState( bool state ) {
        for( int i = 0; i < Imgs.Length; i++ ) {
            Imgs[i].color = state ? new Color( 200 / 255f, 200 / 255f, 200 / 255f ) : Color.white;
        }
    }

    public void SetLandlordState( bool state ) {
        landlordSign.SetActive( state );
    }

    public void Init( int id ) {
        this.CardID = id;
        SetLandlordState( false );
    }

    public void Deselect( ) {
        if( this == null )
            return;
        SetGrayState( false );
        transform.DOLocalMove( new Vector3( transform.localPosition.x, OriginY, 0 ), 0.05f );
    }

    public void Select( ) {
        Vector3 tar = new Vector3( transform.localPosition.x, OriginY + Raise );
        transform.DOLocalMove( tar, 0.1f, true );
    }

    public void OnBeginDrag( PointerEventData eventData ) {
    }

    public void OnDrag( PointerEventData eventData ) {
        canMultiSelect = true;
        MultiSelect( this );
    }


    public void OnEndDrag( PointerEventData eventData ) {
        canMultiSelect = false;
        SetMuitiSelectState( );
        Log.Debug( "---------------------enddrag" );
    }

    public void SetMuitiSelectState( ) {
        for( int i = 0; i < selectedList.Count; i++ ) {
            var item = selectedList[i];
            if( item == null )
                continue;
            item.SetGrayState( false );
            if( isSelected )
                item.Deselect( );
            else
                item.Select( );
        }
        preSelectedList.Clear( );
        preSelectedList.AddRange( selectedList );
        selectedList.Clear( );
    }

    public int CompareTo( CardItem other ) {
        return other.CardID - this.CardID;
    }


    public void OnPointerUp( PointerEventData eventData ) {
    }



    public void OnPointerClick( PointerEventData eventData ) {
        if( !isSelected ) {
            Select( );
        }
        else {
            Deselect( );
        }
    }

    public void OnPointerEnter( PointerEventData eventData ) {
        MultiSelect( this );
    }

    private void MultiSelect( CardItem item ) {
        if( selectedList.Contains( item ) ) {
            return;
        }
        if( !canMultiSelect )
            return;
        selectedList.Add( this );

        for( int i = 0; i < selectedList.Count; i++ ) {
            if( selectedList[i] == null )
                continue;
            selectedList[i].SetGrayState( true );
        }
    }

    private const float scaleTime= 0.5f;
    private const float positionTime = 0.5f;
//    public void DoTweenPartner( BasePlayer owner ) {
//        SetFrontActive( false );
//        transform.DOScale( 0.24f, scaleTime );
//        transform.DOMove( owner.cardCountLabel.transform.parent.position, positionTime );
//    }
//
//    public void DoTweenMainPlayer( MainPlayer owner, int index ) {
//        SetFrontActive( false );
//        transform.DOScale( Def.HAND_CARD_SCALE, scaleTime );
//        var pos = owner.GetCardPos( index, Def.NORMAL_CARD );
//        DOTween.Sequence( )
//            .Append( transform.DOLocalMove( pos, positionTime ) )
//            .Append( transform.DOLocalMove( pos, 0.05f ) )
//            .OnComplete( ( ) =>
//            {
//                SetFrontActive( true );
//            } );
//    }

    private static Action onMPOutCardTweenFinished;
    public const float OUTCARD_TWEEN_TIME=0.2f;
//    public void OutCardTweenMainPlayer( MainPlayer owner, int index, int count, Action callback ) {
//        onMPOutCardTweenFinished = callback;
//        transform.DOLocalMove( owner.GetOutCardPos( index, count ), OUTCARD_TWEEN_TIME ).SetEase( Ease.InSine );
//        transform.DOScale( Def.OUT_CARD_SCALE_MP, OUTCARD_TWEEN_TIME ).OnComplete( ( ) =>
//        {
//            if( onMPOutCardTweenFinished != null ) {
//                onMPOutCardTweenFinished( );
//                onMPOutCardTweenFinished = null;
//            }
//        } );
//        //transform.DOLocalMoveY( OUTCARD_TWEEN_POSY, first );
//        //Group.DOFade( 0f, first ).OnComplete( ( ) =>
//        //{
//        //    transform.DOLocalMove( owner.GetOutCardPos( index, count ), second ).SetEase(Ease.InSine);
//        //    DOTween.Sequence( )
//        //       .Append( transform.DOScale( Def.OUT_CARD_SCALE * 0.9f, second ) )
//        //       .Append( transform.DOScale( Def.OUT_CARD_SCALE, 0.02f ) );
//        //    Group.alpha = 1;
//        //} );        
//    }

//    public void HandCardSmoothMove(MainPlayer owner,int index, int count) {
//        var pos = owner.GetCardPos( index, count );
//        transform.DOLocalMove( pos, 0.1f );
//    }

    public void OutCardTweenPartner( ) {

    }
}