using Arcen.Universal;
using Arcen.AIW2.Core;
using System;
using System.Collections.Generic;
using UnityEngine;

namespace Arcen.AIW2.External
{
    public class Window_InGameBottomMenu : WindowControllerAbstractBase
    {
        public static Window_InGameBottomMenu Instance;
        public Window_InGameBottomMenu()
        {
            Instance = this;
            this.OnlyShowInGame = true;
            this.SupportsMasterMenuKeys = true;
        }

        public class bToggleMasterMenu : WindowTogglingButtonController
        {
            public static bToggleMasterMenu Instance;
            public bToggleMasterMenu() : base( "Menu", "^" ) { Instance = this; }
            public override ToggleableWindowController GetRelatedController() { return Window_InGameMasterMenu.Instance; }
        }

        public void CloseAllExpansions()
        {
            this.CloseWindowsOtherThanThisOne( null );
        }

        public class bsControlGroupRow : ButtonSetAbstractBase
        {
            public override void OnUpdate()
            {
                WorldSide localSide = World_AIW2.Instance.GetLocalSide();
                if ( localSide == null )
                    return;
                ArcenUI_ButtonSet elementAsType = (ArcenUI_ButtonSet)Element;
                //Window_InGameBottomMenu windowController = (Window_InGameBottomMenu)Element.Window.Controller;

                if ( elementAsType.Buttons.Count <= 0 )
                {
                    elementAsType.ClearButtons();

                    int numberOfButtons = 9;
                    for ( int x = 0; x < numberOfButtons; x++ )
                    {
                        bControlGroup newButtonController = new bControlGroup( x );
                        Vector2 offset;
                        offset.x = x * elementAsType.ButtonWidth;
                        offset.y = 0;
                        Vector2 size;
                        size.x = elementAsType.ButtonWidth;
                        size.y = elementAsType.ButtonHeight;
                        elementAsType.AddButton( newButtonController, size, offset );
                    }

                    {
                        bToggleMasterMenu newButtonController = new bToggleMasterMenu();
                        Vector2 offset;
                        offset.x = numberOfButtons * elementAsType.ButtonWidth;
                        offset.y = 0;
                        Vector2 size;
                        size.x = elementAsType.ButtonWidth;
                        size.y = elementAsType.ButtonHeight;
                        elementAsType.AddButton( newButtonController, size, offset );
                    }
                }
            }
        }

        private class bControlGroup : ButtonAbstractBase
        {
            public int ControlGroupIndex;

            public bControlGroup( int MenuIndex )
            {
                this.ControlGroupIndex = MenuIndex;
            }

            private ControlGroup GetControlGroup()
            {
                WorldSide localSide = World_AIW2.Instance.GetLocalSide();
                if ( this.ControlGroupIndex < 0 || this.ControlGroupIndex >= localSide.ControlGroups.Count )
                    return null;
                return localSide.ControlGroups[this.ControlGroupIndex];
            }

            public override void GetTextToShow( ArcenDoubleCharacterBuffer Buffer )
            {
                base.GetTextToShow( Buffer );
                //Buffer.Add( ( this.ControlGroupIndex + 1 ) );
                ControlGroup group = this.GetControlGroup();
                if ( group != null )
                {
                    //Buffer.Add( " (" );
                    Buffer.Add( group.Name );
                    //Buffer.Add( ")" );
                }
                else
                {
                    //Buffer.Add( "Empty" );
                }
            }

            public override void HandleClick()
            {
                bool clearSelectionFirst = false;
                bool unselectingInstead = false;
                if ( Engine_AIW2.Instance.PresentationLayer.GetAreInputFlagsActive( ArcenInputFlags.Additive ) )
                { }
                else if ( Engine_AIW2.Instance.PresentationLayer.GetAreInputFlagsActive( ArcenInputFlags.Subtractive ) )
                {
                    unselectingInstead = true;
                }
                else
                {
                    clearSelectionFirst = true;
                }

                bool isAssigningToGroup = Engine_AIW2.Instance.PresentationLayer.GetAreInputFlagsActive( ArcenInputFlags.ModifyingControlGroup );

                ControlGroup group = this.GetControlGroup();

                if ( group == null )
                    return;

                Planet planet = Engine_AIW2.Instance.NonSim_GetPlanetBeingCurrentlyViewed();
                bool foundOne = false;

                if ( isAssigningToGroup )
                {
                    GameCommandType commandType = GameCommandType.SetControlGroupPopulation;
                    if ( unselectingInstead )
                        commandType = GameCommandType.RemoveFromControlGroupPopulation;
                    else if ( !clearSelectionFirst )
                        commandType = GameCommandType.AddToControlGroupPopulation;
                    else
                        commandType = GameCommandType.SetControlGroupPopulation;
                    GameCommand command = GameCommand.Create( commandType );
                    command.SentWithToggleSet_SetOrdersForProducedUnits = Engine_AIW2.Instance.SettingOrdersForProducedUnits;
                    command.RelatedControlGroup = group;
                    Engine_AIW2.Instance.DoForSelected( delegate ( GameEntity selected )
                    {
                        command.RelatedEntityIDs.Add( selected.PrimaryKeyID );
                        return DelReturn.Continue;
                    } );
                    World_AIW2.Instance.QueueGameCommand( command );
                }
                else
                {
                    group.DoForEntities( delegate ( GameEntity entity )
                    {
                        if ( entity.Combat.Planet != planet )
                            return DelReturn.Continue;
                        if ( !foundOne )
                        {
                            foundOne = true;
                            if ( clearSelectionFirst )
                            {
                                if ( entity.GetIsSelected() )
                                    Engine_AIW2.Instance.PresentationLayer.CenterPlanetViewOnEntity( entity, false );
                                Engine_AIW2.Instance.ClearSelection();
                            }
                        }
                        if ( unselectingInstead )
                            entity.Unselect();
                        else
                            entity.Select();
                        return DelReturn.Continue;
                    } );
                    if ( !foundOne && clearSelectionFirst )
                    {
                        Engine_AIW2.Instance.ClearSelection();
                        group.DoForEntities( delegate ( GameEntity entity )
                        {
                            if ( !foundOne )
                            {
                                foundOne = true;
                                Engine_AIW2.Instance.PresentationLayer.ReactToLeavingPlanetView( planet );
                                planet = entity.Combat.Planet;
                                World_AIW2.Instance.SwitchViewToPlanet( planet );
                                Engine_AIW2.Instance.PresentationLayer.CenterPlanetViewOnEntity( entity, true );
                                Engine_AIW2.Instance.PresentationLayer.ReactToEnteringPlanetView( planet );
                            }
                            if ( entity.Combat.Planet != planet )
                                return DelReturn.Continue;
                            entity.Select();
                            return DelReturn.Continue;
                        } );
                    }
                }
            }

            public override void HandleMouseover() { }

            public override void OnUpdate()
            {
            }
        }
    }
}