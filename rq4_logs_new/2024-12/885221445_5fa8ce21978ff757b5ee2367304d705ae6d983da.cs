using System.Collections;
using System.Collections.Generic;
using UnityEngine;

static public class RuneSystem
{
    static public string RuneAssign(int x, int y)
    {
        if (y <= 5 && y >= 2)
        {
            if ((y == 2 || y == 5) && (x == 0 || x == 2 || x == 5 || x == 7))
            {
                return "None";
            }
            else
            {
                int rand = Random.Range(1, 11);
                switch (rand)
                {
                    case 1: return "Parelyze";
                    case 2: return "Vines";
                    case 3: return "Frozen";
                    case 4: return "Haste";
                    case 5: return "Slow";
                    case 6: return "Smite";
                    case 7: return "Flash";
                    case 8: return "Observe";
                    case 9: return "Erase";
                    case 10: return "Choose";
                    default: return "None";
                }
            }
        }
        return "None";
    }

    static public void RuneActivate(int x, int y)
    {
        string activeRune = GameObject.Find("ChessBoard").GetComponent<Chessboard>().tiles[x, y].GetComponent<Rune>().tileRune;
        GameObject ui = GameObject.Find("UI");
        if (activeRune != "None")
        {
            ui.GetComponent<UI>().displayRune(activeRune);
        }
        switch (activeRune)
        {
            case "Parelyze":
                Debug.Log("Rune: Parelyze");
                parelyze();
                break;
            case "Vines":
                Debug.Log("Rune:Vines");
                vines();
                break;
            case "Frozen":
                Debug.Log("Frozen");
                frozen();
                break;
            case "Erase":
                Debug.Log("Rune:Erase");
                erase();
                break;
            case "Slow":
                Debug.Log("Rune:Slow");
                slow();
                break;
            default:
                GameObject.Find("ChessBoard").GetComponent<Chessboard>().isRunePhase = false;
                ui.GetComponent<UI>().resetTimer(30);
                break;
        }
    }

    static private void slow()
    {
        Chessboard chessboard = GameObject.Find("ChessBoard").GetComponent<Chessboard>();
        chessboard.isRunePhase = false;
        GameObject.Find("UI").GetComponent<UI>().displayRune("");
        GameObject.Find("UI").GetComponent<UI>().resetTimer(15);
    }

    static private void erase()
    {
        Chessboard chessboard = GameObject.Find("ChessBoard").GetComponent<Chessboard>();
        RaycastHit info; // raycast가 충돌한 정보 저장
        Ray ray = chessboard.currentCamera.ScreenPointToRay(Input.mousePosition); // 마우스 위치에서 화면 상 좌표를 3D로 변환하는 ray 생성

        if (Physics.Raycast(ray, out info, 100, LayerMask.GetMask("Tile", "Hover", "Highlight", "Insight", "Special")))
        {
            Vector2Int hitPosition = chessboard.GetComponent<Chessboard>().LookUpTileIndex(info.transform.gameObject);
            if (chessboard.currentHover == -Vector2Int.one) // hovering background -> tile (new hovering)
            {
                chessboard.currentHover = hitPosition;
                chessboard.tiles[hitPosition.x, hitPosition.y].layer = LayerMask.NameToLayer("Hover"); // tile에 hover 효과 적용을 위해 layer 변경 ("Tile" -> "Hover")
            }
            // tile A -> tile B hovering 경우 (hovering change)
            if (chessboard.currentHover != hitPosition) // hovering tile A -> tile B (tile 변경)
            {
                // 기존 "Highlight" or "Tile" layer로 각각 복구
                chessboard.tiles[chessboard.currentHover.x, chessboard.currentHover.y].layer = chessboard.ReturnToOriginalTile();
                chessboard.currentHover = hitPosition;
                chessboard.tiles[hitPosition.x, hitPosition.y].layer = LayerMask.NameToLayer("Hover"); // 새로운 hover tile -> "Hover" layer로 변경
            }

            // 선택한 chess 말 이동에 대한 작업 - chess 말 선택 (click) -> 이동 tile 선택 (click)으로 2회 click으로 구성
            if (Input.GetMouseButtonDown(0)) // mouse left 클릭
            {
                chessboard.tiles[hitPosition.x, hitPosition.y].GetComponent<Rune>().tileRune = "None";
                chessboard.tiles[hitPosition.x, hitPosition.y].transform.name = string.Format("X:{0} Y:{1} Rune:None", hitPosition.x, hitPosition.y);
                chessboard.isRunePhase = false;
                GameObject.Find("UI").GetComponent<UI>().displayRune("");
                GameObject.Find("UI").GetComponent<UI>().resetTimer(30);
            }


        }
    }

    static private void frozen()
    {
        Chessboard chessboard = GameObject.Find("ChessBoard").GetComponent<Chessboard>();
        Vector2Int lastMove = chessboard.lastMove;
        chessboard.chessPieces[lastMove.x, lastMove.y].isInvincible = true;
        chessboard.isRunePhase = false;
        GameObject.Find("UI").GetComponent<UI>().displayRune("");
        GameObject.Find("UI").GetComponent<UI>().resetTimer(30);
    }

    static private void vines()
    {
        Chessboard chessboard = GameObject.Find("ChessBoard").GetComponent<Chessboard>();
        Vector2Int lastMove = chessboard.lastMove;
        chessboard.chessPieces[lastMove.x, lastMove.y].isActive = false;
        chessboard.chessPieces[lastMove.x, lastMove.y].isVined = true;
        chessboard.isRunePhase = false;
        GameObject.Find("UI").GetComponent<UI>().displayRune("");
        GameObject.Find("UI").GetComponent<UI>().resetTimer(30);
    }

    static private void parelyze()
    {
        Chessboard chessboard = GameObject.Find("ChessBoard").GetComponent<Chessboard>();
        RaycastHit info; // raycast가 충돌한 정보 저장
        Ray ray = chessboard.currentCamera.ScreenPointToRay(Input.mousePosition); // 마우스 위치에서 화면 상 좌표를 3D로 변환하는 ray 생성
        if (Physics.Raycast(ray, out info, 100, LayerMask.GetMask("Tile", "Hover", "Highlight", "Insight", "Special")))
        {
            Vector2Int hitPosition = chessboard.GetComponent<Chessboard>().LookUpTileIndex(info.transform.gameObject);
            if (chessboard.currentHover == -Vector2Int.one) // hovering background -> tile (new hovering)
            {
                chessboard.currentHover = hitPosition;
                chessboard.tiles[hitPosition.x, hitPosition.y].layer = LayerMask.NameToLayer("Hover"); // tile에 hover 효과 적용을 위해 layer 변경 ("Tile" -> "Hover")
            }
            // tile A -> tile B hovering 경우 (hovering change)
            if (chessboard.currentHover != hitPosition) // hovering tile A -> tile B (tile 변경)
            {
                // 기존 "Highlight" or "Tile" layer로 각각 복구
                chessboard.tiles[chessboard.currentHover.x, chessboard.currentHover.y].layer = chessboard.ReturnToOriginalTile();
                chessboard.currentHover = hitPosition;
                chessboard.tiles[hitPosition.x, hitPosition.y].layer = LayerMask.NameToLayer("Hover"); // 새로운 hover tile -> "Hover" layer로 변경
            }

            // 선택한 chess 말 이동에 대한 작업 - chess 말 선택 (click) -> 이동 tile 선택 (click)으로 2회 click으로 구성
            if (Input.GetMouseButtonDown(0)) // mouse left 클릭
            {
                // 첫번째 클릭에 관한 작업 (chess 말 선택)
                if (chessboard.chessPieces[hitPosition.x, hitPosition.y] != null && !chessboard.islifting) // 클릭한 위치에 chessPiece 존재 && chessPiece lifting X (none chosen chessPiece)
                {
                    if ((chessboard.chessPieces[hitPosition.x, hitPosition.y].team == 1 && !chessboard.isWhiteTurn && chessboard.currentTeam == 1)
                    || (chessboard.chessPieces[hitPosition.x, hitPosition.y].team == 0 && chessboard.isWhiteTurn && chessboard.currentTeam == 0)) // check turn
                    {
                        chessboard.currentlyDragging = chessboard.chessPieces[hitPosition.x, hitPosition.y]; // 현재 클릭 한 chess 말 저장
                        chessboard.HighlightTiles(); // highlight
                        chessboard.islifting = chessboard.liftingPiece(); // chessPiece lifting (islifting = true로 변경)
                    }
                }
                else if (chessboard.islifting) // islifting = chessPiece가 선택되어 있고 공중에 있음 (already chosen ChessPiece)
                {
                    Vector2Int previousPosition = new Vector2Int(chessboard.currentlyDragging.currentX, chessboard.currentlyDragging.currentY); // currentX, Y = 이동 전 위치

                    if (previousPosition == hitPosition)
                    {
                        chessboard.chessPieces[hitPosition.x, hitPosition.y].isActive = false;
                        chessboard.isRunePhase = false;
                        GameObject.Find("UI").GetComponent<UI>().displayRune("");
                        GameObject.Find("UI").GetComponent<UI>().resetTimer(30);
                        Debug.Log("RunPhase off");
                    }
                    //else // 이동 불가
                    {
                        chessboard.currentlyDragging.SetPosition(chessboard.GetTileCenter(previousPosition.x, previousPosition.y), false); // 위치 원상 복구
                        chessboard.RemoveHighlightTiles(); // highlight 제거
                        chessboard.islifting = chessboard.landingPiece(); // chessPiece landing (islifting = false로 변경)
                        chessboard.currentlyDragging = null; // 선택 말 해제
                    }
                }
            }
        }
        else // "Tile", "Hover", "Highlight" 객체와 충돌 X (board 아닌 곳 hover 하는 중)
        {
            // tile -> background
            if (chessboard.currentHover != -Vector2Int.one) // tile 작업 - chess board에서 벗어난 곳으로 hover 시
            {
                chessboard.tiles[chessboard.currentHover.x, chessboard.currentHover.y].layer = chessboard.ReturnToOriginalTile();
                chessboard.currentHover = -Vector2Int.one; // tile 밖이므로 위치 정보 (-1, -1) 저장
            }

            if (chessboard.currentlyDragging && Input.GetMouseButtonDown(0) && chessboard.islifting) // chess 말 작업 - chess board에서 벗어난 곳으로 drag 시
            {
                chessboard.currentlyDragging.SetPosition(chessboard.GetTileCenter(chessboard.currentlyDragging.currentX, chessboard.currentlyDragging.currentY), false); // 위치 원상 복구
                chessboard.RemoveHighlightTiles();
                chessboard.islifting = chessboard.landingPiece();
                chessboard.currentlyDragging = null;
            }
        }
    }
}