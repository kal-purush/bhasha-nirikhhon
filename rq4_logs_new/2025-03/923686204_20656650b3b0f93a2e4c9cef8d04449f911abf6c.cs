using UnityEngine;

public class RemovingState : IBuildingState
{
    [Header("External Scripts")]
    [SerializeField] private ObjectPlacer objectPlacer;
    [SerializeField] private PreviewSystem previewSystem;

    [Header("Tower Info")]
    //-1 means null there's isn't a tower selected

    [SerializeField] private int selectedTowerIndex = -1;
    [SerializeField] private int towerID;

    [Header("Tower Grid Info")]
    [SerializeField] private Grid grid;
    [SerializeField] private GridData towerData;

    public RemovingState(ObjectPlacer objectPlacer, PreviewSystem previewSystem, int selectedTowerIndex, int towerID, Grid grid, GridData towerData)
    {
        this.objectPlacer = objectPlacer;
        this.previewSystem = previewSystem;
        this.selectedTowerIndex = selectedTowerIndex;
        this.towerID = towerID;
        this.grid = grid;
        this.towerData = towerData;

        previewSystem.StartShowingrRemovePreview();
    }

    public void EndState()
    {
        previewSystem.StopShowingPreview();
    }

    public void OnAction(Vector3Int gridPos)
    {
        throw new System.NotImplementedException();
    }

    public void UpdateState(Vector3Int gridPos)
    {
        throw new System.NotImplementedException();
    }
}