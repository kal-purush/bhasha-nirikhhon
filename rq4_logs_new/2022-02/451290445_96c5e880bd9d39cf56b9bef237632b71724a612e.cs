using System;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class BoardMediator : MonoBehaviour
{
    [SerializeField] private TMP_Text txtName, txtStake,txtMinU, txtStatus;
    [SerializeField] private Image imgIsGa, imgIsKhoa;
    [SerializeField] private Image[] playerImages;
    [SerializeField] private Sprite[] playerSprites, iconLocks,iconGas;
    [SerializeField] private GameObject popupNhapMatKhau,imgStatus;
    [SerializeField] TMP_InputField ipfPassword;
    [SerializeField] Button btnVaoBan;
    private ScreenManager screenManager;
    private UserModel userModel = UserModel.Instance;
    private BoardInfoModel boardInfoModel = BoardInfoModel.Instance;
    private int boardId;
    private BoardInfo bInfo;

    void Awake()
    {
        screenManager = ScreenManager.Instance;
    }

    public void SetData(int id)
    {
        boardId = id;
        txtName.text = (id + 1).ToString();
    }

    public void OnBoardClick()
    {
        if(locked && !GameUtils.IsMod(int.Parse(userModel.uid))) AskPass();
        else
        {
            screenManager.JoinBoard(boardId,screenManager.room);
        }
    }

    public BoardInfo GetBoardInfo => bInfo;
    public int GetBoardID => boardId;
    public bool IsFullBoard => GetBoardInfo.sitCount >= 4;

    public void UpdateData(BoardInfo info)
    {
        bInfo = info;

        for(var i = 0; i < playerImages.Length;i++)
        {
            playerImages[i].sprite = i < info.sitCount ? info.isPlaying ? playerSprites[2] : playerSprites[1] : playerSprites[0];
            playerImages[i].SetNativeSize();
        }

            txtName.text = (boardId + 1).ToString();
            txtStake.text = StringUtils.FormatMoneyK(info.stake);
            imgIsKhoa.sprite = info.isLocked ? iconLocks[1] : iconLocks[0];
            imgIsKhoa.SetNativeSize();
            imgIsGa.sprite = info.ga ? iconGas[1] : iconGas[0];
            imgIsGa.SetNativeSize();

            if (txtStatus != null)
            {
                txtStatus.text = info.isPlaying ? "<color=#d07116>Đang chơi</color>" : "<color=#08dd9b>Đang chờ</color>";
            }

            switch (info.minU)
            {
            case 2:
                txtMinU.text = "Suông";
                break;
            case 3:
                txtMinU.text = "3 điểm";
                break;
            case 4:
                txtMinU.text = "4 điểm";
                break;
            }
    }

    private void AskPass()
    {
        popupNhapMatKhau.Show();
        ipfPassword.text = "";
        btnVaoBan.onClick.RemoveAllListeners();
        btnVaoBan.onClick.AddListener(JoinRoomWithPassword);
    }

    private void JoinRoomWithPassword()
    {
        popupNhapMatKhau.Hide();
        if (string.IsNullOrEmpty(ipfPassword.text))
        {
            Debug.Log("Lỗi");
        }
        else
        {
            screenManager.JoinBoard(boardId, screenManager.room, ipfPassword.text);
        }
    }

    private bool locked => boardInfoModel.GetInfo(boardId).isLocked;
}