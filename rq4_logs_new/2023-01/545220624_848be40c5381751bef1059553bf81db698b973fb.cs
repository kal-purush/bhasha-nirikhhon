using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Windows.Forms; //OpenFileDialog用に使う
using System.IO;

public class MakeAudioClip : MonoBehaviour
{
    public AudioSource mainBGM;
    public AudioSource beat;

    int BPM = 120;

    public void Make()
    {
        if(mainBGM==null || beat == null)   return;
        
        float duration = mainBGM.clip.length;

        int frequency = 44100; //サンプリング周波数

        int sampleCount = (int)(frequency * duration); //音声データのサンプル数

        float[] samples = new float[sampleCount]; //音声データ用の配列
        
        beat.clip.GetData(samples, 0); //既存のAudioClipから音声データを取得

        float beatInterval = 60f / (float)BPM; //1拍の長さ(秒)
        int beatIntervalSampleCount = (int)(beatInterval * frequency); //1拍の長さのサンプル数

        for (int i = 0; i < sampleCount; i++) {
            //音声データを切り出し
            samples[i] = samples[i % beatIntervalSampleCount];
        }

        AudioClip clip = AudioClip.Create("ExistingClip", sampleCount, 1, frequency, false);
        clip.SetData(samples, 0);

        Save(clip);
    }

    void Save(AudioClip clip){
        // ダイアログボックスの表示()
        SaveFileDialog sfd = new SaveFileDialog();

        sfd.FileName = "ファイル";
        sfd.InitialDirectory = "";
        sfd.Filter = "wavファイル|*.wav";
        sfd.FilterIndex = 2;
        sfd.Title = "保存先のファイルを選択してください";
        sfd.RestoreDirectory = true;//ダイアログボックスを閉じる前に現在のディレクトリを復元するようにする

        if (sfd.ShowDialog() == DialogResult.OK)
        {
            File.WriteAllBytes(sfd.FileName, WavUtility.FromAudioClip(clip));
        }
    }

}