import matplotlib.pyplot as plt
import nrrd
import os


def display_needle(subject, slice, expt, phase='AC', save=True):
    file_path = "C:/Users/roybo/OneDrive - University College London/PhD/PhD_Prog/007_CNN_Virtual_Contrast/Phase2/output"
    save_path = "C:/Users/roybo/OneDrive - University College London/PhD/Publications/005_QuantVirtualContrast/needle"
    imgs = [f for f in os.listdir(f"{file_path}/{expt}/Needle") if subject in f]

    if expt == "Real":
        HQ = [f for f in imgs if 'HQ' in f]
        HQ.sort()
        N = len(HQ)
    else:
        AC = [f for f in imgs if 'AP' in f]
        VC = [f for f in imgs if 'VP' in f]
        AC.sort()
        VC.sort()
        assert len(AC) == len(VC)
        N = len(AC)

    nrows = 3
    ncols = N // nrows
    remainder = N % nrows

    plt.figure(figsize=(18, 12))
    for i in range(N):
        if phase == 'AC' and expt != "Real":
            im, _ = nrrd.read(f"{file_path}/{expt}/Needle/{AC[i]}")
        elif phase == 'VC' and expt != "Real":
            im, _ = nrrd.read(f"{file_path}/{expt}/Needle/{VC[i]}")
        else:
            im, _ = nrrd.read(f"{file_path}/{expt}/Needle/{HQ[i]}")
        plt.subplot(nrows, ncols + remainder, i + 1)
        plt.imshow(im[:, :, slice].T, cmap="gray", vmin=-150, vmax=250)
        plt.axis("off")

    if save:
        plt.savefig(f"{save_path}/{subject}_{slice}_{expt}.png")
        plt.close()
    else:
        plt.show()


if __name__ == "__main__":
    expt_list = ["Real",
                 "UNetACVC",
                 "UNetT_save1000",
                 "2_save170_patch",
                 "CycleGANT_save880"]
    subject_dict = {"T057A0": [22, 34],
                    "T058A0": [12, 20, 31],
                    "T061A0": [26, 29],
                    "T063A0": [51, 56],
                    "T064A0": [31, 42],
                    "T069A0": [67]}

    for subject, slices in subject_dict.items():
        for slice in slices:
            for expt in expt_list:
                display_needle(subject, slice, expt, save=True)


""" T057A0 22, 34
    T058A0 12, 20, 31
    T061A0 26, 29
    T063A0 51, 56
    T064A0 31, 42
    T069A0 67 """