import os
import cv2
from scipy.special import gammainc


def chi_square(image):
    hist = cv2.calcHist([image], [0], None, [256], [0, 256]).ravel()

    k = hist.shape[0] // 2

    chi_sq = 0.0

    for i in range(k):
        n2i = hist[2*i]
        n2i1 = hist[2*i+1]
        avg_pair = (n2i + n2i1) / 2.0
        numerator = (n2i - avg_pair) ** 2
        denominator = avg_pair + 1e-10

        chi_sq += numerator / denominator

    df = k - 1
    p_value = 1.0 - gammainc(df / 2.0, chi_sq / 2.0)

    return chi_sq, p_value


def chi_square_analysis(stego_image_path):
    stego_image = cv2.imread(stego_image_path, cv2.IMREAD_GRAYSCALE)

    chi_sq, p_value = chi_square(stego_image)

    print(f"chi-square = {chi_sq}")
    print(f"p-value = {p_value}")

    alpha = 0.05
    if p_value < alpha:
        print(
            f"\nW tym obrazie prawdopodobnie została ukryta wiadomość, ponieważ {p_value} < {alpha}")
    else:
        print(
            f"\nW tym obrazie prawdopodobnie nie została ukryta wiadomość, ponieważ {p_value} >= {alpha}")


if __name__ == "__main__":
    # stego_image_path ='./LSB/LSB_1.jpg'
    # chi_square_analysis(stego_image_path)
    directory = "./LSB"
    for entry in os.listdir(directory):
        stego_image_path = os.path.join(directory, entry)
        print(stego_image_path)

        chi_square_analysis(stego_image_path)