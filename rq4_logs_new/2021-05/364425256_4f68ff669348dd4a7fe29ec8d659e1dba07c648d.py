from time import time
from random import randint

from data.images3 import *
from data.images2 import *
from data.images1 import *
from data.defaultImages import *

def play_word(word, speed, display_image, window, v):
    print('playing word')
    startTime = time()
    interval = 60/speed
    index = 0
    lastLetter = ''
    letter = word[index]
    r = randint(1, 3)
    while index < len(word):
        if time() >= startTime+interval*(index+1):
            index += 1
            lastLetter = letter
            if index == len(word):
                break
            letter = word[index]
            if v:print(lastLetter, letter)
        else:
            if lastLetter == letter:
                double = True
            else:
                double = False
            if r == 0:
                print("ERROR")


            elif r == 4:
                if letter == 'a':
                    if double:
                        display_image.configure(image = A42render)
                        display_image.image = A42render
                    else:
                        display_image.configure(image = A4render)
                        display_image.image = A4render
                    window.update()

                elif letter == 'b':
                    if double:
                        display_image.configure(image = B42render)
                        display_image.image = B42render
                    else:
                        display_image.configure(image = B4render)
                        display_image.image = B4render
                    window.update()
                elif letter == 'c':
                    if double:
                        display_image.configure(image = C42render)
                        display_image.image = C42render
                    else:
                        display_image.configure(image = C4render)
                        display_image.image = C4render
                    window.update()
                elif letter == 'd':
                    if double:
                        display_image.configure(image = D42render)
                        display_image.image = D42render
                    else:
                        display_image.configure(image = D4render)
                        display_image.image = D4render
                    window.update()
                elif letter == 'e':
                    if double:
                        display_image.configure(image = E42render)
                        display_image.image = E42render
                    else:
                        display_image.configure(image = E4render)
                        display_image.image = E4render
                    window.update()
                elif letter == 'f':
                    if double:
                        display_image.configure(image = F42render)
                        display_image.image = F42render
                    else:
                        display_image.configure(image = F4render)
                        display_image.image = F4render
                    window.update()
                elif letter == 'g':
                    if double:
                        display_image.configure(image = G42render)
                        display_image.image = G42render
                    else:
                        display_image.configure(image = G4render)
                        display_image.image = G4render
                    window.update()
                elif letter == 'h':
                    if double:
                        display_image.configure(image = H42render)
                        display_image.image = H42render
                    else:
                        display_image.configure(image = H4render)
                        display_image.image = H4render
                    window.update()
                elif letter == 'i':
                    if double:
                        display_image.configure(image = I42render)
                        display_image.image = I42render
                    else:
                        display_image.configure(image = I4render)
                        display_image.image = I4render
                    window.update()
                elif letter == 'j':
                    if double:
                        display_image.configure(image = J42render)
                        display_image.image = J42render
                    else:
                        display_image.configure(image = J4render)
                        display_image.image = J4render
                    window.update()
                elif letter == 'k':
                    if double:
                        display_image.configure(image = K42render)
                        display_image.image = K42render
                    else:
                        display_image.configure(image = K4render)
                        display_image.image = K4render
                    window.update()
                elif letter == 'l':
                    if double:
                        display_image.configure(image = L42render)
                        display_image.image = L42render
                    else:
                        display_image.configure(image = L4render)
                        display_image.image = L4render
                    window.update()
                elif letter == 'm':
                    if double:
                        display_image.configure(image = M42render)
                        display_image.image = M42render
                    else:
                        display_image.configure(image = M4render)
                        display_image.image = M4render
                    window.update()
                elif letter == 'n':
                    if double:
                        display_image.configure(image = N42render)
                        display_image.image = N42render
                    else:
                        display_image.configure(image = N4render)
                        display_image.image = N4render
                    window.update()
                elif letter == 'o':
                    if double:
                        display_image.configure(image = O42render)
                        display_image.image = O42render
                    else:
                        display_image.configure(image = O4render)
                        display_image.image = O4render
                    window.update()
                elif letter == 'p':
                    if double:
                        display_image.configure(image = P42render)
                        display_image.image = P42render
                    else:
                        display_image.configure(image = P4render)
                        display_image.image = P4render
                    window.update()
                elif letter == 'q':
                    if double:
                        display_image.configure(image = Q42render)
                        display_image.image = Q42render
                    else:
                        display_image.configure(image = Q4render)
                        display_image.image = Q4render
                    window.update()
                elif letter == 'r':
                    if double:
                        display_image.configure(image = R42render)
                        display_image.image = R42render
                    else:
                        display_image.configure(image = R4render)
                        display_image.image = R4render
                    window.update()
                elif letter == 's':
                    if double:
                        display_image.configure(image = S42render)
                        display_image.image = S42render
                    else:
                        display_image.configure(image = S4render)
                        display_image.image = S4render
                    window.update()
                elif letter == 't':
                    if double:
                        display_image.configure(image = T42render)
                        display_image.image = T42render
                    else:
                        display_image.configure(image = T4render)
                        display_image.image = T4render
                    window.update()
                elif letter == 'u':
                    if double:
                        display_image.configure(image = U42render)
                        display_image.image = U42render
                    else:
                        display_image.configure(image = U4render)
                        display_image.image = U4render
                    window.update()
                elif letter == 'v':
                    if double:
                        display_image.configure(image = V42render)
                        display_image.image = V42render
                    else:
                        display_image.configure(image = V4render)
                        display_image.image = V4render
                    window.update()
                elif letter == 'w':
                    if double:
                        display_image.configure(image = W42render)
                        display_image.image = W42render
                    else:
                        display_image.configure(image = W4render)
                        display_image.image = W4render
                    window.update()
                elif letter == 'x':
                    if double:
                        display_image.configure(image = X42render)
                        display_image.image = X42render
                    else:
                        display_image.configure(image = X4render)
                        display_image.image = X4render
                    window.update()
                elif letter == 'y':
                    if double:
                        display_image.configure(image = Y42render)
                        display_image.image = Y42render
                    else:
                        display_image.configure(image = Y4render)
                        display_image.image = Y4render
                    window.update()
                elif letter == 'z':
                    if double:
                        display_image.configure(image = Z42render)
                        display_image.image = Z42render
                    else:
                        display_image.configure(image = Z4render)
                        display_image.image = Z4render
                    window.update()

            elif r == 3:
                if letter == 'a':
                    if double:
                        display_image.configure(image = A32render)
                        display_image.image = A32render
                    else:
                        display_image.configure(image = A3render)
                        display_image.image = A3render
                    window.update()

                elif letter == 'b':
                    if double:
                        display_image.configure(image = B32render)
                        display_image.image = B32render
                    else:
                        display_image.configure(image = B3render)
                        display_image.image = B3render
                    window.update()
                elif letter == 'c':
                    if double:
                        display_image.configure(image = C32render)
                        display_image.image = C32render
                    else:
                        display_image.configure(image = C3render)
                        display_image.image = C3render
                    window.update()
                elif letter == 'd':
                    if double:
                        display_image.configure(image = D32render)
                        display_image.image = D32render
                    else:
                        display_image.configure(image = D3render)
                        display_image.image = D3render
                    window.update()
                elif letter == 'e':
                    if double:
                        display_image.configure(image = E32render)
                        display_image.image = E32render
                    else:
                        display_image.configure(image = E3render)
                        display_image.image = E3render
                    window.update()
                elif letter == 'f':
                    if double:
                        display_image.configure(image = F32render)
                        display_image.image = F32render
                    else:
                        display_image.configure(image = F3render)
                        display_image.image = F3render
                    window.update()
                elif letter == 'g':
                    if double:
                        display_image.configure(image = G32render)
                        display_image.image = G32render
                    else:
                        display_image.configure(image = G3render)
                        display_image.image = G3render
                    window.update()
                elif letter == 'h':
                    if double:
                        display_image.configure(image = H32render)
                        display_image.image = H32render
                    else:
                        display_image.configure(image = H3render)
                        display_image.image = H3render
                    window.update()
                elif letter == 'i':
                    if double:
                        display_image.configure(image = I32render)
                        display_image.image = I32render
                    else:
                        display_image.configure(image = I3render)
                        display_image.image = I3render
                    window.update()
                elif letter == 'j':
                    if double:
                        display_image.configure(image = J32render)
                        display_image.image = J32render
                    else:
                        display_image.configure(image = J3render)
                        display_image.image = J3render
                    window.update()
                elif letter == 'k':
                    if double:
                        display_image.configure(image = K32render)
                        display_image.image = K32render
                    else:
                        display_image.configure(image = K3render)
                        display_image.image = K3render
                    window.update()
                elif letter == 'l':
                    if double:
                        display_image.configure(image = L32render)
                        display_image.image = L32render
                    else:
                        display_image.configure(image = L3render)
                        display_image.image = L3render
                    window.update()
                elif letter == 'm':
                    if double:
                        display_image.configure(image = M32render)
                        display_image.image = M32render
                    else:
                        display_image.configure(image = M3render)
                        display_image.image = M3render
                    window.update()
                elif letter == 'n':
                    if double:
                        display_image.configure(image = N32render)
                        display_image.image = N32render
                    else:
                        display_image.configure(image = N3render)
                        display_image.image = N3render
                    window.update()
                elif letter == 'o':
                    if double:
                        display_image.configure(image = O32render)
                        display_image.image = O32render
                    else:
                        display_image.configure(image = O3render)
                        display_image.image = O3render
                    window.update()
                elif letter == 'p':
                    if double:
                        display_image.configure(image = P32render)
                        display_image.image = P32render
                    else:
                        display_image.configure(image = P3render)
                        display_image.image = P3render
                    window.update()
                elif letter == 'q':
                    if double:
                        display_image.configure(image = Q32render)
                        display_image.image = Q32render
                    else:
                        display_image.configure(image = Q3render)
                        display_image.image = Q3render
                    window.update()
                elif letter == 'r':
                    if double:
                        display_image.configure(image = R32render)
                        display_image.image = R32render
                    else:
                        display_image.configure(image = R3render)
                        display_image.image = R3render
                    window.update()
                elif letter == 's':
                    if double:
                        display_image.configure(image = S32render)
                        display_image.image = S32render
                    else:
                        display_image.configure(image = S3render)
                        display_image.image = S3render
                    window.update()
                elif letter == 't':
                    if double:
                        display_image.configure(image = T32render)
                        display_image.image = T32render
                    else:
                        display_image.configure(image = T3render)
                        display_image.image = T3render
                    window.update()
                elif letter == 'u':
                    if double:
                        display_image.configure(image = U32render)
                        display_image.image = U32render
                    else:
                        display_image.configure(image = U3render)
                        display_image.image = U3render
                    window.update()
                elif letter == 'v':
                    if double:
                        display_image.configure(image = V32render)
                        display_image.image = V32render
                    else:
                        display_image.configure(image = V3render)
                        display_image.image = V3render
                    window.update()
                elif letter == 'w':
                    if double:
                        display_image.configure(image = W32render)
                        display_image.image = W32render
                    else:
                        display_image.configure(image = W3render)
                        display_image.image = W3render
                    window.update()
                elif letter == 'x':
                    if double:
                        display_image.configure(image = X32render)
                        display_image.image = X32render
                    else:
                        display_image.configure(image = X3render)
                        display_image.image = X3render
                    window.update()
                elif letter == 'y':
                    if double:
                        display_image.configure(image = Y32render)
                        display_image.image = Y32render
                    else:
                        display_image.configure(image = Y3render)
                        display_image.image = Y3render
                    window.update()
                elif letter == 'z':
                    if double:
                        display_image.configure(image = Z32render)
                        display_image.image = Z32render
                    else:
                        display_image.configure(image = Z3render)
                        display_image.image = Z3render
                    window.update()

            elif r == 2:
                if letter == 'a':
                    if double:
                        display_image.configure(image = A22render)
                        display_image.image = A22render
                    else:
                        display_image.configure(image = A2render)
                        display_image.image = A2render
                    window.update()

                elif letter == 'b':
                    if double:
                        display_image.configure(image = B22render)
                        display_image.image = B22render
                    else:
                        display_image.configure(image = B2render)
                        display_image.image = B2render
                    window.update()
                elif letter == 'c':
                    if double:
                        display_image.configure(image = C22render)
                        display_image.image = C22render
                    else:
                        display_image.configure(image = C2render)
                        display_image.image = C2render
                    window.update()
                elif letter == 'd':
                    if double:
                        display_image.configure(image = D22render)
                        display_image.image = D22render
                    else:
                        display_image.configure(image = D2render)
                        display_image.image = D2render
                    window.update()
                elif letter == 'e':
                    if double:
                        display_image.configure(image = E22render)
                        display_image.image = E22render
                    else:
                        display_image.configure(image = E2render)
                        display_image.image = E2render
                    window.update()
                elif letter == 'f':
                    if double:
                        display_image.configure(image = F22render)
                        display_image.image = F22render
                    else:
                        display_image.configure(image = F2render)
                        display_image.image = F2render
                    window.update()
                elif letter == 'g':
                    if double:
                        display_image.configure(image = G22render)
                        display_image.image = G22render
                    else:
                        display_image.configure(image = G2render)
                        display_image.image = G2render
                    window.update()
                elif letter == 'h':
                    if double:
                        display_image.configure(image = H22render)
                        display_image.image = H22render
                    else:
                        display_image.configure(image = H2render)
                        display_image.image = H2render
                    window.update()
                elif letter == 'i':
                    if double:
                        display_image.configure(image = I22render)
                        display_image.image = I22render
                    else:
                        display_image.configure(image = I2render)
                        display_image.image = I2render
                    window.update()
                elif letter == 'j':
                    if double:
                        display_image.configure(image = J22render)
                        display_image.image = J22render
                    else:
                        display_image.configure(image = J2render)
                        display_image.image = J2render
                    window.update()
                elif letter == 'k':
                    if double:
                        display_image.configure(image = K22render)
                        display_image.image = K22render
                    else:
                        display_image.configure(image = K2render)
                        display_image.image = K2render
                    window.update()
                elif letter == 'l':
                    if double:
                        display_image.configure(image = L22render)
                        display_image.image = L22render
                    else:
                        display_image.configure(image = L2render)
                        display_image.image = L2render
                    window.update()
                elif letter == 'm':
                    if double:
                        display_image.configure(image = M22render)
                        display_image.image = M22render
                    else:
                        display_image.configure(image = M2render)
                        display_image.image = M2render
                    window.update()
                elif letter == 'n':
                    if double:
                        display_image.configure(image = N22render)
                        display_image.image = N22render
                    else:
                        display_image.configure(image = N2render)
                        display_image.image = N2render
                    window.update()
                elif letter == 'o':
                    if double:
                        display_image.configure(image = O22render)
                        display_image.image = O22render
                    else:
                        display_image.configure(image = O2render)
                        display_image.image = O2render
                    window.update()
                elif letter == 'p':
                    if double:
                        display_image.configure(image = P22render)
                        display_image.image = P22render
                    else:
                        display_image.configure(image = P2render)
                        display_image.image = P2render
                    window.update()
                elif letter == 'q':
                    if double:
                        display_image.configure(image = Q22render)
                        display_image.image = Q22render
                    else:
                        display_image.configure(image = Q2render)
                        display_image.image = Q2render
                    window.update()
                elif letter == 'r':
                    if double:
                        display_image.configure(image = R22render)
                        display_image.image = R22render
                    else:
                        display_image.configure(image = R2render)
                        display_image.image = R2render
                    window.update()
                elif letter == 's':
                    if double:
                        display_image.configure(image = S22render)
                        display_image.image = S22render
                    else:
                        display_image.configure(image = S2render)
                        display_image.image = S2render
                    window.update()
                elif letter == 't':
                    if double:
                        display_image.configure(image = T22render)
                        display_image.image = T22render
                    else:
                        display_image.configure(image = T2render)
                        display_image.image = T2render
                    window.update()
                elif letter == 'u':
                    if double:
                        display_image.configure(image = U22render)
                        display_image.image = U22render
                    else:
                        display_image.configure(image = U2render)
                        display_image.image = U2render
                    window.update()
                elif letter == 'v':
                    if double:
                        display_image.configure(image = V22render)
                        display_image.image = V22render
                    else:
                        display_image.configure(image = V2render)
                        display_image.image = V2render
                    window.update()
                elif letter == 'w':
                    if double:
                        display_image.configure(image = W22render)
                        display_image.image = W22render
                    else:
                        display_image.configure(image = W2render)
                        display_image.image = W2render
                    window.update()
                elif letter == 'x':
                    if double:
                        display_image.configure(image = X22render)
                        display_image.image = X22render
                    else:
                        display_image.configure(image = X2render)
                        display_image.image = X2render
                    window.update()
                elif letter == 'y':
                    if double:
                        display_image.configure(image = Y22render)
                        display_image.image = Y22render
                    else:
                        display_image.configure(image = Y2render)
                        display_image.image = Y2render
                    window.update()
                elif letter == 'z':
                    if double:
                        display_image.configure(image = Z22render)
                        display_image.image = Z22render
                    else:
                        display_image.configure(image = Z2render)
                        display_image.image = Z2render
                    window.update()

            elif r == 1:
                if letter == 'a':
                    if double:
                        display_image.configure(image = A12render)
                        display_image.image = A12render
                    else:
                        display_image.configure(image = A1render)
                        display_image.image = A1render
                    window.update()

                elif letter == 'b':
                    if double:
                        display_image.configure(image = B12render)
                        display_image.image = B12render
                    else:
                        display_image.configure(image = B1render)
                        display_image.image = B1render
                    window.update()
                elif letter == 'c':
                    if double:
                        display_image.configure(image = C12render)
                        display_image.image = C12render
                    else:
                        display_image.configure(image = C1render)
                        display_image.image = C1render
                    window.update()
                elif letter == 'd':
                    if double:
                        display_image.configure(image = D12render)
                        display_image.image = D12render
                    else:
                        display_image.configure(image = D1render)
                        display_image.image = D1render
                    window.update()
                elif letter == 'e':
                    if double:
                        display_image.configure(image = E12render)
                        display_image.image = E12render
                    else:
                        display_image.configure(image = E1render)
                        display_image.image = E1render
                    window.update()
                elif letter == 'f':
                    if double:
                        display_image.configure(image = F12render)
                        display_image.image = F12render
                    else:
                        display_image.configure(image = F1render)
                        display_image.image = F1render
                    window.update()
                elif letter == 'g':
                    if double:
                        display_image.configure(image = G12render)
                        display_image.image = G12render
                    else:
                        display_image.configure(image = G1render)
                        display_image.image = G1render
                    window.update()
                elif letter == 'h':
                    if double:
                        display_image.configure(image = H12render)
                        display_image.image = H12render
                    else:
                        display_image.configure(image = H1render)
                        display_image.image = H1render
                    window.update()
                elif letter == 'i':
                    if double:
                        display_image.configure(image = I12render)
                        display_image.image = I12render
                    else:
                        display_image.configure(image = I1render)
                        display_image.image = I1render
                    window.update()
                elif letter == 'j':
                    if double:
                        display_image.configure(image = J12render)
                        display_image.image = J12render
                    else:
                        display_image.configure(image = J1render)
                        display_image.image = J1render
                    window.update()
                elif letter == 'k':
                    if double:
                        display_image.configure(image = K12render)
                        display_image.image = K12render
                    else:
                        display_image.configure(image = K1render)
                        display_image.image = K1render
                    window.update()
                elif letter == 'l':
                    if double:
                        display_image.configure(image = L12render)
                        display_image.image = L12render
                    else:
                        display_image.configure(image = L1render)
                        display_image.image = L1render
                    window.update()
                elif letter == 'm':
                    if double:
                        display_image.configure(image = M12render)
                        display_image.image = M12render
                    else:
                        display_image.configure(image = M1render)
                        display_image.image = M1render
                    window.update()
                elif letter == 'n':
                    if double:
                        display_image.configure(image = N12render)
                        display_image.image = N12render
                    else:
                        display_image.configure(image = N1render)
                        display_image.image = N1render
                    window.update()
                elif letter == 'o':
                    if double:
                        display_image.configure(image = O12render)
                        display_image.image = O12render
                    else:
                        display_image.configure(image = O1render)
                        display_image.image = O1render
                    window.update()
                elif letter == 'p':
                    if double:
                        display_image.configure(image = P12render)
                        display_image.image = P12render
                    else:
                        display_image.configure(image = P1render)
                        display_image.image = P1render
                    window.update()
                elif letter == 'q':
                    if double:
                        display_image.configure(image = Q12render)
                        display_image.image = Q12render
                    else:
                        display_image.configure(image = Q1render)
                        display_image.image = Q1render
                    window.update()
                elif letter == 'r':
                    if double:
                        display_image.configure(image = R12render)
                        display_image.image = R12render
                    else:
                        display_image.configure(image = R1render)
                        display_image.image = R1render
                    window.update()
                elif letter == 's':
                    if double:
                        display_image.configure(image = S12render)
                        display_image.image = S12render
                    else:
                        display_image.configure(image = S1render)
                        display_image.image = S1render
                    window.update()
                elif letter == 't':
                    if double:
                        display_image.configure(image = T12render)
                        display_image.image = T12render
                    else:
                        display_image.configure(image = T1render)
                        display_image.image = T1render
                    window.update()
                elif letter == 'u':
                    if double:
                        display_image.configure(image = U12render)
                        display_image.image = U12render
                    else:
                        display_image.configure(image = U1render)
                        display_image.image = U1render
                    window.update()
                elif letter == 'v':
                    if double:
                        display_image.configure(image = V12render)
                        display_image.image = V12render
                    else:
                        display_image.configure(image = V1render)
                        display_image.image = V1render
                    window.update()
                elif letter == 'w':
                    if double:
                        display_image.configure(image = W12render)
                        display_image.image = W12render
                    else:
                        display_image.configure(image = W1render)
                        display_image.image = W1render
                    window.update()
                elif letter == 'x':
                    if double:
                        display_image.configure(image = X12render)
                        display_image.image = X12render
                    else:
                        display_image.configure(image = X1render)
                        display_image.image = X1render
                    window.update()
                elif letter == 'y':
                    if double:
                        display_image.configure(image = Y12render)
                        display_image.image = Y12render
                    else:
                        display_image.configure(image = Y1render)
                        display_image.image = Y1render
                    window.update()
                elif letter == 'z':
                    if double:
                        display_image.configure(image = Z12render)
                        display_image.image = Z12render
                    else:
                        display_image.configure(image = Z1render)
                        display_image.image = Z1render
                    window.update()







    display_image.configure(image = blankRender)
    display_image.image = blankRender
    window.update()















