import os

if os.path.exists("final_result.txt"):
    os.remove("final_result.txt")

list_score = []

with open("private_score.txt", "r") as f:
    line = f.read()
    list_score.append((line.strip().split("\n")))

with open("final_result.txt", "w") as f:
    f.write("fname,liveness_score\n")
    f.write("1305.mp4,0.0\n")

with open("final_result.txt", "a") as f:
    for line in list_score[0]:
        video_name, score = line.split()[:2]
        video_name = video_name.split("_")[0] + ".mp4"
        # score = str(int(float(score)))
        r = ",".join([video_name, score])
        f.write(f"{r}\n")