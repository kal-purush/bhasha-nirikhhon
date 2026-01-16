import csv
import random

racers = ["Morten", "Madis", "Bryan", "Kenno", "Ilusspoiss"]
alg, lopp = 23, 26
times = []


def suvalineaeg(a, b):
    aeg = round(random.uniform(a, b), 3)
    return aeg


def taisringi_aeg():
    total = 0
    lap_times = []
    sector_times = []
    errors = []

    for lap in range(1, 11):
        s1 = suvalineaeg(alg, lopp)
        s2 = suvalineaeg(alg, lopp)
        s3 = suvalineaeg(alg, lopp)

        number = random.randint(1, 10)
        if number == 2:
            s1 = suvalineaeg(30, 90)
            s2 = suvalineaeg(30, 90)
            s3 = suvalineaeg(30, 90)
            errors.append("True")
        else:
            errors.append("False")

        lap_time = s1 + s2 + s3
        total += lap_time
        lap_times.append(lap_time)
        sector_times.append([s1, s2, s3])

    return total, lap_times, sector_times, errors


with open("race_results.csv", mode="w", newline="") as file:
    writer = csv.writer(file, delimiter=";")
    writer.writerow(["Ring", "Nimi", "Aeg", "Sektor1", "Sektor2", "Sektor3", "Viga"])

    for racer in racers:
        total_time, lap_times, sector_times, errors = taisringi_aeg()
        for i, (lap_time, sector, error) in enumerate(zip(lap_times, sector_times, errors), start=1):
            s1, s2, s3 = sector
            formatted_time = "{:.3f}".format(lap_time)  # Formaatida aeg kolme kohaga peale koma
            writer.writerow([i, racer, formatted_time, s1, s2, s3, error])

print("Andmed on salvestatud CSV-faili 'race_results.csv'.")