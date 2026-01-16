import csv
from dsmltf import knn_classify, k_neighbours_classify

def load_dataset():
    res = []
    with open("usgs_japan_1968_2018.csv", "r") as f:
        for row in csv.reader(f,delimiter=','):
            try:
                res.append([float(row[1]),float(row[2]),float(row[4])])
            except:
                continue
    return res
        
def main():
    data = load_dataset()

    k0_best = 0
    k1_best = 0
    prew_max_a0 = 0
    prew_max_a1 = 0

    data0 = [(i[:-1],i[-1]) for i in data]
    data1 = [(i[:-1],round(i[-1])) for i in data]
    for k in range(2, 21):
        dict0 = k_neighbours_classify(k,data0[:250])
        dict1 = k_neighbours_classify(k,data1[:250])
        a0 = [dict0[i][0]/dict0[i][1] for i in range(1,k+1)]
        a1 = [dict1[i][0]/dict1[i][1] for i in range(1,k+1)]
        if max(a0) > prew_max_a0 or max(a1) > prew_max_a1:
            k0_best = a0.index(max(a0))+1
            k1_best = a1.index(max(a1))+1


    # Центральная Япония (34-39° с.ш., 136.5-142° в.д.)
    latitude, longitude = map(lambda x: float(x), input("Ведите широту и долготу ").split())
    print(f'Точная магнитуда для к = {k0_best}: {knn_classify(k0_best,data0,(latitude,longitude))}')
    if knn_classify(k1_best,data1,(latitude,longitude)) != round(knn_classify(k0_best,data0,(latitude,longitude))):
        flag = True
        while flag:
            k1_best -= 1
            if knn_classify(k1_best,data1,(latitude,longitude)) == round(knn_classify(k0_best,data0,(latitude,longitude))):
                flag = False
    print(f'Округлённая магнитуда для к = {k1_best}: {knn_classify(k1_best,data1,(latitude,longitude))}')    

if __name__ == "__main__":
    main()