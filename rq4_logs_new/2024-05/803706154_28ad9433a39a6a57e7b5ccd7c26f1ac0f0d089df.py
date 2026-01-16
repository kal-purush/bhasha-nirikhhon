import math

# Öklid mesafesini hesaplayan fonksiyon
def euclideanDistance(point1, point2):
    return math.sqrt((point2[0] - point1[0])**2 + (point2[1] - point1[1])**2)

# Noktaları içeren liste
points = [(1, 2), (4, 6), (7, 8), (2, 1)]

# Mesafeleri saklayacak liste
distances = []

# Her nokta çifti arasındaki mesafeleri hesapla ve distances listesine ekle
for i in range(len(points)):
    for j in range(i + 1, len(points)):
        dist = euclideanDistance(points[i], points[j])
        distances.append(dist)

# Minimum mesafeyi bul
min_distance = min(distances)

print("Her nokta çifti arasındaki mesafeler:", distances)
print("Minimum mesafe:", min_distance)