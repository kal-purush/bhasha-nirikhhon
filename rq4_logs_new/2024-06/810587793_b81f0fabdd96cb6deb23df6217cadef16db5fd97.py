import pandas as pd
import matplotlib.pyplot as plt

# Membaca dataset
data = pd.read_csv('bengkel.csv')

# Menampilkan lima baris pertama dataset
print("Lima baris pertama dataset:")
print(data.head())

# Menampilkan informasi umum tentang dataset
print("\nInformasi umum tentang dataset:")
print(data.info())

# Menampilkan ringkasan statistik tentang data numerik
print("\nRingkasan statistik tentang data:")
print(data.describe())

# Menghitung total pendapatan untuk setiap jenis layanan
total_pendapatan_per_jenis = data.groupby('jenis_layanan')['harga'].sum()
print("\nTotal pendapatan untuk setiap jenis layanan:")
print(total_pendapatan_per_jenis)

# Membuat plot jumlah layanan yang diberikan per jenis layanan
plt.figure(figsize=(10, 8))

plt.subplot(2, 2, 1)
jumlah_layanan_per_jenis = data['jenis_layanan'].value_counts()
jumlah_layanan_per_jenis.plot(kind='bar', color='skyblue', edgecolor='black')
plt.title('Jumlah Layanan per Jenis Layanan')
plt.xlabel('Jenis Layanan')
plt.ylabel('Jumlah')

# Membuat scatter plot hubungan antara durasi dan harga
plt.subplot(2, 2, 2)
plt.scatter(data['durasi'], data['harga'], color='blue', alpha=0.5)
plt.title('Scatter Plot Durasi vs Harga')
plt.xlabel('Durasi (menit)')
plt.ylabel('Harga (IDR)')

# Membuat histogram distribusi durasi layanan
plt.subplot(2, 2, 3)
plt.hist(data['durasi'].dropna(), bins=10, color='skyblue', edgecolor='black')
plt.title('Histogram Distribusi Durasi Layanan')
plt.xlabel('Durasi (menit)')
plt.ylabel('Frekuensi')

# Membuat box plot harga layanan
plt.subplot(2, 2, 4)
plt.boxplot(data['harga'].dropna())
plt.title('Box Plot Harga Layanan')
plt.ylabel('Harga (IDR)')

# Menampilkan semua visualisasi dalam satu window
plt.tight_layout()
plt.show()

# Menghitung rata-rata durasi layanan per pelanggan
rata_rata_durasi_per_pelanggan = data.groupby('pelanggan')['durasi'].mean()
print("\nRata-rata durasi layanan per pelanggan:")
print(rata_rata_durasi_per_pelanggan)