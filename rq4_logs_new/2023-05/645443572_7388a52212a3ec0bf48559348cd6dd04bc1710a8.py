import random
import matplotlib.pyplot as plt
import numpy as np

# Liczba graczy i rund
num_players = 10
num_rounds = 10

# Inicjalizacja monet dla każdego gracza
players = {i: [100] for i in range(num_players)}

# Inicjalizacja kolorów dla wykresu
colors = plt.cm.rainbow(np.linspace(0, 1, num_players))

# Inicjalizacja danych dla wykresu
data = {i: [] for i in range(num_players)}

# Symulacja gry
for round in range(num_rounds):
    for player in range(num_players):
        # Losowanie mnożnika
        multiplier = random.choice([0.5, 1.8])

        # Aktualizacja liczby monet dla gracza
        players[player].append(players[player][-1] * multiplier)

        # Dodanie danych do wykresu
        data[player].append(players[player][-1])

# Obliczanie najmniejszej, największej i średniej wygranej dla wszystkich graczy po 10 rundach
min_winnings = np.min([players[player][-1] for player in range(num_players)])
max_winnings = np.max([players[player][-1] for player in range(num_players)])
avg_winnings = np.mean([players[player][-1] for player in range(num_players)])

# Tworzenie wykresu
fig, ax = plt.subplots()
for player in range(num_players):
    ax.plot(range(num_rounds), data[player], label=f'Gracz {player+1}', color=colors[player])

# Obliczanie ilości możliwych ścieżek do najmniejszej, średniej i największej wygranej
num_paths_to_min = 2 ** num_rounds
num_paths_to_max = 2 ** num_rounds
num_paths_to_avg = 2 ** num_rounds

# Dodanie wartości najmniejszej, największej i średniej wygranej na osi y jako etykiety
ax.axhline(min_winnings, linestyle='--', color='black', alpha=0.5)
ax.text(num_rounds - 0.5, min_winnings, f'Najmniejsza: {min_winnings:.2f} ({num_paths_to_min} ścieżek)', fontsize=8, color='black', ha='right')
ax.axhline(max_winnings, linestyle='--', color='black', alpha=0.5)
ax.text(num_rounds - 0.5, max_winnings, f'Największa: {max_winnings:.2f} ({num_paths_to_max} ścieżek)', fontsize=8, color='black', ha='right')
ax.axhline(avg_winnings, linestyle='--', color='black', alpha=0.5)
ax.text(num_rounds - 0.5, avg_winnings, f'Średnia: {avg_winnings:.2f} ({num_paths_to_avg} ścieżek)', fontsize=8, color='black', ha='right')

# Ustawienie zakresu osi y od 100 do największej wygranej
ax.set_ylim(100, max_winnings)

# Pozostała część kodu pozostaje bez zmian

ax.set_xlabel('Rundy')
ax.set_ylabel('Liczba monet')
ax.legend()
ax.set_title('Symulacja gry')

# Dodanie siatki na wykresie
ax.grid(True)

# Dodanie liczb rund na osi x jako etykiety
xticks = range(num_rounds)
ax.set_xticks(xticks)
ax.set_xticklabels([str(x+1) for x in xticks])

plt.show()