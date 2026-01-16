#!/data/data/com.termux/files/usr/bin/bash

echo -e "\e[031m" "──────▄▀▄─────▄▀▄"
echo -e "\e[031m" "─────▄█░░▀▀▀▀▀░░█▄"
echo -e "\e[031m" "─▄▄──█░░░░░░░░░░░█──▄▄"
echo -e "\e[031m" "█▄▄█─█░░▀░░┬░░▀░░█─█▄▄█"
echo -e "\e[036m" "╔════════════════════════════════════════╗"                                                                          
echo -e "\e[032m" "║ ♚ Project Name : Download YouTube™           ║"
echo -e "\e[032m" "║ ♚ Author :       ARYAN    ║"
echo -e "\e[032m" "║ ♚ Github : g̲i̲t̲h̲u̲b̲.̲ ARYAN/̲    ║"
echo -e "\e[032m" "║ ♚ whatsap 0788087851       ║"
echo -e "\e[036m" "╠════════════════════════════════════════╝"
echo -e "\e[036m" "╠═▶ 𝗦𝗲𝗹𝗲𝗰𝘁 𝗔 𝗙𝗼𝗿𝗺𝗮𝘁 ➳"
echo -e "\e[032m" "╠═▶ 1. Music Mp3♫"
echo -e "\e[032m" "╠═▶ 2. Video 360p"
echo -e "\e[032m" "╠═▶ 3. Video 480p"
echo -e "\e[032m" "╠═▶ 4. Video 720p"
echo -e "\e[032m" "╠═▶ 5. Video 1080p"

command='-no-mtime -o /data/data/com.termux/files/home/storage/shared/Youtube/%(title)s.%(ext)s -f'
read -p " ╚═➤ " option

if [ "$option" -eq "1" ]
then
    echo "$command 140" > ~/.config/youtube-dl/config
    youtube-dl $1

elif [ "$option" -eq "2" ]
then
    echo "$command \"best[height<=360]\"" > ~/.config/youtube-dl/config
    youtube-dl $1

elif [ "$option" -eq "3" ]
then
    echo "$command \"best[height<=480]\"" > ~/.config/youtube-dl/config
    youtube-dl $1

elif [ "$option" -eq "4" ]
then
    echo "$command \"best[height<=720]\"" > ~/.config/youtube-dl/config
    youtube-dl $1

elif [ "$option" -eq "5" ]
then
    echo "$command \"best[height<=1080]\"" > ~/.config/youtube-dl/config
    youtube-dl $1
fi
ho "$command \"best[height<=1080]\"" > ~/.config/youtube-dl/config
    youtube-dl $1
fi