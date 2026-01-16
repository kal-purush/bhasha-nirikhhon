var hitung = function(nilai)
{
    if(nilai <= 60){
        return "KURANG"
    } else if(nilai >60 && nilai <= 70){
        return "CUKUP"
    } else if(nilai >70 && nilai <= 80){
        return "BAIK"
    } else {
        return "LUAR BIASA"
    }
}

console.log(hitung(50),", ",hitung(60),", ",hitung(70),", ",hitung(80),", ",hitung(90))