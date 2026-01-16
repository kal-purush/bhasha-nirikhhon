from dataclasses import dataclass

@dataclass
class Volo:
    _aeroportoPartenza: int
    _aeroportoArrivo: int
    _distanza: int

    @property
    def aeroportoPartenza(self):
        return self._aeroportoPartenza
    
    @property
    def aeroportoArrivo(self):
        return self._aeroportoArrivo
    
    @property
    def distanza(self):
        return self._distanza

    def __str__(self):
        return f"Volo da {self._aeroportoPartenza} a {self._aeroportoArrivo} in {self._distanza} miglia"

    def __hash__(self):
        return hash(self._aeroportoPartenza+self._aeroportoArrivo)