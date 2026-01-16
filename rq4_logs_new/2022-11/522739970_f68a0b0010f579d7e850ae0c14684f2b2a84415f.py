class Curso:

    def  __INIT__(self, codigoCurso, nomeCurso, tempoIntegralizacao, cargaHoraria, turma):
        self.codigoCurso  = codigoCurso
        self.nomeCurso = nomeCurso
        self.tempoIntegralizacao= tempoIntegralizacao
        self.cargaHoraria = cargaHoraria
        self.turma = turma
    def getCodigoCurso(self):
        return self.codigoCurso
    
    def setCodigoCurso(self, codigoCurso):
        self.codigoCurso=codigoCurso
    