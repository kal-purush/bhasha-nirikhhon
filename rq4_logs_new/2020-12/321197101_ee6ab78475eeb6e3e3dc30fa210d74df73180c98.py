"""Ler os dados das cenas."""
import json


class Cenas:
    def __init__(self, file):
        self.file = file
        self.dados = self.ler_cenas()

    def ler_cenas(self):
        """Ler o ficheiro que contem as cenas do jogo.
        :args file: ficheiro que contem as cenas
        :return: Dict
        """
        with open(self.file, 'r') as cenas_file:
            data = json.load(cenas_file)
            return data

    def get_cenas(self):
        """Guardar as cenas que existe no ficheiro(`dados`).
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)

        :return: Generator
        """
        return (cenas for cenas in self.dados.keys())

    def get_contexto(self, cena):
        """Guardar os contextos das cenas.
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)
            cena: cenas do jogo (`get_cenas`)

        :return: str
        """

        return self.dados[cena]['contexto']

    def get_dilema(self, cena):
        """Guardar os dilemas das cenas.
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)
            cena: cenas do jogo (`get_cenas`)

        :return: str
        """
        return self.dados[cena]['dilema']

    def get_opcao(self, cena):
        """Guardar os opção das cenas.
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)
            cena: cenas do jogo (`get_cenas`)

        :return: list
        """
        opcao_1 = self.dados[cena]['opções']['opção_1']
        opcao_2 = self.dados[cena]['opções']['opção_2']
        return [opcao_1, opcao_2]

    def get_proxima_cena(self, cena):
        """Pega a prixima cena da `cena`.
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)
            cena: cenas do jogo (`get_cenas`)

        :return: list
        """
        return self.dados[cena]['proxima_cena']

    def get_fim_cena(self, cena):
        """Pega o fim da `cena`.
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)
            cena: cenas do jogo (`get_cenas`)

        :return: list
        """
        return self.dados[cena]['fim_cena']

    def show_data(self):
        """Agrupar todos os dados das cenas.
        :args
            dados: ficheiro com os dados das cenas (`ler_cenas`)
            cena: cenas do jogo (`get_cenas`)

        :return: dict
        """
        dados_da_cena = {}
        for i in self.get_cenas():
            dados_da_cena[f'contexto-{i}'] = self.get_contexto(i)
            dados_da_cena[f'dilema-{i}'] = self.get_dilema(i)
            dados_da_cena[f'opção-{i}'] = self.get_opcao(i)
            dados_da_cena[f'proxima_cena-{i}'] = self.get_proxima_cena(i)
            dados_da_cena[f'fim_cena-{i}'] = self.get_fim_cena(i)
        return dados_da_cena


if __name__ == "__main__":
    dados = Cenas("cenas.json")
    dado = dados.show_data()
    if dado is not None:
        print(dado)