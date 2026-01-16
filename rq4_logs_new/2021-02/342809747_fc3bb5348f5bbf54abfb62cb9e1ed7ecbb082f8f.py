from scapy.sendrecv import sniff


class Capture(object):
    _capture_instance = None  # Instanta clasei Capture.
    packages = None  # Pachetele ce caracterizeaza o captura

    @staticmethod
    def get_instance():
        """
        Metoda statica.
        :return: Instanta clasei Capture
        """
        if Capture._capture_instance is None:
            Capture()
        return Capture._capture_instance

    def __init__(self):
        """
        Constructor ce initializeaza clasa Capture.
        Implementeaza logica Singleton.
        """
        if Capture._capture_instance is not None:
            raise Exception("The Capture class is Singleton. Only one object can be created.")
        else:
            Capture._capture_instance = self

    def sniff_packages(self, filter_name):
        """
        Metoda prin care putem sa facem o captura pe toate interfetele de retea.
        Numarul de pachete este lasat default la 15 de catre mine.
        Un filtru se poate da ca parametru, insa nu este obligatoriu.
        :param filter_name: Numele filtrului. Poate sa fie tcp, udp, https, etc.
        :return: Cele 15 pachete componente ale capturii.
        """
        self.packages = sniff(count=15, filter=filter_name)
        return self.packages