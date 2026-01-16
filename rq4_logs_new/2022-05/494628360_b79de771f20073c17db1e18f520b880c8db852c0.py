from openpydss.opendss.model.dssobject import DSSObject


class Capacitor(DSSObject):
    def __init__(
            self,
            bus1="capacitor_1",
            bus2="capacitor_1.0.0.0",
            phases="3",
            kvar="[ 1200]",
            kv="12.47",
            conn="wye",
            cmatrix="",
            cuf="[ 20.47]",
            R="[ 0]",
            XL="[ 0]",
            Harm="[ 0]",
            Numsteps="1",
            states="[ 1]",
            normamps="75.0046059412345",
            emergamps="100.006141254979",
            faultrate="0",
            pctperm="1E2",
            repair="3",
            basefreq="60",
            enabled="true",
            like="",
    ):
        super().__init__()
        self.bus1 = bus1
        self.bus2 = bus2
        self.phases = phases
        self.kvar = kvar
        self.kv = kv
        self.conn = conn
        self.cmatrix = cmatrix
        self.cuf = cuf
        self.R = R
        self.XL = XL
        self.Harm = Harm
        self.Numsteps = Numsteps
        self.states = states
        self.normamps = normamps
        self.emergamps = emergamps
        self.faultrate = faultrate
        self.pctperm = pctperm
        self.repair = repair
        self.basefreq = basefreq
        self.enabled = enabled
        self.like = like