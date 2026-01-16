# coding: utf-8
from helpers import *
import argparse


N_PARAM = 3
SCALE_VAL = 100.


class Model(object):
    def __init__(self, pwm, mu, encoding='T', scale=SCALE_VAL):
        self.pwm = pwm  # type: np.ndarray
        self.mu = mu
        self.width = self.pwm.size / N_PARAM
        self.encoding = encoding
        self.enc = Encoder(self.width, encoding)
        self.scale = scale

    def predict(self, seqs, noise_std=None):
        return self.predict_helper(self.encode(seqs), noise_std)

    def predict_helper(self, codes, noise_std=None):
        """
        :type codes: np.ndarray
        :param noise_std: None | float
        :rtype: np.ndarray
        """
        noise = random.normal(0., noise_std, len(codes)) if noise_std else 0.
        energy = np.dot(codes, self.pwm) - self.mu + noise
        return self.scale / (1. + np.exp(energy))

    def encode(self, seqs):
        return np.asarray([self.enc.Encode(seq) for seq in seqs])

    @property
    def pwm(self):
        return self._pwm

    @pwm.setter
    def pwm(self, pwm):
        self._pwm = np.array(pwm)

    def get_ppm(self, seqs, lim=0, weighted=True, noise_std=None):
        counts = self.predict(seqs, noise_std)  # type: np.ndarray
        clean_counts = self.predict(seqs)  # w/o noise

        ind = np.argsort(counts)[-lim:]
        weights = counts[ind] if weighted else np.ones_like(ind)
        seqs_lim = [seqs[i] for i in ind]
        ppm = PFM.Normalize(
            PFM.Normalize(Freq(seqs_lim, weights).GetPFM()), pseudo=1e-6
        )

        x0 = -np.asarray(PFM.Shrink(ppm, self.encoding))
        self.regress(seqs_lim, counts[ind], x0)

        return ppm, counts, clean_counts

    def regress(self, seqs, counts, x0, repeats=3):
        def set_param(x):
            self.pwm = x[:-2]
            self.mu = x[-2]
            self.scale = 10 ** x[-1]

        codes = self.encode(seqs)
        log_counts = np.log(counts)

        def obj(x):
            set_param(x)
            d = np.log(self.predict_helper(codes)) - log_counts
            return np.dot(d, d)

        x_0 = np.hstack((x0, [0., np.log10(SCALE_VAL)]))
        ub = [10.] * self.pwm.size + [10., np.log10(SCALE_VAL) + 1]
        lb = [0.] * self.pwm.size + [-10., np.log10(SCALE_VAL) - 1]

        res = None
        for _ in xrange(repeats):
            tmp = optimize.minimize(
                obj, x_0 + random.normal(scale=.2, size=len(x_0)),
                method='L-BFGS-B', bounds=zip(lb, ub)
            )
            if res is None or tmp.fun < res.fun:
                res = tmp

        set_param(res.x)


def process_cnts(cnts, cnts2):
    """
    :type cnts: np.ndarray
    :type cnts2: np.ndarray
    """
    ind = np.argsort(cnts)
    x = cnts[ind] / cnts.max()
    y = cnts2[ind] / cnts2.max()
    return x, y


def diff(cnts, cnts2, n_top):
    x, y = process_cnts(cnts, cnts2)  # type: np.ndarray
    r2 = stats.spearmanr(x, y)[0] ** 2
    r2_top = stats.pearsonr(
        np.arange(n_top), y.argsort().argsort()[-n_top:]
    )[0] ** 2
    return r2, r2_top


def random_pwm(loc, scale, width):
    pwm = np.maximum(random.normal(loc, scale, width * N_PARAM), 0.)
    encoding = ''.join(random.choice(BASES, width))
    return pwm, encoding


def main(width, mu_, seqs, loc, scale, noise_std, out_path, pct=.01):
    pwm, encoding = random_pwm(loc, scale, width)
    n_top = int(pct * len(seqs))

    def helper(mu, lim, weighted):
        model = Model(pwm, mu, encoding)

        ppm, _, cnts_clean = model.get_ppm(
            seqs, lim, weighted, noise_std
        )
        cnts_ref = cnts_clean  # type: np.ndarray

        pred, _ = PFM.Predictor(ppm)
        cnts_pfm = np.asarray(map(pred.get, seqs))
        tup = diff(cnts_ref, cnts_pfm, n_top)

        if weighted:
            cnts_phy = model.predict(seqs)  # type: np.ndarray
            tup += diff(cnts_ref, cnts_phy, n_top)

        row = '\t'.join('%.3f' % x for x in tup) + '\n'

        filename = 'results_wgt=%d_lim=%d.txt' % (weighted, bool(lim))
        path = os.path.join(out_path, filename)
        CheckDir(path)
        Write(path, row, 'a')

    helper(mu_, 0, True)
    helper(mu_, n_top, True)
    helper(mu_, n_top, False)


def driver():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--distribution', nargs=2, type=float,
                        default=[2.5, 1.], metavar=('MEAN', 'S.D.'),
                        help='mean and standard deviation of the energy distribution (optional, default: [2.5, 1.0])')
    parser.add_argument('-p', '--potential', type=float, default=-3.,
                        help='chemical potential of the protein (optional, default: -3)')
    parser.add_argument('-n', '--noise', type=float, default=0.,
                        help='standard deviation of noise in kT (optional, default: 0)')
    parser.add_argument('-l', '--length', type=int, default=8,
                        help='motif length (optional, default: 8)')
    parser.add_argument('-s', '--sample', type=int, default=1,
                        help='number of random samples (optional, default: 1)')
    parser.add_argument('-o', '--output', default=os.getcwd(),
                        help='output directory (optional, default: current working directory)')
    parser.add_argument('-v', '--version', action='version', version='1.0.0')
    args = parser.parse_args()

    seqs = Product(BASES, args.length)
    loc, scale = args.distribution
    for _ in xrange(args.sample):
        main(args.length, args.potential, seqs, loc, scale, args.noise,
             args.output)


if __name__ == '__main__':
    driver()