import numpy as np

def cpsnr_metric(sr, hr):
	"""
	Clear Peak Signal-to-Noise Ratio. The PSNR score, adjusted for brightness and other volatile features, e.g. clouds.
	Args:
		sr: numpy.ndarray (n, m), super-resolved image
		hr: numpy.ndarray (n, m), high-res ground-truth image
		hr_map: numpy.ndarray (n, m), status map of high-res image, indicating clear pixels by a value of 1
	Returns:
		cPSNR: float, score
	"""
	#hr_map = np.ones((hr.shape[0], hr.shape[1], hr.shape[2]))

	if len(sr.shape) == 2:
		sr = sr[None, ]
		hr = hr[None, ]
		#hr_map = hr_map[None, ]

	if sr.dtype.type is np.uint16:  # integer array is in the range [0, 65536]
		sr = sr / np.iinfo(np.uint16).max  # normalize in the range [0, 1]
	else:
		assert 0 <= sr.min() and sr.max() <= 1, 'sr.dtype must be either uint16 (range 0-65536) or float64 in (0, 1).'
	if hr.dtype.type is np.uint16:
		hr = hr / np.iinfo(np.uint16).max

	n_clear = hr.shape[0] * hr.shape[0]
	#n_clear = np.sum(hr_map, axis=(1, 2))  # number of clear pixels in the high-res patch
	diff = hr - sr
	#bias = np.sum(diff * hr_map, axis=(1, 2)) / n_clear  # brightness bias
	bias = np.sum(diff, axis=(0, 1)) / n_clear

	cMSE = []
	for i in range(bias.shape[0]):
		cMSE.append(np.sum(np.square((diff[:,:,i] - bias[i])), axis=(0, 1)) / n_clear)

	#cMSE = np.sum(np.square((diff - bias[:, None, None])), axis=(0, 1)) / n_clear
	cPSNR = -10 * np.log10(cMSE)  # + 1e-10)

	cPSNR = np.mean(cPSNR)

	return cPSNR