import Gadget2Snapshot as gs
import numpy as np
import unittest

class Gadget2SnapshotTests(unittest.TestCase):
    """
    Unit test on readSnapshot and writeSnapshot.
    Input file: Gadget2.0.7/ICs/galaxy_littleendian.dat

    """

    def test_readSnapshot(self):
        galaxy = gs.Gadget2Snapshot.open('galaxy_littleendian.dat')
        galaxy.readSnapshot()
        galaxy.close()


    def test_writeSnapshot(self):
        galaxy = gs.Gadget2Snapshot.open('galaxy_littleendian.dat')
        galaxy.readSnapshot(readvel=True)

        # Change the 1st position to (0, 0, 0)

        galaxy.positions[0] = np.array([0, 0, 0])
        galaxy.writeSnapshot('galaxy_unittest', HubbleParam=0)
        galaxy_test = gs.Gadget2Snapshot.open('galaxy_unittest')
        self.assertEqual(galaxy_test.getHeader()['HubbleParam'], 0)
        self.assertListEqual(list(galaxy_test.getPositions(save=False)[0]), [0, 0, 0])
        galaxy_test.close()


    def test_scatterXYZ(self):
        galaxy = gs.Gadget2Snapshot.open('galaxy_littleendian.dat')
        galaxy.scatterXYZ()
        galaxy.close()


if __name__ == '__main__':
    unittest.main(exit=False)