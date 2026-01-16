import pyproj

proj_ecef = pyproj.Proj(proj='geocent', ellps='WGS84', datum='WGS84')
proj_lla = pyproj.Proj(proj='latlong', ellps='WGS84', datum='WGS84')


def ecef_to_lla(xyz):
    x, y, z = xyz
    long, lat, alt = pyproj.transform(
        proj_ecef, proj_lla, x, y, z, radians=False)
    return (lat, long, alt)


def lla_to_ecef(lla):
    lat, long, alt = lla
    return pyproj.transform(proj_lla, proj_ecef, long, lat, alt, radians=False)