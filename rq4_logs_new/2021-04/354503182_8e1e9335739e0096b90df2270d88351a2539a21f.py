from .utils import vector_rotation, vector_angle, line_by_vector, clip
from .utils import intersect, normal, line_by_two_points, l2_norm
from math import isclose

EPS = 1e-4


def test_vector_rotation():
    d1 = 10
    d2 = -3
    ans = vector_rotation(1.2217, d1, d2)
    ans1 = vector_rotation(2.2217, d1, d2)
    ans2 = vector_rotation(4.2217, d1, d2)
    ans3 = vector_rotation(1.2217, 0, d2)
    assert (isclose(ans[0], 6.2395, abs_tol=EPS) and
            isclose(ans[1], 8.3706, abs_tol=EPS))
    assert (isclose(ans1[0], -3.6724, abs_tol=EPS) and
            isclose(ans1[1], 9.773, abs_tol=EPS))
    assert (isclose(ans2[0], -7.3583, abs_tol=EPS) and
            isclose(ans2[1], -7.4063, abs_tol=EPS))
    assert (isclose(ans3[0], 2.819, abs_tol=EPS) and
            isclose(ans3[1], -1.0261, abs_tol=EPS))


def test_vector_angle():
    assert (isclose(vector_angle(3, 4, 4, 2), 0.4636, abs_tol=EPS))
    assert (isclose(vector_angle(3, -4, 4, 2), 1.7506, abs_tol=EPS))
    assert (isclose(vector_angle(3, 0, 4, 2), 0.6435, abs_tol=EPS))
    assert (isclose(vector_angle(3, -4, 4, -2), 2.6779, abs_tol=EPS))


def test_line_by_vector():
    ans = line_by_vector(0, 0, 1, 1)
    ans1 = line_by_vector(0, 0, 1, -1)
    ans2 = line_by_vector(6, 5, 1, -4)
    ans3 = line_by_vector(6, 5, 1, 0)
    ans4 = line_by_vector(6, 5, 0, 2)
    ans5 = line_by_vector(6, 5, 0, 0)
    assert (isclose(ans[0], -1, abs_tol=EPS) and
            isclose(ans[1], 1, abs_tol=EPS) and
            isclose(ans[2], 0, abs_tol=EPS))
    assert (isclose(ans1[0], 1, abs_tol=EPS) and
            isclose(ans1[1], 1, abs_tol=EPS) and
            isclose(ans1[2], 0, abs_tol=EPS))
    assert (isclose(ans2[0], 4, abs_tol=EPS) and
            isclose(ans2[1], 1, abs_tol=EPS) and
            isclose(ans2[2], -29, abs_tol=EPS))
    assert (isclose(ans3[0], 0, abs_tol=EPS) and
            isclose(ans3[1], 1, abs_tol=EPS) and
            isclose(ans3[2], -5, abs_tol=EPS))
    assert (isclose(ans4[0], 1, abs_tol=EPS) and
            isclose(ans4[1], 0, abs_tol=EPS) and
            isclose(ans4[2], -6, abs_tol=EPS))
    assert (isclose(ans5[0], 1, abs_tol=EPS) and
            isclose(ans5[1], 0, abs_tol=EPS) and
            isclose(ans5[2], -6, abs_tol=EPS))


def test_intersect():
    ans = intersect(1, 2, 3, -4, 5, 0)
    ans1 = intersect(1, 2, 3, -2, 1, -2)
    ans2 = intersect(1, 2, 3, -4, 0, 0)
    try:
        _ = intersect(1, 2, 3, 1, 2, 0)
    except ValueError:
        pass
    assert (isclose(ans[0], -1.1538, abs_tol=EPS) and
            isclose(ans[1], -0.923, abs_tol=EPS))
    assert (isclose(ans1[0], -1.4, abs_tol=EPS) and
            isclose(ans1[1], -0.8, abs_tol=EPS))
    assert (isclose(ans2[0], 0, abs_tol=EPS) and
            isclose(ans2[1], -1.5, abs_tol=EPS))


def test_normal():
    ans = normal(0, 1, 2, 3)
    ans1 = normal(0, 1, 4, 0)
    ans2 = normal(-5, 1, 2, 3)
    assert (isclose(ans[0], -1, abs_tol=EPS) and
            isclose(ans[1], 0, abs_tol=EPS) and
            isclose(ans[2], 2, abs_tol=EPS))
    assert (isclose(ans1[0], -1, abs_tol=EPS) and
            isclose(ans1[1], 0, abs_tol=EPS) and
            isclose(ans1[2], 4, abs_tol=EPS))
    assert (isclose(ans2[0], -1, abs_tol=EPS) and
            isclose(ans2[1], -5, abs_tol=EPS) and
            isclose(ans2[2], 17, abs_tol=EPS))


def test_line_by_two_points():
    ans = line_by_two_points(0, 0, 1, 1)
    ans1 = line_by_two_points(0, 0, 1, -1)
    ans2 = line_by_two_points(6, 5, 1, -4)
    ans3 = line_by_two_points(6, 5, 1, 0)
    ans4 = line_by_two_points(6, 5, 0, 2)
    ans5 = line_by_two_points(6, 5, 0, 0)
    assert (isclose(ans[0], 0, abs_tol=EPS) and
            isclose(ans[1], 0, abs_tol=EPS) and
            isclose(ans[2], 0, abs_tol=EPS))
    assert (isclose(ans1[0], 2, abs_tol=EPS) and
            isclose(ans1[1], 0, abs_tol=EPS) and
            isclose(ans1[2], 0, abs_tol=EPS))
    assert (isclose(ans2[0], 5, abs_tol=EPS) and
            isclose(ans2[1], -1, abs_tol=EPS) and
            isclose(ans2[2], -29, abs_tol=EPS))
    assert (isclose(ans3[0], 1, abs_tol=EPS) and
            isclose(ans3[1], -1, abs_tol=EPS) and
            isclose(ans3[2], -5, abs_tol=EPS))
    assert (isclose(ans4[0], -2, abs_tol=EPS) and
            isclose(ans4[1], -1, abs_tol=EPS) and
            isclose(ans4[2], 12, abs_tol=EPS))
    assert (isclose(ans5[0], 0, abs_tol=EPS) and
            isclose(ans5[1], -1, abs_tol=EPS) and
            isclose(ans5[2], 0, abs_tol=EPS))


def test_l2_norm():
    assert (isclose(l2_norm([3, 4, 4, 2]), 6.7082, abs_tol=EPS))
    assert (isclose(l2_norm([3, -4, 54, 2, 5]), 54.4977, abs_tol=EPS))


def test_clip():
    assert (isclose(clip(3, 4, 6), 4, abs_tol=EPS))
    assert (isclose(clip(4, 4, 6), 4, abs_tol=EPS))
    assert (isclose(clip(5, 3, 4), 4, abs_tol=EPS))
    try:
        _ = clip(3, 4, 3)
    except AssertionError:
        pass