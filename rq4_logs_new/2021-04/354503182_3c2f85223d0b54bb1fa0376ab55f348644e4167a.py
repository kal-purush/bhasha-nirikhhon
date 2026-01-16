from math import isclose

from client.utils import intersect, normal, line_by_two_points, l2_norm
from client.utils import vector_rotation, vector_angle, line_by_vector, clip

EPS = 1e-4


def is_close(a, b):
    return isclose(a, b, abs_tol=EPS, rel_tol=EPS)


def test_vector_rotation():
    d1 = 10
    d2 = -3
    ans = vector_rotation(1.2217, d1, d2)
    ans1 = vector_rotation(2.2217, d1, d2)
    ans2 = vector_rotation(4.2217, d1, d2)
    ans3 = vector_rotation(1.2217, 0, d2)
    assert (is_close(ans[0], 6.2395) and
            is_close(ans[1], 8.3706))
    assert (is_close(ans1[0], -3.6724) and
            is_close(ans1[1], 9.773))
    assert (is_close(ans2[0], -7.3583) and
            is_close(ans2[1], -7.4063))
    assert (is_close(ans3[0], 2.819) and
            is_close(ans3[1], -1.0261))


def test_vector_angle():
    assert (is_close(vector_angle(3, 4, 4, 2), 0.4636))
    assert (is_close(vector_angle(3, -4, 4, 2), 1.7506))
    assert (is_close(vector_angle(3, 0, 4, 2), 0.6435))
    assert (is_close(vector_angle(3, -4, 4, -2), 2.6779))


def test_line_by_vector():
    ans = line_by_vector(0, 0, 1, 1)
    ans1 = line_by_vector(0, 0, 1, -1)
    ans2 = line_by_vector(6, 5, 1, -4)
    ans3 = line_by_vector(6, 5, 1, 0)
    ans4 = line_by_vector(6, 5, 0, 2)
    ans5 = line_by_vector(6, 5, 0, 0)
    assert (is_close(ans[0], -1) and
            is_close(ans[1], 1) and
            is_close(ans[2], 0))
    assert (is_close(ans1[0], 1) and
            is_close(ans1[1], 1) and
            is_close(ans1[2], 0))
    assert (is_close(ans2[0], 4) and
            is_close(ans2[1], 1) and
            is_close(ans2[2], -29))
    assert (is_close(ans3[0], 0) and
            is_close(ans3[1], 1) and
            is_close(ans3[2], -5))
    assert (is_close(ans4[0], 1) and
            is_close(ans4[1], 0) and
            is_close(ans4[2], -6))
    assert (is_close(ans5[0], 1) and
            is_close(ans5[1], 0) and
            is_close(ans5[2], -6))


def test_intersect():
    ans = intersect(1, 2, 3, -4, 5, 0)
    ans1 = intersect(1, 2, 3, -2, 1, -2)
    ans2 = intersect(1, 2, 3, -4, 0, 0)
    try:
        _ = intersect(1, 2, 3, 1, 2, 0)
    except ValueError:
        pass
    assert (is_close(ans[0], -1.1538) and
            is_close(ans[1], -0.923))
    assert (is_close(ans1[0], -1.4) and
            is_close(ans1[1], -0.8))
    assert (is_close(ans2[0], 0) and
            is_close(ans2[1], -1.5))


def test_normal():
    ans = normal(0, 1, 2, 3)
    ans1 = normal(0, 1, 4, 0)
    ans2 = normal(-5, 1, 2, 3)
    assert (is_close(ans[0], -1) and
            is_close(ans[1], 0) and
            is_close(ans[2], 2))
    assert (is_close(ans1[0], -1) and
            is_close(ans1[1], 0) and
            is_close(ans1[2], 4))
    assert (is_close(ans2[0], -1) and
            is_close(ans2[1], -5) and
            is_close(ans2[2], 17))


def test_line_by_two_points():
    ans = line_by_two_points(0, 0, 1, 1)
    ans1 = line_by_two_points(0, 0, 1, -1)
    ans2 = line_by_two_points(6, 5, 1, -4)
    ans3 = line_by_two_points(6, 5, 1, 0)
    ans4 = line_by_two_points(6, 5, 0, 2)
    ans5 = line_by_two_points(6, 5, 0, 0)
    assert (is_close(ans[0], 0) and
            is_close(ans[1], 0) and
            is_close(ans[2], 0))
    assert (is_close(ans1[0], 2) and
            is_close(ans1[1], 0) and
            is_close(ans1[2], 0))
    assert (is_close(ans2[0], 5) and
            is_close(ans2[1], -1) and
            is_close(ans2[2], -29))
    assert (is_close(ans3[0], 1) and
            is_close(ans3[1], -1) and
            is_close(ans3[2], -5))
    assert (is_close(ans4[0], -2) and
            is_close(ans4[1], -1) and
            is_close(ans4[2], 12))
    assert (is_close(ans5[0], 0) and
            is_close(ans5[1], -1) and
            is_close(ans5[2], 0))


def test_l2_norm():
    assert (is_close(l2_norm([3, 4, 4, 2]), 6.7082))
    assert (is_close(l2_norm([3, -4, 54, 2, 5]), 54.4977))


def test_clip():
    assert (is_close(clip(3, 4, 6), 4))
    assert (is_close(clip(4, 4, 6), 4))
    assert (is_close(clip(5, 3, 4), 4))
    try:
        _ = clip(3, 4, 3)
        assert False
    except AssertionError:
        pass