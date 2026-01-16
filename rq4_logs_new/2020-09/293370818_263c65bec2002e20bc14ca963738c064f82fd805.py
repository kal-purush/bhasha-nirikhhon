import numpy as np

from utils import get_msb, clear_k_bit, bit_count, kth_bit_set


class PyAES:
    mx = 0x1b

    def ff_add(self, a1: int, a2: int) -> int:
        """Performs finite field addition of two integers.

        If a1 and a2 aren't integers, try to cast into base16 integer.
        Addition of two elements in a finite field is achieved by adding
        the coefficients for the corresponding powers in the polynomial for
        the two elements. Addition can be performed with XOR (i.e. modulo 2).
        Identical to polynomial subtraction.

        Example
        -------
        x6 is the same as x^6.
        (x6 + x4 + x2 + x + 1) + (x7 + x + 1) = x7 + x6 + x4 + x2
        {01010111}             + {10000011}   = {11010100}
        {0x57}               XOR {0x83}       =  {0xd4}

        Parameters
        ----------
        :param a1: addend 1
        :param a2: addend 2
        :return: a1 XOR a2
        """

        return a1 ^ a2  # FF addition and subtraction with XOR

    def xtime(self, n: int) -> int:
        """Performs finite field multiplication by x.

        Multiplying the binary polynomial (b) by polynomial x results in
        *(xn == x^n)
        b7x8 + b6x7 + b5x6 + b4x5 + b3x4 + b2x3 + b1x2 + b0x

        The result of x * b(x) is obtained by reducing the above result modulo x.
        If b7 = 0, the result is already reduced. If b7 = 1, the result must be XOR
        with polynomial m(x).

        It  follows  that  multiplication  by  x  (i.e.,{00000010}  or  {02})  can
        be  implemented  at  the  byte  level  as  a  left  shift  and  a  subsequent
        conditional  bitwise  XOR  with  {1b}

        Multiplication by higher powers of x can be implemented by repeated application
        of xtime(). By adding intermediate results, multiplication by any constant can
        be implemented.

        :param n: binary polynomial to be multiplied by x
        :return: n(x) * x
        """
        p = n << 1  # Multiply n by x (ie 00000010 or 02)
        if get_msb(p) <= 128:  # If the MSB is the 8th bit or lower
            return p  # Return because we're already reduced.
        else:
            p1 = p ^ self.mx  # Reduce by XOR the polynomial m(x) = x^8 + x^4 + x^3 + x + 1
            return clear_k_bit(9, p1)  # Drop the 9th bit and return

    def ff_multiply(self, f1: int, f2: int) -> int:
        """Performs finite filed multiplication between two numbers.

        Multiplication in GF(2^8) corresponds  with  the multiplication of polynomials
        modulo an irreducible polynomial of degree 8 (i.e. mx(x)). This can be implemented
        using xtime() and adding intermediate results.

        :param f1: factor1
        :param f2: factor2
        :return: f1 * f2 GF(2^8)
        """

        # Optimized by choosing the factor with fewer bytes as multiplier
        if bit_count(f1) < bit_count(f2):
            f1 = f1 ^ f2
            f2 = f1 ^ f2
            f1 = f1 ^ f2

        # Make the table with all the xtime values for f1
        xtime_table = self.make_xtime_table(f1, f2)
        product = 0
        # Check each bit of f2
        for k in range(8):
            # If k bit of f2 is set, that represent a polynomial value of x^k
            if kth_bit_set(k, f2):
                # Add it to the product
                product = self.ff_add(product, xtime_table[k])
        # Return the product
        return product

    def make_xtime_table(self, f1: int, f2: int) -> list:
        """Creates a table of xtime results.

        When multiplying by higher powers of x, the product is found by
        adding together intermediate xtime results.

        :param f1: factor
        :param f2: factor
        :return: a list of xtime results
        """

        # Xtime table will have 8 elements, corresponding with 8 bits
        xtime_table = [0] * 63
        # First element is f1 multiplicand
        xtime_table[0] = f1
        p = f1
        # Keep track of where to place the xtime result in the table
        idx = 1
        # Compute Xtime until f2 multiplier can't be divided anymore
        while f2 != 1:
            # Compute xtime of number
            p = self.xtime(p)
            # Add it to table at correct index
            xtime_table[idx] = p
            # Increment index
            idx += 1
            # Divide f2
            f2 >>= 1
        # Return the xtime table
        return xtime_table

    ax = np.array([  # a(x) = {03}x^3 + {01}x^2 + {01}x + {02}
        [0x02, 0x03, 0x01, 0x01],
        [0x01, 0x02, 0x03, 0x01],
        [0x01, 0x01, 0x02, 0x03],
        [0x03, 0x01, 0x01, 0x02],
    ])

    def sub_bytes(self):
        """

        sub_bytes() is a non-linear byte substitution that operates
        independently on each byte of the State using a substitution
        table (S-box).

        This S-box, which is invertible, is constructed by composing
        two transformations:
            1. Take the multiplicative inverse of GF(2^8); the element
            {00} is mapped to itself.
            2. Apply the following affine transformation (over GF(2)):
                * See FIPS197
        :return:
        """
        pass

    def shift_rows(self):
        """

        The bytes in the last three rows of the state are cyclically shifted
        over different number of bytes (offsets). The first row, r = 0, is not
        shifted.

        :return:
        """
        pass

    def mix_columns(self, s):
        """Transforms the state matrix column-by-column.

        The mix_columns() transformation operates on the State
        column-by-column, treating each column as a four-term
        polynomial.

        Columns are considered as polynomial over
        GF(2^8) and multiplied modulo x^4 + 1 with a(x) where
        a(x) = {03}x^3 + {01}x^2 + {01}x + {02}

        This can be seen as matrix multiplication; let
            s'(x) = a(x) ffmult s(x)

        :param s: 4 x 4 state matrix
        :return: None, modifies original matrix to be s'(x)
        """

        for col in range(4):
            # Calculate value for each element in s column and return s' column
            s_col = s[:, col]
            s_p_col = self.mix_column(s_col)
            # Replace s column with s' column
            s[:, col] = s_p_col

    def mix_column(self, col):
        """Transforms each column item by multiply modulo x4 + 1 with a(x)

        This is a helper method for mix_columns(). The arithmetic
        is done here. For each item in the column, calculate the
        transformed value by multiplying each column element by
        each row element of a(x). The column elements can be
        thought of as row index for a(x).

        :param col: a list of state column values
        :return: a list of transformed state column values
        """

        transformed_bytes = []
        # Unpack the state column values
        s0, s1, s2, s3 = col[0]

        # We need to calculate each element in col using all elements in col
        for i in range(4):
            # s'(x) = a(x) x s(x)
            a0, a1, a2, a3 = self.ax[i]
            # a0, a1, a2, a3 = map(int, self.ax[i])
            transformed_bytes.append(
                self.ff_add(
                    self.ff_add(
                        self.ff_multiply(s0, a0),
                        self.ff_multiply(s1, a1)
                    ),
                    self.ff_add(
                        self.ff_multiply(s2, a2),
                        self.ff_multiply(s3, a3)
                    )
                )
            )
        return transformed_bytes

    def add_round_key(self):
        """

        A Round Key is added to the State by a simple bitwise XOR operation.
        Each Round Key consists of Nb words from the key schedule. Those Nb words
        are each added into the columns of the state.

        :return:
        """
        pass

    def inv_sub_bytes(self):
        pass

    def inv_shift_rows(self):
        pass

    def inv_mix_columns(self):
        pass

    def sub_word(self):
        """

        Takes four-byte input word and applies the S-box to each
        of the four bytes to produce an output word.

        :return:
        """
        pass

    def rot_word(self):
        """

        Takes a word[a0, a1, a2, a3] as input, performs a cyclic permutation,
        then returns the word[a1, a2, a3, a0]

        :return:
        """
        pass