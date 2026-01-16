import numpy as np
from typing import List, Tuple

class CyclicCode:
    def __init__(self, generator_polynomial: List[int]):
        self.g = np.array(generator_polynomial)
        self.n = len(generator_polynomial) - 1
        self.code_length = 7  # (7,4) cyclic code
        self.k = 4  # information bits
        
    def poly_to_str(self, coefficients: np.ndarray) -> str:
        """Convert polynomial coefficients to string representation"""
        terms = []
        for i, coef in enumerate(coefficients):
            if coef == 1:
                if i == 0:
                    terms.append("1")
                elif i == 1:
                    terms.append("x")
                else:
                    terms.append(f"x^{i}")
        return " + ".join(reversed(terms)) if terms else "0"

    def polynomial_division(self, dividend: np.ndarray, divisor: np.ndarray) -> tuple:
        """Perform polynomial division in GF(2)"""
        dividend = np.trim_zeros(dividend, 'f')
        divisor = np.trim_zeros(divisor, 'f')
        quotient = np.zeros(len(dividend), dtype=int)
        remainder = dividend.copy()
        
        for i in range(len(dividend) - len(divisor) + 1):
            if remainder[i] == 1:
                quotient[i] = 1
                for j in range(len(divisor)):
                    remainder[i + j] ^= divisor[j]
        
        remainder = np.trim_zeros(remainder, 'f')
        return quotient, remainder

    def calculate_syndrome(self, received: np.ndarray) -> np.ndarray:
        """Calculate syndrome by polynomial division"""
        _, syndrome = self.polynomial_division(received, self.g)
        return np.pad(syndrome, (0, 3 - len(syndrome))) if len(syndrome) > 0 else np.zeros(3, dtype=int)

    def encode_block(self, message_block: str) -> Tuple[str, dict]:
        """Encode a message block using cyclic code"""
        # Convert message to polynomial coefficients
        m = np.array([int(bit) for bit in message_block])
        mx = np.pad(m, (0, self.n))  # Multiply by x^n
        
        # Calculate remainder
        _, remainder = self.polynomial_division(mx, self.g)
        remainder = np.pad(remainder, (0, self.n - len(remainder)))
        
        # Combine message and remainder
        encoded = mx ^ np.pad(remainder, (len(mx) - len(remainder), 0))
        
        encoding_info = {
            'message_poly': self.poly_to_str(m),
            'shifted_poly': self.poly_to_str(mx),
            'remainder_poly': self.poly_to_str(remainder),
            'codeword_poly': self.poly_to_str(encoded),
            'generator_poly': self.poly_to_str(self.g)
        }
        
        return ''.join(map(str, encoded)), encoding_info

    def find_error_pattern(self, syndrome: np.ndarray) -> Tuple[int, np.ndarray]:
        """Find error pattern using syndrome lookup"""
        syndromes = self.generate_error_syndromes()
        error_pos = syndromes.get(tuple(syndrome), -1)
        
        if error_pos != -1:
            error_pattern = np.zeros(self.code_length, dtype=int)
            error_pattern[-(error_pos + 1)] = 1
            return error_pos, error_pattern
        return -1, np.zeros(self.code_length, dtype=int)

    def decode_block(self, received_block: str) -> Tuple[str, dict]:
        """Decode a received block and correct errors if possible"""
        r = np.array([int(bit) for bit in received_block])
        syndrome = self.calculate_syndrome(r)
        
        # Store the first k bits as message bits
        message_bits = received_block[:self.k]
        
        # Find error pattern
        error_pos, error_pattern = self.find_error_pattern(syndrome)
        
        error_info = {
            'received_poly': self.poly_to_str(r),
            'syndrome_poly': self.poly_to_str(syndrome),
            'error_detected': np.any(syndrome != 0),
            'error_position': error_pos,
            'error_pattern_poly': self.poly_to_str(error_pattern) if error_pos != -1 else "No error pattern found",
            'corrected': False,
            'original_block': received_block,
            'corrected_block': received_block,
            'message_bits': message_bits,
            'syndrome_calculation': {
                'dividend': self.poly_to_str(r),
                'divisor': self.poly_to_str(self.g),
                'remainder': self.poly_to_str(syndrome)
            }
        }
        
        if not error_info['error_detected']:
            return message_bits, error_info
            
        if error_pos != -1:
            error_info['corrected'] = True
            corrected = r ^ error_pattern
            error_info['corrected_block'] = ''.join(map(str, corrected))
            error_info['corrected_poly'] = self.poly_to_str(corrected)
            
        return message_bits, error_info

    def simulate_transmission(self, encoded_block: str, error_positions: List[int] = None) -> Tuple[str, dict]:
        """Simulate transmission with errors at specific positions"""
        received = list(encoded_block)
        error_pattern = np.zeros(len(received), dtype=int)
        
        if error_positions:
            for pos in error_positions:
                if 0 <= pos < len(received):
                    received[-(pos + 1)] = '1' if received[-(pos + 1)] == '0' else '0'
                    error_pattern[-(pos + 1)] = 1
        
        transmission_info = {
            'original_poly': self.poly_to_str(np.array([int(bit) for bit in encoded_block])),
            'error_pattern_poly': self.poly_to_str(error_pattern),
            'received_poly': self.poly_to_str(np.array([int(bit) for bit in received])),
            'error_positions': error_positions if error_positions else []
        }
        
        return ''.join(received), transmission_info

    def generate_error_syndromes(self) -> dict:
        """Generate all possible single-error syndromes"""
        syndromes = {}
        for i in range(self.code_length):
            error = np.zeros(self.code_length, dtype=int)
            error[-(i+1)] = 1
            syndrome = self.calculate_syndrome(error)
            syndromes[tuple(syndrome)] = i
        return syndromes