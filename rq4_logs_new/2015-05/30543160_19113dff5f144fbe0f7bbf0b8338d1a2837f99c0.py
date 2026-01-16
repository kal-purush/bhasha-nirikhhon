"""c01db33f Christmas Challenge.

This challenge involves logging into the server at 0wn.b33f.me and recovering
the keys from the service.

  * Flag 1: Encrypted response can be reversed to find the flag.
  * Flag 2: Execute a FORTRAN program.
  * Flag 3: (not solved)
"""

import socket
import struct
import sys

# This is the response required for determining the crypto.
ENCRYPTED_RESPONSE = """\
To enhance the security of our members, we have implemented a what-the-factor \
authentication system. If you haven't yet received your passphrase or are \
having issues using your issued prng token, please contact member support.
I need your passphrase, sir?
"""

# Utility functions.

def ListToHex(list_):
  list_ =  int(''.join(list_).encode('hex'), 16)
  return int(struct.pack('<L', list_).encode('hex'), 16)


def ROL(byte, count):
  """Implementation of the x86 rol instruction."""
  return ((byte << count) | (byte >> (32 - count))) & 0xffffffff


def LittleEndian(input_string):
  """Convert a byte string into its little Endian equivalent."""
  return int(
      struct.pack('<L', int(input_string.encode('hex'), 16)).encode('hex'), 16)


class ChristmasChallenge(object):
  """Interact with http://0wn.b33f.me/ and get the treasures."""

  def __init__(self):
    """Instantiate the connection."""

    self.encrypt_array = []
    self.edx_counter = 0
    self.ecx_counter = 1

    # Some things behave differently locally.
    self.local = True

    if self.local:
      self._sock = socket.create_connection(('0.0.0.0', 9989))
      self.flag_1 = 'CHRISTMAS{AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA}\n'
      self.flag_2 = 'CHRISTMAS{BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB}\n'
    else:
      self._sock = socket.create_connection(('162.243.220.222', 9989))
      self.flag_1 = 'CHRISTMAS{mY_crYpt0_sucks_aNd_1_shoU1d_b3_4shaMed}\n'
      self.flag_2 = 'CHRISTMAS{h4cKing_l1ek_it5_1956_all_0v3r_aGa1n}\n'

  def InitArrays(self):
    """This is used only for testing and is not required on the live version."""
    # Starting 4-byte /dev/urandom key.
    self.starting_key = int(raw_input('Enter value: '), 16)

    # The constant 0x2e1832af at memory address 0x804e220 is XOR'd with the key.
    # 0x804b937       xor    DWORD PTR [ebx+0x124],esi
    self.xor_const = 0x2e1832af ^ self.starting_key

    # The key is then AND'd with a constant 0xFF0. This becomes the value that is
    # checked during the LOOP below. This is at most 4080 (0xFF0).
    self.loop_iterations = self.starting_key & 0xFF0

    # Following the CHRISTMAS{A*39} string, there are additional junk bytes that are
    # used when calculating the hash. Note that the final four-byte sequence is
    # actually determined based on the key value.
    self._array_bytes = [
        0x49524843, 0x414d5453, 0x41417b53, 0x41414141,
        0x41414141, 0x41414141, 0x41414141, 0x41414141,
        0x41414141, 0x41414141, 0x41414141, 0x41414141,
        0x14007d41, 0x80802c19, 0x13e06a53, 0xf5af7004,
        0x40b79d02, 0x3f34167b, 0x94283bed, 0xab3cd88b,
        0x399ae388, 0xa825c405, 0x47e8f8a0, 0x4436ed8d,
        0x0dbef722, 0x752f11bc, 0x7bf809d7, 0xf7d61652,
        0x829c1ce8, 0x1926a149, 0xeb1c5b56, self.xor_const,
    ]

    self.ecx_counter = 1
    self.edx_counter = 0

    # Generate the CHRISTMAS_ARRAY based on the above bytes.
    self._christmas_array = []
    for byte_ in self._array_bytes:
      packed = struct.pack('<L', byte_)
      self._christmas_array.extend(packed)

    self.encrypt_array = []

    # Generate the starting buffer based on the random key.
    self.GenInitialChristmasArray()

  def GenInitialChristmasArray(self):
    """Generate the initial state of the christmas_array."""
    while self.loop_iterations:
      # Hash the starting array.
      self.ModifyChristmasArray(self._christmas_array)
      self.loop_iterations -= 1

  def GenNewChristmasArray(self):
    new_arr = []
    for i in range(32):
      new_arr.extend(self.ModifyChristmasArray(self._christmas_array))
    return new_arr

  def EncryptString(self, target):
    """Encrypt a string using the current encryption scheme.

    When a message is encrypted, the same general process is followed, but the
    difference is that instead of each counter starting at 0 and 1, they start
    at different positions, pulling data out of the array, adding, rotating and
    reinserting.
    """
    to_encrypt_array = list(target)
    len_encrypt = len(target)

    # Generate a new CHRISTMAS_ARRAY buffer. This is a 128-byte buffer that is
    # generated and then used when encrypting strings. When it is exhausted, a new
    # one is created.
    position = 0
    encrypted = []
    while position < len_encrypt:
      to_encrypt = to_encrypt_array[position:position+32]
      for char in to_encrypt:

        # If the array has been exhausted, refill it.
        if not self.encrypt_array:
          self.encrypt_array = self.GenNewChristmasArray()

        xor_byte = self.encrypt_array.pop(0)
        encrypted.append(struct.pack('<B', ord(char) ^ ord(xor_byte)))

      position += 32

    return struct.pack('<%dc' % len_encrypt, *encrypted)

  def DecryptString(self, target):
    decryped = []
    for char in target:
      # If the array has been exhausted, refill it.
      if not self.encrypt_array:
        self.encrypt_array = self.GenNewChristmasArray()

      xor_byte = self.encrypt_array.pop(0)
      decryped.append(ord(char) ^ ord(xor_byte))

    return ''.join(chr(char) for char in decryped)

  def FindEncryptionKey(self, bytes_, target):
    """Given a byte stream and its known equivalent, find the encryption key.

    Take the byte stream we get back and see if we can derive the state of the
    christmas_array at the time that we started encoding the first 128 bytes.

    We can find this by backing into the encrypt_array used to generate the
    first 128 bytes of the encrypted response (by just XOR'ing the bytes with
    their known values). The resulting array can then be continuously rotated by
    blocks of four bytes, stopping when one of the shifted arrays encrypts the
    target string into the same byte sequence as we observed.
    """
    # The usage of the xor_const at the end of the array makes brute forcing
    # impossible. Therefore, reverse the encryption process.
    self.encrypt_array = []

    # During the first pass, the Christmas array will not be populated. Therefore
    # we can derive it from the first 128 bytes.
    christmas_array = []
    for index, byte in enumerate(bytes_[:128]):
      christmas_array.append(
          struct.pack('<B', int(byte.encode('hex'), 16) ^ ord(target[index])))

    # At this point, christmas_array contains the same values as
    # first_christmas_array, but 'rotated' by a certain amount (since the
    # starting point is determined by the position of $ecx after the original
    # array is created). Since there are only 32 possible positions, iterate
    # through all of them until we find the one that when used creates a matching
    # hash for the first encrypted array.

    # NOTE: For some reason, the answer is always found at position 124, with
    # ecx_counter at 1 and eax_counter at 0. Unclear why this is as it seems
    # that counter values (17, 16) should be allowed as well ( & 0xFF0 can be
    # any number between 16 and 4080, step 16).
    self._christmas_array = christmas_array[124:] + christmas_array[:124]
    self.ecx_counter = 1
    self.edx_counter = 0
    if self.EncryptString(target) == bytes_:
      return

    print 'Incorrect crypto - exiting.'
    sys.exit(1)

  def FindFlagOne(self):
    """Find the first key."""
    ecx = 0
    edx = 31

    x = 4080
    while x >= 0:
      observed = ListToHex(self._christmas_array[ecx*4:(ecx*4)+4])
      edi = ListToHex(self._christmas_array[edx*4:(edx*4)+4])

      unrotated = ROL(observed, 0x13)

      unadded = unrotated - edi
      if unadded < 0:
        unadded = ((unrotated | 0xF00000000) - edi) & 0xFFFFFFFF

      self._christmas_array[ecx*4:(ecx*4)+4] = struct.pack('<L', unadded)

      ecx = 31 if ecx == 0 else ecx - 1
      edx = 31 if edx == 0 else edx - 1

      x -= 1

      # Look for the CHRISTMAS{ marker.
      if 'CHRISTMAS{' in ''.join(self._christmas_array):
        flag_array = ''.join(self._christmas_array)
        flag_pos = flag_array.find('CHRISTMAS{')
        print flag_array[flag_pos:flag_pos+50]
        break

  def ModifyChristmasArray(self, buf):
    """Modify the christmas_array and return $edi."""
    # ecx starts at 1 | edx starts at 0.

    # Pull the values from the buffer at the positions indicated by
    # self.ecx_counter and self.edx_counter.
    eax_value = LittleEndian(
        ''.join(buf[self.ecx_counter*4:(self.ecx_counter*4)+4]))
    edi_value = LittleEndian(
        ''.join(buf[self.edx_counter*4:(self.edx_counter*4)+4]))

    # Add the two values, ignoring the carry.
    edi_plus_eax = (edi_value + eax_value) & 0xFFFFFFFF

    # Rotate the digits 13 places left.
    rotated = ROL(edi_plus_eax, 0xd)
    rotated_hex = struct.pack('<L', rotated)

    # Replace the byte at self.ecx_counter*4 in buf with the rotated byte.
    buf[self.ecx_counter*4:(self.ecx_counter*4)+4] = rotated_hex

    # Increment the counters, resetting to 0 if either reaches 31.
    self.ecx_counter = 0 if self.ecx_counter == 31 else self.ecx_counter + 1
    self.edx_counter = 0 if self.edx_counter == 31 else self.edx_counter + 1

    # After modifying the array, return the eax value.
    return struct.pack('<L', eax_value)

  def Recv(self, bytes_):
    """Read a number of bytes from the socket."""
    return self._sock.recv(bytes_)

  def RecvUntil(self, target):
    """Read from the socket until the target is found."""
    seen = ''
    while target not in seen:
        seen += self._sock.recv(1)
    return seen.strip()

  def Send(self, payload):
    """Send a payload to the socket."""
    self._sock.send(payload)

  def Join(self):
    """Join the club."""
    self.Send('join\n')
    referral = self.Recv(4096)
    assert (referral == (
      'Does sir have a referral from an upstanding member of the club?\n'))

    self.Send(self.flag_1)
    register = self.Recv(80)
    assert (register == (
      'Very good sir. And what name would you like to register under?\n'))

    self.Send('RocketDonkey\n')

    # Receive 'Will there by anything else today, sir?\n'
    received_else = self.Recv(40)
    assert (received_else ==
            'Will there be anything else today, sir?\n')

  def Secure(self):
    """Secure the connection."""
    self.Send('secure\n')
    print self.RecvUntil('You *do* have your prng token to hand, sir?\n')
    self.Send('But of course, Jarvis.\n')
    print self.RecvUntil('Very well, sir, enabling encryption.\n')

    # Decrypt the response, finding both the crypto and the first flag.
    self.FindEncryptionKey(self.Recv(256), ENCRYPTED_RESPONSE)

    # Find the flag (CHRISTMAS{mY_crYpt0_sucks_aNd_1_shoU1d_b3_4shaMed})
    # self.FindFlagOne()

    # Respond with the passphrase.
    self.Send(self.EncryptString('Oh, well, tra-la-la!\n'))

    # Receive 'Very good, sir.\n'
    received_good = self.Recv(15)
    assert received_good == self.EncryptString('Very good sir.\n')

    # Receive 'Will there by anything else today, sir?\n'
    received_else = self.Recv(40)
    assert (received_else ==
            self.EncryptString('Will there be anything else today, sir?\n'))

  def Login(self):
    """Login to the server."""
    self.Send(self.EncryptString('login\n'))

    # 'Please provide your membership number to authenticate:'
    print self.DecryptString(self.Recv(4096))

    # Flag 1.
    self.Send(self.EncryptString(self.flag_1))

    # 'Ah, I see, sir has a basic account. In that case, your limited cloud
    # execution access has been provisioned.'
    print self.DecryptString(self.Recv(4096))

    # 'Will there by anything else today, sir?'
    print self.DecryptString(self.Recv(4096))

  def Fortran(self):
    """Interact with the FORTRAN service."""
    self.Send(self.EncryptString('fortran\n'))

    # FORTRAN banner.
    print self.DecryptString(self.Recv(4096))

    # SEND PROGRAM.
    print self.DecryptString(self.Recv(4096))

    fortran_program = (
        '       DIMENSION A(47)',
        '       READ TAPE 1,(A(B),B=1,47)',
        '     1 FORMAT (I)',
        '       PRINT 1,(A(1))',
        '       PRINT 1,(A(2))',
        '       PRINT 1,(A(3))',
        '       PRINT 1,(A(4))',
        '       PRINT 1,(A(5))',
        '       PRINT 1,(A(6))',
        '       PRINT 1,(A(7))',
        '       PRINT 1,(A(8))',
        '       PRINT 1,(A(9))',
        '       PRINT 1,(A(10))',
        '       PRINT 1,(A(11))',
        '       PRINT 1,(A(12))',
        '       PRINT 1,(A(13))',
        '       PRINT 1,(A(14))',
        '       PRINT 1,(A(15))',
        '       PRINT 1,(A(16))',
        '       PRINT 1,(A(17))',
        '       PRINT 1,(A(18))',
        '       PRINT 1,(A(19))',
        '       PRINT 1,(A(20))',
        '       PRINT 1,(A(21))',
        '       PRINT 1,(A(22))',
        '       PRINT 1,(A(23))',
        '       PRINT 1,(A(24))',
      )

    # Pad the output so that each line is exactly 72 characters.
    fortran_program = ['{:<72s}'.format(stmt) for stmt in fortran_program]

    # Add a newline.
    fortran_program[-1] += '\n'

    # Send the program.
    self.Send(self.EncryptString(''.join(fortran_program)))

    fortran_output = '\n'.join('INIT: %s' % st for st in fortran_program)

    # WARMING DRUMS.
    print self.DecryptString(self.Recv(18))

    # Consume post-WARMING dots.
    for _ in range(8):
      print self.DecryptString(self.Recv(1)),

    # SPOOLING TAPE.
    print self.DecryptString(self.Recv(17))

    # Consume post-SPOOLING dots.
    for _ in range(8):
      print self.DecryptString(self.Recv(1)),

    # LOADING PROGRAM.
    print self.DecryptString(self.Recv(17))

    # Receive the response program.
    program_output = self.DecryptString(self.Recv(4096))
    if not self.local:
      program_output += self.DecryptString(self.Recv(4096))

    # Print the output plus EXECUTING PROGRAM/newlines.
    program_start_offset = len(fortran_output) + 19
    print program_output[:program_start_offset]

    # The next 141 bytes are the bytes we need to translate.
    flag_bytes = program_output[program_start_offset:program_start_offset+141]
    flag = []
    for byte in flag_bytes.split('\n'):
      two_chars = int(byte)
      flag.append(chr(two_chars & 0xFF))
      flag.append(chr(two_chars >> 8))

    # Print the flag.
    print 'FLAG: %s' % ''.join(flag)

    # This consumes up until 'Will there be anything else today sir?'.

  def Elevate(self):
    """Elevate the membership (using flag_2)."""
    self.Send(self.EncryptString('elevate\n'))
    print self.DecryptString(self.Recv(4096))

    self.Send(self.EncryptString(self.flag_2))
    print self.DecryptString(self.Recv(4096))

    self.Send(self.EncryptString('RocketDonkey\n'))
    print self.DecryptString(self.Recv(4096))

  def Secret(self):
    self.Send(self.EncryptString('secret\n'))
    print self.DecryptString(self.Recv(4096))

  def Private(self):
    """Read the private members."""
    self.Send(self.EncryptString('private\n'))
    print self.DecryptString(self.Recv(4096))
    print self.DecryptString(self.Recv(4096))




def main():
  """Interact with the 0wn.b33f.me Christmas challenge."""
  challenge = ChristmasChallenge()

  # Opening flow.
  print challenge.RecvUntil('accessing sensitive services.\n')

  # Join. Works fine on the server but not locally because I have no idea why.
  # challenge.Join()

  # Secure the connection.
  challenge.Secure()

  # Login.
  challenge.Login()

  # Fortran.
  challenge.Fortran()

  # Elevate.
  # challenge.Elevate()

  # Private.
  challenge.Private()


if __name__ == '__main__':
  main()