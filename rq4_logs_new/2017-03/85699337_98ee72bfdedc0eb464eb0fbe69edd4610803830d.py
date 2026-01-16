from passlib.hash import pbkdf2_sha1
from passlib import utils
import hashlib
import binascii
import hmac
import sys

# Recuperation des arguments

print "username: " + sys.argv[1]
print "password: " + sys.argv[2]
print "client Nonce: " + sys.argv[3]
print "server Nonce: " + sys.argv[4]
print "server salt: " + sys.argv[5]
print "iteration: " + sys.argv[6]
print "clientProof to test with: " + sys.argv[7]

username = sys.argv[1]
password = sys.argv[2]
clientNonce = sys.argv[3]
serverNonce = sys.argv[4]
serverSalt = sys.argv[5]
iteration = sys.argv[6]
clientProofToFound = sys.argv[7]


normalizedPassword = utils.saslprep(unicode(password))

clientInitMessage = "n=" + username + ",r=" + clientNonce

serverInitReply = "r=" + clientNonce + serverNonce + ",s=" + serverSalt + ",i=" + iteration

clientFinal = "c=biws,r=" + clientNonce + serverNonce

saltedPassword = pbkdf2_sha1.using(rounds=iteration, salt=serverSalt.decode("base64")).hash(normalizedPassword).split('$')[-1]

clientKey = hmac.new(utils.ab64_decode(saltedPassword), "Client Key", hashlib.sha1)

storedKey = hashlib.sha1(clientKey.digest())

authMessage = clientInitMessage + "," + serverInitReply + "," + clientFinal

clientSignature = hmac.new(storedKey.digest(), authMessage, hashlib.sha1)

clientProof = binascii.hexlify("".join(chr(ord(c1) ^ ord(c2)) for c1, c2 in
                                    zip(binascii.unhexlify(clientKey.hexdigest()),
                                        binascii.unhexlify(clientSignature.hexdigest()))))


if clientProof.decode("hex") == binascii.a2b_base64(clientProofToFound):
    print "match with " + password
    exit(1)

print "doesn't match with " + password