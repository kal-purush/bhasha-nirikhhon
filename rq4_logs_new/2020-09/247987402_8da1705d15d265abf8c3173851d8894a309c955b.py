import hashlib

from services.proto import database_pb2
from services.proto import users_pb2
from services.proto import general_pb2


class FeedVerificationHandler:
    _UTF8 = "utf-8"
    _HASH_ALGORITHM = "sha512"
    _ITERATIONS = 10000
    _FEED_VERIFICATION_PREFIX = "rabble_verification_"

    def __init__(self, logger, db_stub):
        self._logger = logger
        self._db = db_stub

    def _create_rabble_identifier(self, user):
        return "{}_{}_".format(self._FEED_VERIFICATION_PREFIX, user.global_id)

    def _generate_hash(self, user, feed_url):
        hash = hashlib.pbkdf2_hmac(
            self._HASH_ALGORITHM,
            feed_url.encode(self._UTF8),
            user.private_key.encode(self._UTF8),
            self._ITERATIONS)
        return self._create_rabble_identifier(user) + hash.hex()

    def CreateFeedVerificationHash(self, req, context):
        self._logger.info(
            "Request to create a verification hash by user id %s for %s",
            req.user_id, req.feed_url)
        user_find_resp = self._db.Users(database_pb2.UsersRequest(
            request_type=database_pb2.RequestType.FIND,
            match=database_pb2.UsersEntry(
                global_id=req.user_id,
            )
        ))
        if user_find_resp.result_type != general_pb2.ResultType.OK:
            self._logger.error("Error getting CSS: %s", user_find_resp.error)
            return users_pb2.CreateFeedVerificationHashResponse(
                result=general_pb2.ResultType.ERROR,
                error=user_find_resp.error,
            )
        elif len(user_find_resp.results) != 1:
            self._logger.error(
                "Got wrong number of results for user find. Expected 1 got %d",
                len(user_find_resp.results))
            return users_pb2.GetCssResponse(
                result=general_pb2.ResultType.ERROR,
                error="Got wrong number of results during user find",
            )
        hash = self._generate_hash(user_find_resp.results[0], req.feed_url)
        return users_pb2.CreateFeedVerificationHashResponse(
            result=general_pb2.ResultType.OK,
            verification_hash=hash,
        )