from securenative.sdk import SecureNative
from securenative.sdk_options import SecureNativeOptions

_sn_sdk = None


def init(api_key, options=SecureNativeOptions()):
    global _sn_sdk
    if _sn_sdk is None:
        _sn_sdk = SecureNative(api_key, options)


def track(event):
    sdk = _get_or_throw()
    sdk.track(event)


def verify(event):
    sdk = _get_or_throw()
    return sdk.verify(event)


def verify_webhook(hmac_header, body):
    sdk = _get_or_throw()
    sdk.verify_webhook(hmac_header=hmac_header, body=body)


def flush():
    sdk = _get_or_throw()
    sdk.flush()


def _get_or_throw():
    global _sn_sdk
    if _sn_sdk is None:
        raise ValueError(
            u'You should call securenative.init(api_key, options) before making any other sdk function call.')
    return _sn_sdk