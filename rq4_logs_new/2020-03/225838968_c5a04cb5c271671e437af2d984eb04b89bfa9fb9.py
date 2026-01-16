from superdesk.signals import item_validate
from superdesk import get_resource_service
from flask_babel import _


def validate(sender, item, response, error_fields, **kwargs):
    if not item.get('profile'):
        return

    associations = item.get('associations', {})
    video_profile = get_resource_service('content_types').find_one(req=None, label='VideoItem')

    if (video_profile and str(video_profile.get('_id', '')) != item.get('profile', '')
            or (associations and not associations.get('featuremedia', {}))):
        return

    if (associations.get('featuremedia', {}).get('type', '') == 'video'
            and not associations.get('featuremedia', {}).get('renditions', {}).get('thumbnail', '')
            and not associations.get('VideoThumbnail', '')):

        response.append(_('The card cannot be published. The thumbnail image for video is missing.'))


def init_app(app):
    item_validate.connect(validate)