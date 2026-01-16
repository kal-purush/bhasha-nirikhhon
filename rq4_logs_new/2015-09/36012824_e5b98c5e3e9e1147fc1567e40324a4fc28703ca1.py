def includeme(config):
    # add to springboard routes
    config.add_route('content_section', '/section/{slug}/')
    config.add_route('personae', '/persona/')
    config.add_route('skip_persona_selection', '/persona/skip/')
    config.add_route('select_persona', '/persona/{slug}/')
    config.add_route('about', '/about/')
    config.add_tween('springboard_iogt.views.persona_tween_factory')
    config.scan('.views')
    config.scan('.events')

    config.include('springboard.config')
    config.override_asset(
        to_override='springboard:templates/',
        override_with='springboard_iogt:templates/')
    config.add_static_view(
        'static', 'springboard_iogt:static', cache_max_age=3600)
    config.add_translation_dirs('springboard_iogt:locale/')