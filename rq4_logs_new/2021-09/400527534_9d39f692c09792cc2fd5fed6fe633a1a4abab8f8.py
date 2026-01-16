from django_component import Library,Component
register=Library()


@register.component
class component_io_Button(Component):template='component/io/Button.html'

@register.component
class component_decoration_RoundBottomBorder(Component):template='component/decoration/RoundBottomBorder.html'

@register.component
class page_About(Component):template='page/About.html'

@register.component
class page_Home(Component):template='page/Home.html'

@register.component
class layout_Root(Component):template='layout/Root.html'

@register.component
class layout_Simple(Component):template='layout/Simple.html'