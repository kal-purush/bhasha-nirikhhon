import matplotlib.font_manager

fonts = set(f.name for f in matplotlib.font_manager.fontManager.ttflist)
for font in sorted(fonts):
    print(font)