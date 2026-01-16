import av

container = av.open('media/first.mp4')
video = next(s for s in container.streams if s.type == b'video')

for packet in container.demux(video):
    for frame in packet.decode():
        frame.to_image().save('frames/frame-%04d.jpg' % frame.index)