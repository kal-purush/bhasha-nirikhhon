import sys

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject


from ck_ab import ck_ab

class ck_ab_gst(ck_ab):

    def run_pipeline(self, pipeline):

        if self.args.verbose: print(pipeline)

        def on_eos(bus, message):
            print('Received EOS-Signal')
            sys.exit(0)

        def on_error(bus, message):
            print('Received Error-Signal')
            (error, debug) = message.parse_error()
            print('Error-Details: #%u: %s' % (error.code, debug))
            sys.exit(2)

        Gst.init([])

        print('starting pipeline...')
        senderPipeline = Gst.parse_launch(pipeline)

        # Binding End-of-Stream-Signal on Source-Pipeline
        senderPipeline.bus.add_signal_watch()
        senderPipeline.bus.connect("message::eos", on_eos)
        senderPipeline.bus.connect("message::error", on_error)

        print("playing...")
        senderPipeline.set_state(Gst.State.PLAYING)

        mainloop = GObject.MainLoop()
        try:
            mainloop.run()
        except KeyboardInterrupt:
            print('Terminated via Ctrl-C')

        print('Shutting down...')
        senderPipeline.set_state(Gst.State.NULL)
        print('Done.')

        return

    def test(self):
        pass


if __name__=='__main__':
    t = ck_video()
    t.main()
