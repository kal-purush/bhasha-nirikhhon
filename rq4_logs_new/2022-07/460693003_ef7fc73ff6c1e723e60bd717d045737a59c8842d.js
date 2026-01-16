import { useEffect } from "react";
import { ADS_ID } from "../lib/constants";

const Banner = ({
  className,
  style,
  layout,
  format,
  client = ADS_ID,
  slot,
  responsive,
  layoutKey,
  auto,
}) => {
  useEffect(() => {
    try {
      const adsbygoogle = window.adsbygoogle || [];
      adsbygoogle.push({});
    } catch (e) {
      console.error(e);
    }
  }, []);

  return auto ? (
    <div
      className={`${className} relative z-0 mx-auto mb-2 flex justify-center overflow-hidden bg-black/10 after:absolute after:bottom-0.5 after:left-1/2 after:-z-10 after:-translate-x-1/2 after:text-xs after:text-white/20 after:content-['ADVERTISEMENT']`}
    >
      <ins
        className={`adsbygoogle`}
        style={style}
        data-ad-layout={layout}
        data-ad-format={format}
        data-ad-client={client}
        data-ad-slot={slot}
        data-ad-layout-key={layoutKey}
        data-full-width-responsive={responsive}
      />
    </div>
  ) : (
    <div
      className={`${className} AdContainer relative z-0 mx-auto mb-2 flex justify-center overflow-hidden bg-black/10 after:absolute after:bottom-0.5 after:left-1/2 after:-z-10 after:-translate-x-1/2 after:text-xs after:text-white/20 after:content-['ADVERTISEMENT']`}
    >
      <ins
        className={`adsbygoogle AdContainer`}
        style={style}
        data-ad-layout={layout}
        data-ad-format={format}
        data-ad-client={client}
        data-ad-slot={slot}
        data-ad-layout-key={layoutKey}
        data-full-width-responsive={responsive}
      />
    </div>
  );
};

export default Banner;