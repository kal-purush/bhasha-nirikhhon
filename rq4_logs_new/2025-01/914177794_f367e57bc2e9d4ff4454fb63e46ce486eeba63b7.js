// components/Captcha.js
"use client"
import { useEffect, useState } from 'react';

const Captcha = ({ onSuccess }) => {
  const [scriptLoaded, setScriptLoaded] = useState(false);

  useEffect(() => {
    const loadScript = () => {
      const script = document.createElement('script');
      script.src = "https://09bd26e5e726.eu-west-3.captcha-sdk.awswaf.com/09bd26e5e726/jsapi.js";
      script.type = "text/javascript";
      script.defer = true;
      script.onload = () => setScriptLoaded(true);
      document.head.appendChild(script);
    };

    if (typeof window !== 'undefined' && !scriptLoaded) {
      loadScript();
    }
  }, [scriptLoaded]);

  useEffect(() => {
    if (scriptLoaded && typeof window !== 'undefined' && window.AwsWafCaptcha) {
      window.showMyCaptcha = function () {
        var container = document.querySelector("#my-captcha-container");

        window.AwsWafCaptcha.renderCaptcha(container, {
          apiKey: process.env.NEXT_PUBLIC_WAF_API_KEY,
          onSuccess: function (wafToken) {
            onSuccess();
          },
          onError: function (error) {
            console.error('CAPTCHA error:', error);
          },
        });
      };

      window.showMyCaptcha();
    }
  }, [scriptLoaded, onSuccess]);

  return (
    <div>
      <div id="my-captcha-container">
      </div>
    </div>
  );
};

export default Captcha;