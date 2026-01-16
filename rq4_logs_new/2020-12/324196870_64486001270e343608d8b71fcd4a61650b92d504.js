import React, { useEffect } from 'react'
import Router from 'next/router';

import settings from '../settings/settings'
import loadFacebookPixel from '../utils/loadFacebookPixel'
import fbTrackEvent from '../utils/fbTrackEvent'

const FacebookPixel = () => {
  useEffect(()=>{
    loadFacebookPixel()
    const fbPageView = () => {
        fbTrackEvent('PageView')
    }
    Router.events.on('routeChangeStart', fbPageView); 

    return () => {
      Router.events.off('routeChangeStart', fbPageView); 
    } 
  },[])

    return (
            <noscript><img height="1" width="1" style={{display:"none"}}
            src={`https://www.facebook.com/tr?id=${settings.FB_PIXEL}&ev=PageView&noscript=1`}
            /></noscript>
        )
}

export default FacebookPixel