import ReactGA from "react-ga4";

export const initGA = () => {
    try {
        // Only initialize in production and when window exists (client-side)
        if (typeof window !== 'undefined' && process.env.NODE_ENV === 'production') {
            ReactGA.initialize("G-BTW8J5KHGG");
            console.log('GA initialized successfully');
        }
    } catch (error) {
        console.error('Error initializing GA:', error);
    }
};

export const logPageView = (path: string) => {
    try {
        if (typeof window !== 'undefined' && process.env.NODE_ENV === 'production') {
            ReactGA.send({
                hitType: "pageview",
                page: path
            });
            console.log('Page view logged:', path);
        }
    } catch (error) {
        console.error('Error logging page view:', error);
    }
};

// Utility function for tracking custom events
export const logEvent = (category: string, action: string, label?: string) => {
    try {
        if (typeof window !== 'undefined' && process.env.NODE_ENV === 'production') {
            ReactGA.event({
                category,
                action,
                label
            });
        }
    } catch (error) {
        console.error('Error logging event:', error);
    }
};