import { useMemo } from 'react';
import { usePathname } from 'next/navigation';
import { BsNewspaper } from 'react-icons/bs';
import { GiEvilMinion } from 'react-icons/gi';

const useProjectRoutes = () => {
    const pathname = usePathname();
    
    const routes = useMemo(() => [
        {
            label: 'News Sentiment Analysis',
            href: '/projects/news-sentiment-analysis',
            icon: BsNewspaper,
            active: pathname === '/projects/news-sentiment-analysis'
        },
        {
            label: 'Crypto Fraud Detection',
            href: '/projects/crypto-fraud-detection',
            icon: GiEvilMinion,
            active: pathname === '/projects/crpyto-fraud-detection'
        },
    ], [pathname])

    return routes
}

export default useProjectRoutes;