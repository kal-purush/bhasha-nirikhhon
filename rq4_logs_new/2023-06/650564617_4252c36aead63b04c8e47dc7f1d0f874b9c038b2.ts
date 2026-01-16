import { useMemo } from 'react';
import { usePathname } from 'next/navigation';
import { HiChat } from 'react-icons/hi';
import { HiArrowLeftOnRectangle, HiUsers } from 'react-icons/hi2';
import { GiMedusaHead } from 'react-icons/gi'

const useRoutes = () => {
    const pathname = usePathname();
    
    const routes = useMemo(() => [
        {
            label: 'Projects',
            href: '/projects',
            icon: GiMedusaHead,
            active: pathname === '/projects'
        },
        {
            label: 'Profile',
            href: '/profile',
            icon: HiUsers,
            active: pathname === '/users'
        },
        {
            label: 'Logout',
            href: '#',
            // TODO: Create sign out once JWT Authentication is setup
            // onClick: () => signOut(),
            icon: HiArrowLeftOnRectangle
        }
    ], [pathname])

    return routes
}

export default useRoutes;