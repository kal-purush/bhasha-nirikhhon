/*
  Modal Private Hooks.
*/

import { useState, useEffect, useRef, Dispatch, SetStateAction, RefObject } from 'react'
import { store } from '@/ducks'
import { aboutMeToggle } from '@/ducks/AboutMe'
import { useSelector, shallowEqual } from 'react-redux'
import { StoreTypes } from '@/types/type'

export const cubesProps = [
  {
    textureName: 'github',
    linkUrl: 'https://github.com/kojiyamauchi',
    positionValue: {
      x: {
        mobileMin: -1.125,
        mobileMax: -0.875,
        tabletMin: -1.125,
        tabletMax: -0.875,
        deskTopMin: -4,
        deskTopMax: -3
      },
      y: {
        mobileMin: 2.75,
        mobileMax: 3,
        tabletMin: 2.75,
        tabletMax: 3,
        deskTopMin: 1.5,
        deskTopMax: 2.5
      }
    },
    rotate: {
      x: Math.random() * (0.02 - 0.01) + 0.01,
      y: Math.random() * (0.02 - 0.01) + 0.01,
      z: Math.random() * (0.02 - 0.01) + 0.01
    }
  },
  {
    textureName: 'linkedin',
    linkUrl: 'https://www.linkedin.com/in/kojiyyyyamauchi/',
    positionValue: {
      x: {
        mobileMin: -0.125,
        mobileMax: 0.125,
        tabletMin: -0.125,
        tabletMax: 0.125,
        deskTopMin: -4,
        deskTopMax: -3
      },
      y: {
        mobileMin: 2.75,
        mobileMax: 3,
        tabletMin: 2.75,
        tabletMax: 3,
        deskTopMin: -0.5,
        deskTopMax: 0.5
      }
    },
    rotate: {
      x: Math.random() * (0.02 - 0.01) + 0.01,
      y: Math.random() * (0.02 - 0.01) + 0.01,
      z: Math.random() * (0.02 - 0.01) + 0.01
    }
  },
  {
    textureName: 'behance',
    linkUrl: 'https://www.behance.net/kojiyamauchi',
    positionValue: {
      x: {
        mobileMin: 0.875,
        mobileMax: 1.125,
        tabletMin: 0.875,
        tabletMax: 1.125,
        deskTopMin: -4,
        deskTopMax: -3
      },
      y: {
        mobileMin: 2.75,
        mobileMax: 3,
        tabletMin: 2.75,
        tabletMax: 3,
        deskTopMin: -2.5,
        deskTopMax: -1.5
      }
    },
    rotate: {
      x: Math.random() * (0.02 - 0.01) + 0.01,
      y: Math.random() * (0.02 - 0.01) + 0.01,
      z: Math.random() * (0.02 - 0.01) + 0.01
    }
  },
  {
    textureName: 'instagram',
    linkUrl: 'https://www.instagram.com/kojiyamauchi/',
    positionValue: {
      x: {
        mobileMin: -1.125,
        mobileMax: -0.875,
        tabletMin: -1.125,
        tabletMax: -0.875,
        deskTopMin: 3,
        deskTopMax: 4
      },
      y: {
        mobileMin: 1.625,
        mobileMax: 1.875,
        tabletMin: 1.625,
        tabletMax: 1.875,
        deskTopMin: 1.5,
        deskTopMax: 2.5
      }
    },
    rotate: {
      x: Math.random() * (0.02 - 0.01) + 0.01,
      y: Math.random() * (0.02 - 0.01) + 0.01,
      z: Math.random() * (0.02 - 0.01) + 0.01
    }
  },
  {
    textureName: 'twitter',
    linkUrl: 'https://twitter.com/KojiyyyYamauchi',
    positionValue: {
      x: {
        mobileMin: -0.125,
        mobileMax: 0.125,
        tabletMin: -0.125,
        tabletMax: 0.125,
        deskTopMin: 3,
        deskTopMax: 4
      },
      y: {
        mobileMin: 1.625,
        mobileMax: 1.875,
        tabletMin: 1.625,
        tabletMax: 1.875,
        deskTopMin: -0.5,
        deskTopMax: 0.5
      }
    },
    rotate: {
      x: Math.random() * (0.02 - 0.01) + 0.01,
      y: Math.random() * (0.02 - 0.01) + 0.01,
      z: Math.random() * (0.02 - 0.01) + 0.01
    }
  },
  {
    textureName: 'facebook',
    linkUrl: 'https://www.facebook.com/kojiyyyyamauchi',
    positionValue: {
      x: {
        mobileMin: 0.875,
        mobileMax: 1.125,
        tabletMin: 0.875,
        tabletMax: 1.125,
        deskTopMin: 3,
        deskTopMax: 4
      },
      y: {
        mobileMin: 1.625,
        mobileMax: 1.875,
        tabletMin: 1.625,
        tabletMax: 1.875,
        deskTopMin: -2.5,
        deskTopMax: -1.5
      }
    },
    rotate: {
      x: Math.random() * (0.02 - 0.01) + 0.01,
      y: Math.random() * (0.02 - 0.01) + 0.01,
      z: Math.random() * (0.02 - 0.01) + 0.01
    }
  }
]

export const generateDeviceValue = (
  mobileMin: number,
  mobileMax: number,
  tabletMin: number,
  tabletMax: number,
  deskTopMin: number,
  deskTopMax: number
): { mobile: number; tablet: number; deskTop: number } => {
  const mobile = Math.random() * (mobileMax - mobileMin) + mobileMin
  const tablet = Math.random() * (tabletMax - tabletMin) + tabletMin
  const deskTop = Math.random() * (deskTopMax - deskTopMin) + deskTopMin
  return { mobile, tablet, deskTop }
}

export const generatePositionValue = cubesProps.map((info) => {
  const x = generateDeviceValue(
    info.positionValue.x.mobileMin,
    info.positionValue.x.mobileMax,
    info.positionValue.x.tabletMin,
    info.positionValue.x.tabletMax,
    info.positionValue.x.deskTopMin,
    info.positionValue.x.deskTopMax
  )
  const y = generateDeviceValue(
    info.positionValue.y.mobileMin,
    info.positionValue.y.mobileMax,
    info.positionValue.y.tabletMin,
    info.positionValue.y.tabletMax,
    info.positionValue.y.deskTopMin,
    info.positionValue.y.deskTopMax
  )
  return { x, y }
})

export const switchGraffiti = (myComponentCurrent: HTMLDivElement, setCopyrightHover: Dispatch<SetStateAction<boolean>>): void => {
  myComponentCurrent.querySelector('.copyright')?.addEventListener('mouseenter', () => {
    window.innerWidth > 767 && setCopyrightHover(true)
  })
  myComponentCurrent.querySelector('.copyright')?.addEventListener('mouseleave', () => {
    window.innerWidth > 767 && setCopyrightHover(false)
  })
}

export const switchModal = (modalState: boolean, myComponentCurrent: HTMLDivElement): void => {
  modalState ? myComponentCurrent.classList.add('is-active') : myComponentCurrent.classList.remove('is-active')
}

export const openAboutMe = (): void => void store.dispatch(aboutMeToggle({ isOpen: true }))

export type hooksReturnType = {
  myComponent: RefObject<HTMLDivElement>
  cubesProps: typeof cubesProps
  generatePositionValue: typeof generatePositionValue
  openAboutMe: () => void
  copyrightHover: boolean
}

export const useModal = (): hooksReturnType => {
  const myComponent = useRef<HTMLDivElement>(null)
  const [copyrightHover, setCopyrightHover] = useState(false)
  const isOpen = useSelector<StoreTypes, boolean>((state) => state.modalStore.isOpen, shallowEqual)

  useEffect(() => void myComponent.current!.classList.add('is-display'))

  useEffect(() => {
    switchGraffiti(myComponent.current!, setCopyrightHover)
  }, [copyrightHover])

  useEffect(() => {
    switchModal(isOpen, myComponent.current!)
  }, [isOpen])

  return { myComponent, cubesProps, generatePositionValue, openAboutMe, copyrightHover }
}