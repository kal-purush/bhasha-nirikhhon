/*
 * @Date: 2023-04-29 17:20:11
 * @LastEditors: mario marioworker@163.com
 * @LastEditTime: 2023-04-29 19:16:33
 * @Description: Do not edi
 */
import { ThemeType } from '@/theme'
import { backgroundGradient } from '@/theme/keyframes'
import { css } from '@emotion/react'

export const LoginModalStyle = (theme: ThemeType) =>
  css({
    '& .ant-form-item-label': {
      label: {
        color: theme.colors.text,
      },
    },
  })

export const LoginButtonStyle = (theme: ThemeType) =>
  css({
    '&.custom-login-button': {
      width: '100%',
      color: '#fff',
      border: 'none',
      background: `linear-gradient(-45deg,${theme.colors.antButtonGradient.join(',')})`,
      paddingTop: '6px',
      paddingBottom: '6px',
      transition: 'all 0.2s ease',
      borderRadius: '50px',
      backgroundSize: '1000% 1000%',
      animation: `${backgroundGradient} 40s linear infinite`,
    },
  })
export const FormItemLastChild = () =>
  css({
    '&.custom-form-item': {
      marginBottom: '0px',
    },
  })