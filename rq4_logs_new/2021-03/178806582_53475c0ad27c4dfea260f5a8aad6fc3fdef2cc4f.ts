import type { SocialNetworkUI } from '../../../social-network-next'

const en = ['This tweet is encrypted with #mask_io (@realMaskbook). 📪🔑', 'Install {{encrypted}} to decrypt it.'].join(
    '\n\n',
)
const zh = ['此推文已被 Mask（@realmaskbook）加密。📪🔑', '請安裝 {{encrypted}} 進行解密。'].join('\n\n')
export const oldTwitterI18NOverwrite = {
    en: { additional_post_box__encrypted_post_pre: en },
    zh: { additional_post_box__encrypted_post_pre: zh },
}

export const i18NOverwriteTwitter: SocialNetworkUI.Customization.I18NOverwrite = {
    mask: {
        additional_post_box__encrypted_post_pre: { en, zh },
    },
}