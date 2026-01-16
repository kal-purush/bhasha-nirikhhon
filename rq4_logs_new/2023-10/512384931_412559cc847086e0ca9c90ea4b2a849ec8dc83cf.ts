import {OrganizationDto} from '^types/organization.type';
import {TypeCast} from '^types/utils/class-transformer';
import {TeamMemberDto} from '^types/team-member.type';
import {SubscriptionDto} from '^types/subscription.type';
import {BillingHistoryDto} from '^types/billing.type';
import {cardSign} from '^config/environments';
import CryptoJS from 'crypto-js';

export class CreditCardDto {
    id: number; // 카드 ID
    sign: string; // 카드 정보(카드번호, 비밀번호, 유효기간, CVC)
    name?: string | null; // 카드 이름
    issuerCompany?: string | null; // 카드 발급사
    networkCompany?: string | null; // 카드 결제 네트워크
    memo?: string | null; // 메모
    isPersonal: boolean; // 법인 카드 여부
    organizationId: number; // 조직 ID
    @TypeCast(() => OrganizationDto) organization?: OrganizationDto | null; // 조직
    holdingMemberId?: string | null; // 카드 소유자 ID
    @TypeCast(() => TeamMemberDto) holdingMember?: TeamMemberDto | null; // 카드 소유자
    @TypeCast(() => SubscriptionDto) subscriptions?: SubscriptionDto[] | null; // 결제한 구독 목록
    @TypeCast(() => BillingHistoryDto) billingHistories?: BillingHistoryDto[] | null; // 결제 내역

    decryptSign(): CreditCardSecretInfo {
        const json = CryptoJS.AES.decrypt(this.sign, cardSign).toString(CryptoJS.enc.Utf8);
        return JSON.parse(json) as CreditCardSecretInfo;
    }
}

export class CreditCardSecretInfo {
    number1?: string | null;
    number2?: string | null;
    number3?: string | null;
    number4?: string | null;
    password?: string;
    cvc?: string;
    expiry?: string;
}

export class UnSignedCreditCardFormData extends CreditCardSecretInfo {
    name?: string | null;
    issuerCompany?: string | null;
    networkCompany?: string | null;
    memo?: string | null;
    isPersonal?: boolean | null;
    holdingMemberId?: string | null;
    productIds?: number[] | null;

    get sign(): string {
        const json = JSON.stringify({...this.secretInfo});
        return CryptoJS.AES.encrypt(json, cardSign).toString();
    }

    toCreateDto(): CreateCreditCardDto {
        return {
            sign: this.sign,
            name: this.name,
            issuerCompany: this.issuerCompany,
            networkCompany: this.networkCompany,
            memo: this.memo,
            isPersonal: this.isPersonal,
            holdingMemberId: this.holdingMemberId,
            productIds: this.productIds,
        };
    }

    toUpdateDto(): UpdateCreditCardDto {
        return {
            sign: this.sign,
            name: this.name,
            issuerCompany: this.issuerCompany,
            networkCompany: this.networkCompany,
            memo: this.memo,
            isPersonal: this.isPersonal,
            holdingMemberId: this.holdingMemberId,
            productIds: this.productIds,
        };
    }

    private get secretInfo(): CreditCardSecretInfo {
        return {
            number1: this.number1,
            number2: this.number2,
            number3: this.number3,
            number4: this.number4,
            password: this.password,
            cvc: this.cvc,
            expiry: this.expiry,
        };
    }
}

export type CreateCreditCardDto = {
    sign: string;
    name?: string | null;
    issuerCompany?: string | null;
    networkCompany?: string | null;
    memo?: string | null;
    isPersonal?: boolean | null;
    holdingMemberId?: string | null;
    productIds?: number[] | null;
};

export type UpdateCreditCardDto = {
    sign?: string;
    name?: string | null;
    issuerCompany?: string | null;
    networkCompany?: string | null;
    memo?: string | null;
    isPersonal?: boolean | null;
    holdingMemberId?: string | null;
    productIds?: number[] | null;
};