/**
 * 駅の扱い方
 * - 0  ... 運行なし
 * - 10 ... 停車
 * - 20 ... 通過
 * - 30 ... 経由なし
 * - 40 ...直通
 *
 * Station handling type
 * - 0  ... no operation
 * - 10 ... stop
 * - 20 ... passing through
 * - 30 ... not via
 * - 40 ... direct
 */

export const stHandlingType = {
  fromNative(v: string): number {
    if (v === '1') return 10;
    if (v === '2') return 20;
    if (v === '3') return 30;
    if (v === '4') return 40;

    return 0;
  },
  fromOO(v: number): string {
    if (v === 10) return '1';
    if (v === 20) return '2';
    if (v === 30) return '3';
    if (v === 40) return '4';

    return '';
  },
};