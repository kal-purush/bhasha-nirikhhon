import { BaseHelper } from '@/baseHelper';
import { expect } from '@playwright/test';
import chalk from 'chalk';

export class NotificationHelper extends BaseHelper {
    constructor(page) {
        super(page);
    }

    async getToastSuccess(): Promise<string> {
        const toast = this.locate('div', {
            class: ['ct-toast-success'],
        })._locator;
        const toastCount = await toast.count();
        if (toastCount > 0) {
            for (let i = 0; i < toastCount; i++) {
                const successMsg = await toast.last().innerText();
                return successMsg;
            }
        }
    }

    async checkToastSuccess(message: string | number): Promise<void> {
        const toastMessage = await this.getToastSuccess();
        expect(toastMessage, chalk.red('Toast Message check ')).toBe(message);
    }

    async getToastError(): Promise<string> {
        const toast = this.locate('div', {
            class: ['ct-toast-error'],
        })._locator;

        const toastCount = await toast.count();
        if (toastCount > 0) {
            for (let i = 0; i < toastCount; i++) {
                const errorMsg = await toast.last().innerText();
                return errorMsg;
            }
        }
    }
    async checkToastError(message: string | number): Promise<void> {
        const toastMessage = await this.getToastError();
        expect(toastMessage, chalk.red('Toast Message check ')).toBe(message);
    }

    async getToastWarning(): Promise<string> {
        const toast = this.locate('div', { class: ['ct-toast-warn'] })._locator;
        const toastCount = await toast.count();
        if (toastCount > 0) {
            for (let i = 0; i < toastCount; i++) {
                const warningMsg = await toast.last().innerText();
                return warningMsg;
            }
        }
    }
    async checkToastWarning(message: string | number): Promise<void> {
        const toastMessage = await this.getToastWarning();
        expect(toastMessage, chalk.red('Toast Message check ')).toBe(message);
    }

    async getToastLoading(): Promise<string> {
        const toast = this.locate('div', { class: ['ct-toast-load'] })._locator;
        const toastCount = await toast.count();
        if (toastCount > 0) {
            for (let i = 0; i < toastCount; i++) {
                const loadingMsg = await toast.last().innerText();
                return loadingMsg;
            }
        }
    }

    async checkToastLoading(message: string | number): Promise<void> {
        const toastMessage = await this.getToastLoading();
        expect(toastMessage, chalk.red('Toast Message check ')).toBe(message);
    }
    async getErrorMessage() {
        const error = this.locate('span', {
            class: ['label.text-error'],
        })._locator;
        const errorCount = await error.count();

        if (errorCount > 0) {
            console.log(chalk.red(`Error ocurred: ${errorCount}`));
            for (let i = 0; i < errorCount; i++) {
                const errorMsg = await error.nth(i).innerText();
                return errorMsg;
            }
        }
    }

    async checkErrorMessage(message: string | number): Promise<void> {
        const toastMessage = await this.getErrorMessage();
        expect(toastMessage, chalk.red('Error Message check ')).toBe(message);
    }
}