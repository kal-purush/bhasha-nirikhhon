import {Page, Locator} from '@playwright/test'
import { randomInt } from 'crypto';

export class BaseFunctions {
    page: Page;
    constructor(page: Page) {
        this.page = page
    }
    
    click_element = async (locator: Locator, index: number = 0) => {
        await locator.nth(index).waitFor({state: 'visible'})
        await locator.nth(index).click()
        await this.page.waitForTimeout(500)
    }

    generateRandomIndex = async (locator: Locator) => {
        await this.page.waitForLoadState('load')
        await this.page.waitForTimeout(1000)
        const count = await locator.count()
        return Math.floor(Math.random() * count)
    }
    
    input_text = async (locator: Locator, text: string) => {
        await locator.fill(text)
        await this.page.waitForTimeout(500)
    }

    get_element_text = async (locator: Locator, index: number = 0): Promise<string> => {
        await locator.nth(index).waitFor({state: 'visible'})
        return await locator.nth(index).innerText()
    }

    get_ticket_price = async (locator: Locator, index: number = 0) => {
        const text = await this.get_element_text(locator, index);
        const floatValue = parseFloat(text.replace(/[^\d.-]/g, ''))
        console.log("Ticket Price: ", floatValue.toFixed(2))
        return floatValue.toFixed(2)
    }

    get_cart_total = async (index: number = 0) => {
        const integer = await this.page.locator('.price__integers').nth(index).innerText()
        const decimal = await this.page.locator('.price__decimals').nth(index).innerText()
        const cart_total = parseFloat(`${integer}.${decimal}`)
        console.log("Cart Total: ", cart_total.toFixed(2))
        return cart_total.toFixed(2)
    }

    /**
     * Selects two random flight days from the given locator of a calendar element.
     * 
     * @param {Locator} locator - The locator to select elements from. Must be an array of days.
     * @return {Promise<void>} - A promise that resolves when the flight days are selected.
     * @throws {Error} - If there are not enough elements to select two unique ones.
     */
    pick_flight_dates = async(locator: Locator): Promise<void> => {
            
        // Get all elements matching the locator
        let elements = await locator.all()
        let count = elements.length;
    
        if (count < 2) {
            throw new Error("Not enough elements to select two unique ones.")
        }
    
        // Generate two unique indices
        const firstIndex = randomInt(0, count - 2)
        const firstElement = elements[firstIndex]
        await firstElement.click()

        
        // Select elements based on indices
        elements = await locator.all()
        count = elements.length
        const secondIndex = randomInt(0, count - 1)
        const secondElement = elements[secondIndex]
    
        console.log(`Selected indices: ${firstIndex}, ${secondIndex}`)
        console.log(`Selected elements:`, firstElement, secondElement)

        await secondElement.click()
    }


    
    

}