import { faker } from "@faker-js/faker";
import dayjs from "dayjs";
import * as employeeRepository from "../repositories/employeeRepository.js";
import * as cardRepository from "../repositories/cardRepository.js";

export async function create(employeeId: number, type: cardRepository.TransactionTypes) {
    const cardData = await getCardData(employeeId, type);

    await cardRepository.insert(cardData);
}

export async function validateCreation(employeeId: number, companyId: number, type: cardRepository.TransactionTypes) {
    const employee = await employeeRepository.findById(employeeId);

    if (!employee) throw { status: 404 };
    if (employee.companyId !== companyId) throw { status: 401 };

    const card = await cardRepository.findByTypeAndEmployeeId(type, employeeId);
    if (card) throw { status: 422 };
}

async function getCardData(employeeId: number, type: cardRepository.TransactionTypes) {
    const cardholderName = await getEmployeeName(employeeId);
    const number = faker.finance.creditCardNumber('mastercard');
    const securityCode = faker.finance.creditCardCVV();
    const expirationDate = dayjs().add(5, "year").format("MM/YY");

    return {
        employeeId: employeeId,
        number,
        cardholderName,
        securityCode,
        expirationDate,
        password: null,
        isVirtual: false,
        originalCardId: null,
        isBlocked: false,
        type
    }
}

async function getEmployeeName(employeeId: number) {
    const { fullName } = await employeeRepository.findById(employeeId);
    return formatName(fullName);
}

function formatName(name: string) {
    const nameArray = name.toUpperCase().split(' ');

    let cardholderName = nameArray[0];
    for (let i = 1; i < (nameArray.length - 1); i++) {
        if (nameArray[i].length >= 3) {
            cardholderName += ` ${nameArray[i][0]}`;
        }
    }

    return cardholderName + ` ${nameArray[nameArray.length - 1]}`;
}