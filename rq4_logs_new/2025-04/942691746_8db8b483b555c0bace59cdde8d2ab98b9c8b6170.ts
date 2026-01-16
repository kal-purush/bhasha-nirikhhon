import { GAME_CONSTANTS } from "../Constant/constant";
import { Duck } from "../Types/Duck";

function updateDuckSprite(duck: Duck, duckElement: HTMLImageElement): void {
    // Chuyển sprite bay thay vì đi bộ
    if (duck.direction.x === -1) {
        duckElement.src = `../assets/duck/fly/a${duck.frame+ 2}.png`; // a1, a2
    } else {
        duckElement.src = `../assets/duck/fly/a${duck.frame}.png`; // a3, a4
    }

    // Đổi frame liên tục để vỗ cánh
    duck.frame = duck.frame === 1 ? 2 : 1;
}

export function moveFly(duck: Duck): void {
    const { DUCK } = GAME_CONSTANTS;

    // Di chuyển theo hướng x như linear
    duck.position.left += duck.direction.x * duck.speed;

    // Bay nhẹ lên/xuống
    duck.position.top += Math.sin(Date.now() * 0.005) * 0.5;

    // Giới hạn ranh giới
    if (duck.position.left >= DUCK.MAX_LEFT || duck.position.left <= DUCK.MIN_LEFT) {
        duck.direction.x *= -1;
    }
    if (duck.position.top >= DUCK.MAX_TOP || duck.position.top <= DUCK.MIN_TOP) {
        duck.direction.y *= -1;
    }

    // Cập nhật sprite bay
    const duckElement = document.getElementById(duck.id) as HTMLImageElement;
    if (duckElement) updateDuckSprite(duck, duckElement);
}