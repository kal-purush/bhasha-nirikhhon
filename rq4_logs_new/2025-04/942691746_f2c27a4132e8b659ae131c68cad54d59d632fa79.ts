// Tutorial.ts
// Định nghĩa các bước hướng dẫn game

// Interface định nghĩa một bước hướng dẫn
interface TutorialStep {
    message: string;          // Nội dung tin nhắn hướng dẫn
    targetElement: string;    // ID hoặc class của phần tử cần highlight
    position?: 'top' | 'bottom' | 'left' | 'right'; // Vị trí hiển thị tin nhắn
}

// Danh sách các bước hướng dẫn
const tutorialSteps: TutorialStep[] = [
    {
        message: "Chào mừng bạn đến với Duck Egg Hunt! Hãy khám phá trò chơi qua hướng dẫn này.",
        targetElement: "body",
        position: "top"
    },
    {
        message: "Đây là khu vực hiển thị số lượng trứng và xu bạn đã thu thập được.",
        targetElement: ".counter-container",
        position: "bottom"
    },
    {
        message: "Nhấp vào túi để mở cửa hàng. Tại đây bạn có thể đổi trứng lấy xu và mua vịt.",
        targetElement: ".icon-bag",
        position: "right"
    },
    {
        message: "Nhấp vào biểu tượng này để xem thông tin hợp đồng và thực hiện giao dịch.",
        targetElement: ".icon-contract",
        position: "right"
    },
    {
        message: "Xem danh sách nhiệm vụ hàng ngày ở đây để nhận thưởng.",
        targetElement: ".icon-task",
        position: "left"
    },
    {
        message: "Điều chỉnh âm lượng game tại đây.",
        targetElement: ".settings-btn",
        position: "left"
    },
    {
        message: "Thông tin tài khoản của bạn. Nhấp vào để xem chi tiết.",
        targetElement: ".user-profile",
        position: "left"
    },
    {
        message: "Bạn đã sẵn sàng! Giờ bạn có thể bắt đầu chơi và thu thập trứng. Chúc vui vẻ!",
        targetElement: "body",
        position: "top"
    }
];

// Lớp quản lý hướng dẫn
class TutorialManager {
    private currentStep = 0;
    private overlay: HTMLElement;
    private message: HTMLElement;
    private nextButton: HTMLElement;
    private startButton: HTMLElement;
    private isActive = false;
    private highlightElement: HTMLElement | null = null;

    constructor() {
        this.overlay = document.getElementById('tutorialOverlay') as HTMLElement;
        this.message = document.getElementById('tutorialMessage') as HTMLElement;
        this.nextButton = document.getElementById('nextTutorialBtn') as HTMLElement;
        this.startButton = document.getElementById('startTutorialBtn') as HTMLElement;

        this.initEventListeners();
    }

    private initEventListeners(): void {
        // Nút bắt đầu hướng dẫn
        this.startButton.addEventListener('click', () => this.startTutorial());
        
        // Nút tiếp tục sang bước tiếp theo
        this.nextButton.addEventListener('click', () => this.nextStep());

        // Tùy chọn: Cho phép nhấp vào overlay để tiếp tục
        this.overlay.addEventListener('click', (e) => {
            if (e.target === this.overlay) {
                this.nextStep();
            }
        });
    }

    // Bắt đầu hướng dẫn
    public startTutorial(): void {
        if (this.isActive) return;
        
        this.isActive = true;
        this.currentStep = 0;
        this.showStep();
    }

    // Chuyển sang bước tiếp theo
    private nextStep(): void {
        this.currentStep++;
        
        if (this.currentStep < tutorialSteps.length) {
            this.showStep();
        } else {
            this.endTutorial();
        }
    }

    // Hiển thị bước hiện tại
    private showStep(): void {
        const step = tutorialSteps[this.currentStep];
        this.message.textContent = step.message;
        
        // Hiển thị overlay
        this.overlay.style.display = 'flex';
        
        // Highlight phần tử được chỉ định
        this.highlightElement = this.createHighlight(step.targetElement);
        
        // Định vị tin nhắn hướng dẫn tương đối với phần tử được highlight
        this.positionMessage(this.highlightElement, step.position || 'bottom');
    }

    // Tạo hiệu ứng highlight cho phần tử
    private createHighlight(selector: string): HTMLElement {
        // Xóa highlight cũ nếu có
        if (this.highlightElement) {
            document.body.removeChild(this.highlightElement);
        }

        // Tìm phần tử cần highlight
        const targetElement = selector === 'body' 
            ? document.body 
            : document.querySelector(selector) as HTMLElement;
        
        if (!targetElement) {
            throw new Error(`Target element not found for selector: ${selector}`);
        }

        // Lấy vị trí và kích thước của phần tử
        const rect = targetElement.getBoundingClientRect();
        
        // Tạo phần tử highlight
        const highlight = document.createElement('div');
        highlight.className = 'tutorial-highlight';
        highlight.style.position = 'absolute';
        highlight.style.left = `${rect.left + window.scrollX}px`;
        highlight.style.top = `${rect.top + window.scrollY}px`;
        highlight.style.width = `${rect.width}px`;
        highlight.style.height = `${rect.height}px`;
        highlight.style.border = '3px solid #FFD700';
        highlight.style.borderRadius = '5px';
        highlight.style.boxShadow = '0 0 0 9999px rgba(0, 0, 0, 0.5)';
        highlight.style.zIndex = '9998';
        highlight.style.pointerEvents = 'none';
        
        document.body.appendChild(highlight);
        
        return highlight;
    }

    // Định vị tin nhắn hướng dẫn
    private positionMessage(targetElement: HTMLElement, position: string): void {
        if (!targetElement) return;
        
        const rect = targetElement.getBoundingClientRect();
        const messageRect = this.message.getBoundingClientRect();
        
        // Tính toán vị trí dựa trên tùy chọn
        let top, left;
        
        switch (position) {
            case 'top':
                top = rect.top - messageRect.height - 20;
                left = rect.left + (rect.width - messageRect.width) / 2;
                break;
            case 'bottom':
                top = rect.bottom + 20;
                left = rect.left + (rect.width - messageRect.width) / 2;
                break;
            case 'left':
                top = rect.top + (rect.height - messageRect.height) / 2;
                left = rect.left - messageRect.width - 20;
                break;
            case 'right':
                top = rect.top + (rect.height - messageRect.height) / 2;
                left = rect.right + 20;
                break;
            default:
                top = rect.bottom + 20;
                left = rect.left + (rect.width - messageRect.width) / 2;
        }
        
        // Đảm bảo tin nhắn luôn nằm trong viewport
        const viewportWidth = window.innerWidth;
        const viewportHeight = window.innerHeight;
        
        if (left < 10) left = 10;
        if (left + messageRect.width > viewportWidth - 10) left = viewportWidth - messageRect.width - 10;
        if (top < 10) top = 10;
        if (top + messageRect.height > viewportHeight - 10) top = viewportHeight - messageRect.height - 10;
        
        // Cập nhật vị trí của tin nhắn
        this.message.style.position = 'absolute';
        this.message.style.top = `${top}px`;
        this.message.style.left = `${left}px`;
    }

    // Kết thúc hướng dẫn
    private endTutorial(): void {
        this.isActive = false;
        
        // Ẩn overlay
        this.overlay.style.display = 'none';
        
        // Xóa highlight
        if (this.highlightElement) {
            document.body.removeChild(this.highlightElement);
            this.highlightElement = null;
        }
        
        // Lưu trạng thái đã hoàn thành hướng dẫn
        localStorage.setItem('tutorialCompleted', 'true');
    }

    // Kiểm tra xem người dùng đã hoàn thành hướng dẫn chưa
    public checkTutorialStatus(): void {
        const completed = localStorage.getItem('tutorialCompleted') === 'true';
        
        // Nếu đây là lần đầu tiên người dùng truy cập, tự động hiển thị hướng dẫn
        if (!completed && !this.isActive) {
            // Đợi một chút để đảm bảo trang đã tải xong
            setTimeout(() => this.startTutorial(), 1000);
        }
    }
}

// Khởi tạo và xuất đối tượng quản lý hướng dẫn
const tutorialManager = new TutorialManager();

// Kiểm tra trạng thái hướng dẫn khi trang được tải
document.addEventListener('DOMContentLoaded', () => {
    tutorialManager.checkTutorialStatus();
});

export default tutorialManager;