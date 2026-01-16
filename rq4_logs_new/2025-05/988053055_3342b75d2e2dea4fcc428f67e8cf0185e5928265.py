import argparse
import os
import numpy as np
import torch
import matplotlib.pyplot as plt
from typing import Dict, List

# ────────────────────────────── 기본 상수 ────────────────────────────── #
JOINT_LABELS = [
    "wrist", "thumbKnuckle", "thumbIntermediateBase", "thumbIntermediateTip", "thumbTip",
    "indexFingerMetacarpal", "indexFingerKnuckle", "indexFingerIntermediateBase",
    "indexFingerIntermediateTip", "indexFingerTip",
    "middleFingerMetacarpal", "middleFingerKnuckle", "middleFingerIntermediateBase",
    "middleFingerIntermediateTip", "middleFingerTip",
    "ringFingerMetacarpal", "ringFingerKnuckle", "ringFingerIntermediateBase",
    "ringFingerIntermediateTip", "ringFingerTip",
    "littleFingerMetacarpal", "littleFingerKnuckle", "littleFingerIntermediateBase",
    "littleFingerIntermediateTip", "littleFingerTip",
]
FINGER_JOINT_COUNTS = [4, 5, 5, 5, 5]

# ────────────────────────────── 유틸 함수 ────────────────────────────── #
def frames_from(mat: np.ndarray) -> List[np.ndarray]:
    arr = np.asarray(mat).squeeze()
    if arr.ndim == 0:
        return []
    flat = arr.ravel()
    if flat.size % 16 != 0:
        return []
    return [m.reshape(4, 4) for m in flat.reshape(-1, 16)]

def np2tensor(data: Dict[str, np.ndarray], device) -> Dict[str, torch.Tensor]:
    """VisionPro raw dict → torch tensor dict"""
    for key, val in data.items():
        if key in ["left_wrist", "right_wrist", "head"]:
            mats = frames_from(val)
            data[key] = (
                torch.tensor(np.stack(mats), dtype=torch.float32, device=device)
                if mats else torch.zeros((1, 4, 4), dtype=torch.float32, device=device)
            )
        elif key in ["left_fingers", "right_fingers"]:
            mats = frames_from(val)
            data[key] = [torch.tensor(m, dtype=torch.float32, device=device) for m in mats]
        else:
            data[key] = torch.tensor(val, dtype=torch.float32, device=device)
    return data

def draw_frame(ax, M, scale=0.1, label=None):
    origin, R = M[:3, 3], M[:3, :3]
    ax.quiver(*origin, *R[:, 0], length=scale, normalize=True, color="r")
    ax.quiver(*origin, *R[:, 1], length=scale, normalize=True, color="g")
    ax.quiver(*origin, *R[:, 2], length=scale, normalize=True, color="b")
    if label:
        ax.text(*origin, label, color="k", fontsize=8)

# ────────────────────────────── 시각화 환경 ────────────────────────────── #
class MatplotlibVisualizerEnv:
    def __init__(self, args):
        self.args = args
        self.device = "cpu"
        self.fig = plt.figure()
        self.ax = self.fig.add_subplot(111, projection="3d")
        plt.ion()

        # 키 입력-콜백 등록
        self.fig.canvas.mpl_connect("key_press_event", self.on_key)
        # 최신 프레임을 외부에서 밀어넣도록 임시 버퍼
        self.latest_transformation = None

    # 메인 루프가 최신 프레임을 넣어 줌
    def step(self, transformation: Dict[str, torch.Tensor]):
        self.latest_transformation = transformation
        self.render(transformation)

    # ── 키보드 이벤트 ── #
    def on_key(self, event):
        if event.key in ("l", "r"):
            if self.latest_transformation is None:
                print("No data yet!")
                return
            self.save_hand(self.latest_transformation, "left" if event.key == "l" else "right")

    # ── 저장 루틴 ── #
    def save_hand(self, transformation: Dict[str, torch.Tensor], hand: str):
        save_dir = self.args.save_dir
        os.makedirs(save_dir, exist_ok=True)
        key = "left" if hand == "left" else "right"

        # 기존 파일 파악 → 다음 번호 계산
        files = [f for f in os.listdir(save_dir)
                 if f.startswith(f"{key}_hand_data_") and f.endswith(".npz")]
        next_num = 1 if not files else max(int(f.split('_')[-1].split('.')[0]) for f in files) + 1
        out_file = os.path.join(save_dir, f"{key}_hand_data_{next_num:03d}.npz")

        hand_data = {
            f"{key}_wrist": transformation.get(f"{key}_wrist").cpu().numpy(),
            f"{key}_fingers": np.stack([m.cpu().numpy() for m in transformation.get(f"{key}_fingers", [])]),
            f"{key}_pinch_distance": transformation.get(f"{key}_pinch_distance", torch.zeros(1)).cpu().numpy(),
            f"{key}_wrist_roll": transformation.get(f"{key}_wrist_roll", torch.zeros(1)).cpu().numpy(),
        }
        np.savez(out_file, **hand_data)
        print(f"[{key.upper()}] Saved hand pose → {out_file}")

    # ── 렌더링 ── #
    def render(self, transformation: Dict[str, torch.Tensor]):
        ax = self.ax
        ax.cla()
        ax.set_xlim(-0.7, 0.7); ax.set_ylim(-0.7, 0.7); ax.set_zlim(0.0, 2.0)
        ax.set_title("3D Hand Pose"); ax.set_xlabel("X"); ax.set_ylabel("Y"); ax.set_zlabel("Z")

        # 머리
        head = transformation.get("head")
        if head is not None:
            h_pose = head[0]
            head_pos = h_pose[:3, 3].cpu().numpy()
            draw_frame(ax, h_pose.cpu().numpy(), scale=0.08, label="Head")
            ax.scatter(*head_pos, c="red", s=50); ax.text(*head_pos, "H", color="red", fontsize=10)

        # 양손
        for side, color in [("left", "green"), ("right", "blue")]:
            wrist_key, finger_key = f"{side}_wrist", f"{side}_fingers"
            wrist = transformation.get(wrist_key); fingers = transformation.get(finger_key, [])
            if wrist is None or not fingers:   # 데이터 없으면 스킵
                continue

            w_pose = wrist[0]; wrist_pos = w_pose[:3, 3].cpu().numpy()
            draw_frame(ax, w_pose.cpu().numpy(), scale=0.1, label=f"{side}_wrist")

            positions = [wrist_pos]
            for rel in fingers:
                abs_pose = w_pose @ rel
                positions.append(abs_pose[:3, 3].cpu().numpy())
                ax.scatter(*positions[-1], c=color, s=20)

            skip = {(5, 6), (10, 11), (15, 16), (20, 21)}  # 손바닥-메타카팔 skip
            for i in range(len(positions) - 1):
                if (i, i + 1) in skip:
                    continue
                ax.plot([positions[i][0], positions[i+1][0]],
                        [positions[i][1], positions[i+1][1]],
                        [positions[i][2], positions[i+1][2]],
                        color=color, linewidth=2)

        plt.draw(); plt.pause(0.001)

# ────────────────────────────── 상위 래퍼 ────────────────────────────── #
class MatplotlibVisualizer:
    def __init__(self, args):
        from avp_stream import VisionProStreamer
        self.streamer = VisionProStreamer(args.ip, args.record)
        self.env = MatplotlibVisualizerEnv(args)

    def run(self):
        while plt.fignum_exists(self.env.fig.number):
            latest = self.streamer.latest   # Dict[str, np.ndarray]
            if not latest:
                plt.pause(0.01); continue
            tensor_data = np2tensor(latest, self.env.device)
            self.env.step(tensor_data)

# ────────────────────────────── main ────────────────────────────── #
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="192.168.0.70")
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--save_dir", type=str, default="data")
    parser.add_argument("--load", type=str, help=".npz file to visualise (offline)")
    args = parser.parse_args()

    if args.load:
        # 오프라인 시각화
        data = dict(np.load(args.load, allow_pickle=True))
        # npz 안의 dict 키들이 그대로 transformation 역할
        tensor_data = np2tensor(data, "cpu")
        env = MatplotlibVisualizerEnv(args)
        env.step(tensor_data)
        print("Press any key in the figure window to close.")
        plt.ioff(); plt.show()
    else:
        vis = MatplotlibVisualizer(args)
        print("Figure focused 상태에서  [l] : 왼손  /  [r] : 오른손  저장")
        vis.run()