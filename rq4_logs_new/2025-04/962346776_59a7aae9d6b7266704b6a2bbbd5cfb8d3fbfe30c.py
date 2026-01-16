import tkinter as tk
from tkinter import ttk, messagebox, font
import base64
import pyperclip  # For clipboard functionality

class EmojiConverterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Emoji String Converter")
        self.root.geometry("850x650")
        self.root.minsize(650, 450)
        
        # Configure styles
        self.style = ttk.Style()
        self.style.configure('TFrame', background='#f0f0f0')
        self.style.configure('TButton', font=('Helvetica', 10), padding=5)
        self.style.configure('TLabel', background='#f0f0f0', font=('Helvetica', 10))
        self.style.configure('Header.TLabel', font=('Helvetica', 12, 'bold'))
        
        # Emoji set
        self.emoji_set = [
            '😀', '😃', '😄', '😁', '😆', '😅', '😂', '🤣', '😊', '😇', 
            '🙂', '🙃', '😉', '😌', '😍', '🥰', '😘', '😗', '😙', '😚', 
            '😋', '😛', '😝', '😜', '🤪', '🤨', '🧐', '🤓', '😎', '🥸', 
            '🤩', '🥳', '😏', '😒', '😞', '😔', '😟', '😕', '🙁', '☹️', 
            '😣', '😖', '😫', '😩', '🥺', '😢', '😭', '😤', '😠', '😡', 
            '🤬', '🤯', '😳', '🥵', '🥶', '😱', '😨', '😰', '😥', '😓', 
            '🫣', '🤗', '🫡', '🤔', '🫢', '🤭', '🤫', '🤥', '😶', '😶‍🌫️', 
            '😐', '😑', '😬', '🫠', '🙄', '😯', '😦', '😧', '😮', '😲', 
            '🥱', '😴', '🤤', '😪', '😵', '😵‍💫', '🫥', '🤐', '🥴', '🤢', 
            '🤮', '🤧', '😷', '🤒', '🤕', '🤑', '🤠', '😈', '👿', '👹', 
            '👺', '🤡', '💩', '👻', '💀', '☠️', '👽', '👾', '🤖', '🎃', 
            '😺', '😸', '😹', '😻', '😼', '😽', '🙀', '😿', '😾', '🙈', 
            '🙉', '🙊', '💌', '💘', '💝', '💖', '💗', '💓', '💞', '💕', 
            '💟', '❣️', '💔', '❤️‍🔥', '❤️‍🩹', '❤️', '🧡', '💛', '💚', '💙', 
            '💜', '🤎', '🖤', '🤍', '🤲', '👐', '🙌', '👏', '🤝', '👍', 
            '👎', '👊', '✊', '🤛', '🤜', '🤞', '✌️', '🤟', '🤘', '👌', 
            '🤌', '🤏', '👈', '👉', '👆', '👇', '☝️', '✋', '🤚', '🖐️', 
            '🖖', '👋', '🤙', '💪', '🦾', '🖕', '✍️', '🙏', '🦶', '🦵', 
            '🦿', '💄', '💋', '👄', '🦷', '👅', '👂', '🦻', '👃', '👣', 
            '👁️', '👀', '🧠', '🫀', '🫁', '🦴', '👓', '🕶️', '🥽', '🥼', 
            '🦺', '👔', '👕', '👖', '🧣', '🧤', '🧥', '🧦', '👗', '👘', 
            '🥻', '🩱', '🩲', '🩳', '👙', '👚', '👛', '👜', '👝', '🎒', 
            '👞', '👟', '🥾', '🥿', '👠', '👡', '🩰', '👢', '👑', '👒', 
            '🎩', '🎓', '🧢', '⛑️', '💍', '💼', '🌂', '🧳', '☂️', '💎'
        ]
        
        self.create_widgets()
    
    def create_widgets(self):
        # Main container
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Header
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 15))
        
        ttk.Label(header_frame, text="Emoji String Converter", 
                 style='Header.TLabel').pack()
        ttk.Label(header_frame, text="Convert between text and emoji sequences").pack()
        
        # Text sections container
        text_container = ttk.Frame(main_frame)
        text_container.pack(fill=tk.BOTH, expand=True)
        
        # Normal text section
        normal_frame = ttk.LabelFrame(text_container, text="Normal Text", padding=10)
        normal_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.normal_text = tk.Text(normal_frame, height=8, wrap=tk.WORD,
                                 font=('Helvetica', 11), padx=10, pady=10)
        self.normal_text.pack(fill=tk.BOTH, expand=True)
        
        # Normal text buttons
        normal_btn_frame = ttk.Frame(normal_frame)
        normal_btn_frame.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Button(normal_btn_frame, text="Copy Text", 
                  command=self.copy_normal_text).pack(side=tk.LEFT, padx=2)
        ttk.Button(normal_btn_frame, text="Paste Text", 
                  command=self.paste_normal_text).pack(side=tk.LEFT, padx=2)
        
        # Emoji text section
        emoji_frame = ttk.LabelFrame(text_container, text="Emoji Text", padding=10)
        emoji_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        self.emoji_text = tk.Text(emoji_frame, height=8, wrap=tk.WORD,
                                font=('Helvetica', 14), padx=10, pady=10)
        self.emoji_text.pack(fill=tk.BOTH, expand=True)
        
        # Emoji text buttons
        emoji_btn_frame = ttk.Frame(emoji_frame)
        emoji_btn_frame.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Button(emoji_btn_frame, text="Copy Emojis", 
                  command=self.copy_emoji_text).pack(side=tk.LEFT, padx=2)
        ttk.Button(emoji_btn_frame, text="Paste Emojis", 
                  command=self.paste_emoji_text).pack(side=tk.LEFT, padx=2)
        
        # Action buttons
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=15)
        
        ttk.Button(action_frame, text="Encode to Emoji", 
                  command=self.encode_to_emoji).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="Decode from Emoji", 
                  command=self.decode_from_emoji).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="Clear All", 
                  command=self.clear_all).pack(side=tk.RIGHT, padx=5)
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, 
                             relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(fill=tk.X, pady=(10, 0))
    
    # Clipboard functions
    def copy_normal_text(self):
        text = self.normal_text.get("1.0", tk.END).strip()
        if text:
            pyperclip.copy(text)
            self.status_var.set("Text copied to clipboard!")
        else:
            self.status_var.set("No text to copy")
    
    def paste_normal_text(self):
        try:
            text = pyperclip.paste()
            if text:
                self.normal_text.delete("1.0", tk.END)
                self.normal_text.insert(tk.END, text)
                self.status_var.set("Text pasted from clipboard!")
            else:
                self.status_var.set("Clipboard is empty")
        except:
            self.status_var.set("Clipboard access error")
    
    def copy_emoji_text(self):
        emojis = self.emoji_text.get("1.0", tk.END).strip()
        if emojis:
            pyperclip.copy(emojis)
            self.status_var.set("Emojis copied to clipboard!")
        else:
            self.status_var.set("No emojis to copy")
    
    def paste_emoji_text(self):
        try:
            emojis = pyperclip.paste()
            if emojis:
                self.emoji_text.delete("1.0", tk.END)
                self.emoji_text.insert(tk.END, emojis)
                self.status_var.set("Emojis pasted from clipboard!")
            else:
                self.status_var.set("Clipboard is empty")
        except:
            self.status_var.set("Clipboard access error")
    
    # Conversion functions
    def encode_to_emoji(self):
        try:
            text = self.normal_text.get("1.0", tk.END).strip()
            if not text:
                messagebox.showwarning("Warning", "Please enter text to encode.")
                return
            
            text_bytes = text.encode('utf-8')
            base64_bytes = base64.b64encode(text_bytes)
            
            self.emoji_text.delete("1.0", tk.END)
            
            # Convert each byte to an emoji
            emoji_str = ''.join([self.emoji_set[b % len(self.emoji_set)] for b in base64_bytes])
            self.emoji_text.insert(tk.END, emoji_str)
            
            self.status_var.set("Text successfully encoded to emojis!")
        except Exception as e:
            messagebox.showerror("Error", f"Encoding error: {str(e)}")
            self.status_var.set("Encoding error")
    
    def decode_from_emoji(self):
        try:
            emoji_str = self.emoji_text.get("1.0", tk.END).strip()
            if not emoji_str:
                messagebox.showwarning("Warning", "Please enter emojis to decode.")
                return
            
            # Create reverse mapping from emoji to index
            emoji_to_index = {emoji: idx for idx, emoji in enumerate(self.emoji_set)}
            
            # Convert each emoji back to its index
            indices = []
            unknown_emojis = set()
            
            for emoji in emoji_str:
                if emoji in emoji_to_index:
                    indices.append(emoji_to_index[emoji])
                else:
                    unknown_emojis.add(emoji)
            
            if unknown_emojis:
                messagebox.showwarning("Warning", 
                    f"Unknown emojis found: {', '.join(unknown_emojis)}\n"
                    "These will be ignored in decoding.")
                self.status_var.set(f"Warning: {len(unknown_emojis)} unknown emojis ignored")
            
            # Convert indices back to base64 bytes
            base64_bytes = bytes(indices)
            
            # Decode from base64 to original text
            try:
                text_bytes = base64.b64decode(base64_bytes)
                text = text_bytes.decode('utf-8')
                
                self.normal_text.delete("1.0", tk.END)
                self.normal_text.insert(tk.END, text)
                self.status_var.set("Emojis successfully decoded to text!")
            except base64.binascii.Error:
                messagebox.showerror("Error", "The emojis don't represent valid encoded data.")
                self.status_var.set("Decoding error - invalid data")
        except Exception as e:
            messagebox.showerror("Error", f"Decoding error: {str(e)}")
            self.status_var.set("Decoding error")
    
    def clear_all(self):
        self.normal_text.delete("1.0", tk.END)
        self.emoji_text.delete("1.0", tk.END)
        self.status_var.set("Cleared all fields")

if __name__ == "__main__":
    try:
        import pyperclip
    except ImportError:
        print("Please install pyperclip: pip install pyperclip")
        exit()
    
    root = tk.Tk()
    app = EmojiConverterApp(root)
    root.mainloop()