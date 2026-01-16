import os
import hashlib
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import time
import subprocess
from datetime import datetime
import stat
import mimetypes
import pwd
import sys

try:
    import psutil
except ImportError:
    print("Warning: psutil not available. Drive info will be limited.")
    psutil = None


class FileToolsApp:
    def __init__(self, master):
        self.master = master
        master.title("Duplicate File Finder Pro v1.0")
        master.configure(bg="#f0f4f7")
        master.geometry("1000x700")

        self.create_menu_bar()

        master.columnconfigure(1, weight=1)
        master.columnconfigure(2, weight=1)
        master.rowconfigure(5, weight=1)

        self.style = ttk.Style()
        self.style.configure("TProgressbar", thickness=20)

        # Directory Section
        self.directory_label = tk.Label(master, text="Directory:", font=("Arial", 12, "bold"), bg="#f0f4f7")
        self.directory_label.grid(row=1, column=0, sticky="w", padx=15, pady=10)

        self.directory_entry = tk.Entry(master, width=50, font=("Arial", 12), relief="solid", borderwidth=1)
        self.directory_entry.grid(row=1, column=1, columnspan=2, sticky="ew", padx=15, pady=10)
        self.directory_entry.insert(0, "/")

        self.browse_dir_button = tk.Button(master, text="Browse", command=self.browse_directory,
                                           width=15, font=("Arial", 12), bg="#4CAF50", fg="white",
                                           relief="flat", activebackground="#45a049")
        self.browse_dir_button.grid(row=1, column=3, padx=15, pady=10)

        # Buttons Frame
        self.button_frame = tk.Frame(master, bg="#f0f4f7")
        self.button_frame.grid(row=2, column=0, columnspan=4, pady=10)

        self.find_button = tk.Button(self.button_frame, text="Find Duplicates", command=self.start_duplicate_scan,
                                     width=20, font=("Arial", 12), bg="#008CBA", fg="white",
                                     relief="flat", activebackground="#007B9A")
        self.find_button.pack(side=tk.LEFT, padx=15)

        self.full_disk_button = tk.Button(self.button_frame, text="Scan Drives", command=self.select_drives,
                                          width=20, font=("Arial", 12), bg="#f44336", fg="white",
                                          relief="flat", activebackground="#da190b")
        self.full_disk_button.pack(side=tk.LEFT, padx=15)

        # Progress Section
        self.progress_frame = tk.Frame(master, bg="#f0f4f7")
        self.progress_frame.grid(row=3, column=0, columnspan=4, sticky="ew", padx=15, pady=10)

        self.progress_label = tk.Label(self.progress_frame, text="Scanning Progress:",
                                       font=("Arial", 12, "bold"), bg="#f0f4f7")
        self.progress_label.pack(side=tk.LEFT)

        self.progress_bar = ttk.Progressbar(self.progress_frame, length=600, mode="determinate")
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)

        # Status Section
        self.status_frame = tk.Frame(master, bg="#f0f4f7")
        self.status_frame.grid(row=4, column=0, columnspan=4, sticky="ew", padx=15, pady=10)

        self.status_label = tk.Label(self.status_frame, text="Status: Ready", font=("Arial", 11), bg="#f0f4f7")
        self.status_label.pack(side=tk.LEFT)

        self.time_label = tk.Label(self.status_frame, text="Estimated Time: N/A", font=("Arial", 11), bg="#f0f4f7")
        self.time_label.pack(side=tk.RIGHT)

        # Results Section
        self.result_frame = tk.Frame(master, bg="#f0f4f7")
        self.result_frame.grid(row=5, column=0, columnspan=4, sticky="nsew", padx=15, pady=10)

        # Action Buttons Frame
        self.action_frame = tk.Frame(self.result_frame, bg="#f0f4f7")
        self.action_frame.pack(side=tk.BOTTOM, fill="x", pady=5)

        self.view_button = tk.Button(self.action_frame, text="View Selected", command=self.view_selected_files,
                                     width=15, font=("Arial", 10), bg="#2196F3", fg="white",
                                     relief="flat", activebackground="#1976D2")
        print("Created view_button in __init__")
        self.view_button.pack(side=tk.LEFT, padx=5)

        self.delete_button = tk.Button(self.action_frame, text="Delete Selected", command=self.delete_selected_files,
                                       width=15, font=("Arial", 10), bg="#ff4444", fg="white",
                                       relief="flat", activebackground="#cc0000")
        print("Created delete_button in __init__")
        self.delete_button.pack(side=tk.LEFT, padx=5)

        self.metadata_button = tk.Button(self.action_frame, text="Show Metadata", command=self.show_metadata,
                                         width=15, font=("Arial", 10), bg="#8BC34A", fg="white",
                                         relief="flat", activebackground="#7CB342")
        print("Created metadata_button in __init__")
        self.metadata_button.pack(side=tk.LEFT, padx=5)

        self.duplicates_dict = {}
        self.is_scanning = False
        self.original_hashes = {}
        self.tree = None

    def create_menu_bar(self):
        menu_bar = tk.Menu(self.master)
        self.master.config(menu=menu_bar)

        help_menu = tk.Menu(menu_bar, tearoff=0)
        menu_bar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About Us", command=self.show_about)
        help_menu.add_command(label="Contact Us", command=self.show_contact)
        help_menu.add_separator()
        help_menu.add_command(label="Exit", command=self.master.quit)

    def show_about(self):
        about_text = """Duplicate File Finder Pro v1.0
Created by: xAI Team
Release Date: March 13, 2025
Purpose: Efficiently find and manage duplicate files on your system"""
        messagebox.showinfo("About Us", about_text)

    def show_contact(self):
        contact_text = """Contact Us:
Email: support@xai.com
Website: www.xai.com/filefinder
Support Hours: 9 AM - 5 PM EST, Monday-Friday"""
        messagebox.showinfo("Contact Us", contact_text)

    def browse_directory(self):
        directory = filedialog.askdirectory(initialdir="/", title="Select Directory")
        if directory:
            self.directory_entry.delete(0, tk.END)
            self.directory_entry.insert(0, directory)

    def start_duplicate_scan(self):
        if self.is_scanning:
            return
        directory = self.directory_entry.get()
        if not directory:
            messagebox.showerror("Error", "Please select a directory.")
            return
        if not os.path.exists(directory):
            messagebox.showerror("Error", "The selected directory does not exist.")
            return

        self.prepare_scan()
        scan_thread = threading.Thread(target=self.find_duplicates, args=(directory,))
        scan_thread.daemon = True
        scan_thread.start()

    def select_drives(self):
        if self.is_scanning:
            return

        drives = self.get_all_drives_with_info()
        if not drives:
            messagebox.showerror("Error", "No drives detected on the system.")
            return

        drive_window = tk.Toplevel(self.master)
        drive_window.title("Select Drives to Scan")
        drive_window.geometry("500x600")
        drive_window.transient(self.master)
        drive_window.grab_set()
        drive_window.configure(bg="#f0f4f7")

        header_frame = tk.Frame(drive_window, bg="#f0f4f7")
        header_frame.pack(fill="x", padx=10, pady=10)

        tk.Label(header_frame, text="Available Drives", font=("Arial", 14, "bold"),
                 bg="#f0f4f7").pack(anchor="w")
        tk.Label(header_frame, text="Select drives to scan for duplicates",
                 font=("Arial", 10), bg="#f0f4f7").pack(anchor="w")

        drive_frame = tk.Frame(drive_window, bg="#f0f4f7")
        drive_frame.pack(fill="both", expand=True, padx=10, pady=5)

        canvas = tk.Canvas(drive_frame, bg="#f0f4f7")
        scrollbar = tk.Scrollbar(drive_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg="#f0f4f7")

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        selected_drives = []

        for drive_info in drives:
            drive_container = tk.Frame(scrollable_frame, bg="white", bd=1, relief="solid")
            drive_container.pack(fill="x", pady=5, padx=5)

            var = tk.BooleanVar()
            chk = tk.Checkbutton(drive_container, variable=var, bg="white")
            chk.pack(side=tk.LEFT, padx=5)

            details_frame = tk.Frame(drive_container, bg="white")
            details_frame.pack(side=tk.LEFT, fill="x", expand=True)

            drive_label = f"{drive_info['letter']} - {drive_info['name']}"
            tk.Label(details_frame, text=drive_label, font=("Arial", 11, "bold"),
                     bg="white").pack(anchor="w")

            info_text = f"Type: {drive_info['type']} | Size: {self.format_size(drive_info['total'])} | Free: {self.format_size(drive_info['free'])}"
            tk.Label(details_frame, text=info_text, font=("Arial", 10),
                     bg="white").pack(anchor="w")

            selected_drives.append((drive_info['letter'], var))

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        button_frame = tk.Frame(drive_window, bg="#f0f4f7")
        button_frame.pack(fill="x", pady=10)

        def start_scan():
            selected = [drive for drive, var in selected_drives if var.get()]
            if not selected:
                messagebox.showerror("Error", "Please select at least one drive to scan.")
                return
            drive_window.destroy()
            self.prepare_scan()
            scan_thread = threading.Thread(target=self.scan_selected_drives, args=(selected,))
            scan_thread.daemon = True
            scan_thread.start()

        tk.Button(button_frame, text="Start Scan", command=start_scan,
                  font=("Arial", 11), bg="#4CAF50", fg="white",
                  relief="flat", width=15).pack(side=tk.LEFT, padx=5)

        tk.Button(button_frame, text="Cancel", command=drive_window.destroy,
                  font=("Arial", 11), bg="#f44336", fg="white",
                  relief="flat", width=15).pack(side=tk.LEFT, padx=5)

    def get_all_drives_with_info(self):
        drives = []
        if psutil:
            partitions = psutil.disk_partitions()
            for partition in partitions:
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    drives.append({
                        'letter': partition.mountpoint,
                        'name': partition.device.split('/')[-1] if partition.device else "Disk",
                        'type': partition.fstype,
                        'total': usage.total,
                        'free': usage.free
                    })
                except:
                    drives.append({
                        'letter': partition.mountpoint,
                        'name': "Disk",
                        'type': "Unknown",
                        'total': 0,
                        'free': 0
                    })
        else:
            drives = [{'letter': '/', 'name': "Root", 'type': "Unknown", 'total': 0, 'free': 0}]
        return drives

    def prepare_scan(self):
        self.is_scanning = True
        self.clear_results()
        self.progress_bar["value"] = 0
        self.status_label.config(text="Status: Scanning started...")
        self.time_label.config(text="Estimated Time: Calculating...")
        self.find_button.config(state="disabled")
        self.full_disk_button.config(state="disabled")
        self.view_button.config(state="disabled")
        self.delete_button.config(state="disabled")
        self.metadata_button.config(state="disabled")

    def scan_selected_drives(self, drives):
        duplicates = {}
        for drive in drives:
            self.status_label.config(text=f"Status: Scanning drive {drive}")
            self.master.update_idletasks()
            drive_duplicates = self.find_duplicates(drive, suppress_results=True)
            duplicates.update(drive_duplicates)
        self.display_results_and_handle_deletion(duplicates)

    def find_duplicates(self, directory, suppress_results=False):
        file_hashes = {}
        duplicates = {}
        files_scanned = 0

        total_files = sum(len(files) for _, _, files in os.walk(directory))
        self.progress_bar["maximum"] = total_files if total_files > 0 else 1

        start_time = time.time()
        for dirpath, _, filenames in os.walk(directory):
            if not self.is_scanning:
                break
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                self.status_label.config(text=f"Status: Scanning {file_path}")
                self.master.update_idletasks()

                file_hash = self.calculate_file_hash(file_path)
                files_scanned += 1
                self.progress_bar["value"] = files_scanned

                elapsed_time = time.time() - start_time
                if files_scanned > 0 and elapsed_time > 0:
                    remaining_files = total_files - files_scanned
                    avg_time_per_file = elapsed_time / files_scanned
                    estimated_time_remaining = avg_time_per_file * remaining_files
                    self.update_time_label(estimated_time_remaining)

                if file_hash:
                    if file_hash not in file_hashes:
                        file_hashes[file_hash] = []
                    file_hashes[file_hash].append(file_path)

        for file_list in file_hashes.values():
            if len(file_list) > 1:
                original = self.find_original_file(file_list)
                duplicates[original] = file_list
                self.original_hashes[original] = self.calculate_file_hash(original, 'sha256')

        if not suppress_results:
            self.display_results_and_handle_deletion(duplicates)

        self.is_scanning = False
        self.find_button.config(state="normal")
        self.full_disk_button.config(state="normal")
        self.view_button.config(state="normal")
        self.delete_button.config(state="normal")
        self.metadata_button.config(state="normal")
        return duplicates

    def update_time_label(self, estimated_time_remaining):
        if estimated_time_remaining >= 0:
            minutes, seconds = divmod(int(estimated_time_remaining), 60)
            self.time_label.config(text=f"Estimated Time: {minutes}m {seconds}s")
            self.master.update_idletasks()

    def calculate_file_hash(self, filepath, hash_algo='sha256'):
        hasher = hashlib.new(hash_algo)
        try:
            with open(filepath, 'rb') as file:
                while chunk := file.read(8192):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except (IOError, OSError) as e:
            self.master.after(0, lambda: messagebox.showerror("Error", f"Could not hash {filepath}: {e}"))
            return None

    def get_file_metadata(self, filepath):
        try:
            stats = os.stat(filepath)
            file_type, _ = mimetypes.guess_type(filepath)
            _, file_extension = os.path.splitext(filepath)

            try:
                owner = pwd.getpwuid(stats.st_uid).pw_name
            except:
                owner = str(stats.st_uid)

            metadata = {
                'Path': filepath,
                'File Name': os.path.basename(filepath),
                'Extension': file_extension if file_extension else "None",
                'Size': self.format_size(stats.st_size),
                'Created': datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
                'Modified': datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'Accessed': datetime.fromtimestamp(stats.st_atime).strftime('%Y-%m-%d %H:%M:%S'),
                'Type': file_type if file_type else 'Unknown',
                'Owner': owner,
                'Permissions': oct(stats.st_mode)[-3:],
                'SHA256 Hash': self.original_hashes.get(filepath, 'N/A')
            }
            return metadata
        except Exception as e:
            return {'Error': str(e)}

    def show_metadata(self):
        selected_indices = self.tree.selection()
        if not selected_indices:
            messagebox.showwarning("Warning", "No files selected for metadata.")
            return

        for item in selected_indices:
            file_path = self.tree.item(item, "values")[0]
            metadata = self.get_file_metadata(file_path)

            metadata_window = tk.Toplevel(self.master)
            metadata_window.title(f"Metadata: {os.path.basename(file_path)}")
            metadata_window.geometry("500x400")
            metadata_window.transient(self.master)
            metadata_window.grab_set()
            metadata_window.configure(bg="#f0f4f7")

            header_frame = tk.Frame(metadata_window, bg="#f0f4f7")
            header_frame.pack(fill="x", padx=10, pady=(10, 5))

            tk.Label(header_frame, text="File Metadata", font=("Arial", 14, "bold"), bg="#f0f4f7").pack(anchor="w")
            tk.Label(header_frame, text=file_path, font=("Arial", 10, "italic"), bg="#f0f4f7", wraplength=480).pack(
                anchor="w")

            metadata_frame = tk.Frame(metadata_window, bg="#f0f4f7")
            metadata_frame.pack(fill="both", expand=True, padx=10, pady=5)

            canvas = tk.Canvas(metadata_frame, bg="#f0f4f7", highlightthickness=0)
            scrollbar = tk.Scrollbar(metadata_frame, orient="vertical", command=canvas.yview)
            scrollable_frame = tk.Frame(canvas, bg="#f0f4f7")

            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )

            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)

            row = 0
            for key, value in metadata.items():
                tk.Label(scrollable_frame, text=f"{key}:", font=("Arial", 10, "bold"), bg="#f0f4f7", anchor="w").grid(
                    row=row, column=0, sticky="w", padx=(5, 10), pady=2)
                tk.Label(scrollable_frame, text=value, font=("Arial", 10), bg="#f0f4f7", anchor="w",
                         wraplength=400).grid(row=row, column=1, sticky="w", pady=2)
                row += 1

            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            button_frame = tk.Frame(metadata_window, bg="#f0f4f7")
            button_frame.pack(fill="x", pady=10)

            def copy_to_clipboard():
                metadata_text = "\n".join(f"{key}: {value}" for key, value in metadata.items())
                self.master.clipboard_clear()
                self.master.clipboard_append(metadata_text)
                messagebox.showinfo("Success", "Metadata copied to clipboard!")

            tk.Button(button_frame, text="Copy to Clipboard", command=copy_to_clipboard,
                      font=("Arial", 10), bg="#2196F3", fg="white", relief="flat",
                      activebackground="#1976D2", width=15).pack(side=tk.LEFT, padx=5)

            tk.Button(button_frame, text="Close", command=metadata_window.destroy,
                      font=("Arial", 10), bg="#f44336", fg="white", relief="flat",
                      activebackground="#da190b", width=15).pack(side=tk.RIGHT, padx=5)

    def clear_results(self):
        self.result_frame.destroy()
        self.result_frame = tk.Frame(self.master, bg="#f0f4f7")
        self.result_frame.grid(row=5, column=0, columnspan=4, sticky="nsew", padx=15, pady=10)

        # Re-create action_frame
        self.action_frame = tk.Frame(self.result_frame, bg="#f0f4f7")
        self.action_frame.pack(side=tk.BOTTOM, fill="x", pady=5)

        self.view_button = tk.Button(self.action_frame, text="View Selected", command=self.view_selected_files,
                                     width=15, font=("Arial", 10), bg="#2196F3", fg="white",
                                     relief="flat", activebackground="#1976D2")
        print("Created view_button in clear_results")
        self.view_button.pack(side=tk.LEFT, padx=5)

        self.delete_button = tk.Button(self.action_frame, text="Delete Selected", command=self.delete_selected_files,
                                       width=15, font=("Arial", 10), bg="#ff4444", fg="white",
                                       relief="flat", activebackground="#cc0000")
        print("Created delete_button in clear_results")
        self.delete_button.pack(side=tk.LEFT, padx=5)

        self.metadata_button = tk.Button(self.action_frame, text="Show Metadata", command=self.show_metadata,
                                         width=15, font=("Arial", 10), bg="#8BC34A", fg="white",
                                         relief="flat", activebackground="#7CB342")
        print("Created metadata_button in clear_results")
        self.metadata_button.pack(side=tk.LEFT, padx=5)

        self.duplicates_dict = {}
        self.original_hashes = {}

    def display_results_and_handle_deletion(self, duplicates):
        self.status_label.config(text="Status: Scan completed")
        self.duplicates_dict = duplicates

        self.result_frame.destroy()
        self.result_frame = tk.Frame(self.master, bg="#f0f4f7")
        self.result_frame.grid(row=5, column=0, columnspan=4, sticky="nsew", padx=15, pady=10)

        # Header Frame
        header_frame = tk.Frame(self.result_frame, bg="#f0f4f7")
        header_frame.pack(fill="x", pady=5)

        tk.Label(header_frame, text="Sort by:", font=("Arial", 10, "bold"), bg="#f0f4f7").pack(side=tk.LEFT, padx=5)
        sort_var = tk.StringVar(value="Created")
        sort_options = ["Created", "Size"]
        sort_menu = ttk.OptionMenu(header_frame, sort_var, "Created", *sort_options,
                                   command=lambda v: self.sort_results(v, duplicates))
        sort_menu.pack(side=tk.LEFT, padx=5)

        tk.Label(header_frame, text="Filter:", font=("Arial", 10, "bold"), bg="#f0f4f7").pack(side=tk.LEFT, padx=5)
        filter_var = tk.StringVar(value="All")
        filter_options = ["All", "Originals", "Duplicates"]
        filter_menu = ttk.OptionMenu(header_frame, filter_var, "All", *filter_options,
                                     command=lambda v: self.filter_results(v, duplicates))
        filter_menu.pack(side=tk.LEFT, padx=5)

        # Results Frame
        results_frame = tk.Frame(self.result_frame, bg="#f0f4f7")
        results_frame.pack(fill="both", expand=True, padx=5, pady=5)

        self.tree = ttk.Treeview(results_frame, columns=("Path", "Size", "Created", "Action"), show="headings",
                                 height=15)
        self.tree.pack(fill="both", expand=True)

        self.tree.heading("Path", text="File Path", command=lambda: self.sort_column("Path", False, duplicates))
        self.tree.heading("Size", text="Size", command=lambda: self.sort_column("Size", False, duplicates))
        self.tree.heading("Created", text="Created", command=lambda: self.sort_column("Created", False, duplicates))
        self.tree.heading("Action", text="Action")

        self.tree.column("Path", width=300, anchor="w")
        self.tree.column("Size", width=100, anchor="center")
        self.tree.column("Created", width=150, anchor="center")
        self.tree.column("Action", width=100, anchor="center")

        self.populate_treeview(duplicates)

        scrollbar = ttk.Scrollbar(results_frame, orient="vertical", command=self.tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill="y")
        self.tree.configure(yscrollcommand=scrollbar.set)

        # Action Frame
        self.action_frame = tk.Frame(self.result_frame, bg="#f0f4f7")
        self.action_frame.pack(side=tk.BOTTOM, fill="x", pady=5)

        self.view_button = tk.Button(self.action_frame, text="View Selected", command=self.view_selected_files,
                                     width=15, font=("Arial", 10), bg="#2196F3", fg="white",
                                     relief="flat", activebackground="#1976D2")
        print("Created view_button in display_results")
        self.view_button.pack(side=tk.LEFT, padx=5)

        self.delete_button = tk.Button(self.action_frame, text="Delete Selected", command=self.delete_selected_files,
                                       width=15, font=("Arial", 10), bg="#ff4444", fg="white",
                                       relief="flat", activebackground="#cc0000")
        print("Created delete_button in display_results")
        self.delete_button.pack(side=tk.LEFT, padx=5)

        self.metadata_button = tk.Button(self.action_frame, text="Show Metadata", command=self.show_metadata,
                                         width=15, font=("Arial", 10), bg="#8BC34A", fg="white",
                                         relief="flat", activebackground="#7CB342")
        print("Created metadata_button in display_results")
        self.metadata_button.pack(side=tk.LEFT, padx=5)

    def populate_treeview(self, duplicates):
        self.tree.delete(*self.tree.get_children())
        for original, duplicate_group in duplicates.items():
            if len(duplicate_group) > 1:
                orig_size = os.path.getsize(original)
                orig_time = datetime.fromtimestamp(os.path.getctime(original)).strftime('%Y-%m-%d %H:%M:%S')
                self.tree.insert("", "end", values=(original, self.format_size(orig_size), orig_time, "Original"),
                                 tags=("original",))

                for file in duplicate_group:
                    if file != original:
                        file_size = os.path.getsize(file)
                        file_time = datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
                        self.tree.insert("", "end", values=(file, self.format_size(file_size), file_time, "Duplicate"),
                                         tags=("duplicate",))

        self.tree.tag_configure("original", background="#e0f7fa", font=("Arial", 10))
        self.tree.tag_configure("duplicate", background="#ffffff", font=("Arial", 10))

    def sort_column(self, col, reverse, duplicates):
        items = [(self.tree.set(item, col), item) for item in self.tree.get_children('')]
        items.sort(reverse=reverse)

        for index, (val, item) in enumerate(items):
            self.tree.move(item, '', index)
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse, duplicates))

    def sort_results(self, sort_by, duplicates):
        self.tree.delete(*self.tree.get_children())
        all_items = []
        for original, duplicate_group in duplicates.items():
            if len(duplicate_group) > 1:
                orig_size = os.path.getsize(original)
                orig_time = datetime.fromtimestamp(os.path.getctime(original))
                all_items.append((original, orig_size, orig_time, "Original"))
                for file in duplicate_group:
                    if file != original:
                        file_size = os.path.getsize(file)
                        file_time = datetime.fromtimestamp(os.path.getctime(file))
                        all_items.append((file, file_size, file_time, "Duplicate"))

        if sort_by == "Size":
            all_items.sort(key=lambda x: x[1], reverse=True)
        else:
            all_items.sort(key=lambda x: x[2], reverse=True)

        for item in all_items:
            self.tree.insert("", "end", values=(
                item[0], self.format_size(item[1]), item[2].strftime('%Y-%m-%d %H:%M:%S'), item[3]),
                             tags=("original" if item[3] == "Original" else "duplicate"))

    def filter_results(self, filter_by, duplicates):
        self.tree.delete(*self.tree.get_children())
        for original, duplicate_group in duplicates.items():
            if len(duplicate_group) > 1:
                if filter_by in ["All", "Originals"]:
                    orig_size = os.path.getsize(original)
                    orig_time = datetime.fromtimestamp(os.path.getctime(original)).strftime('%Y-%m-%d %H:%M:%S')
                    self.tree.insert("", "end", values=(original, self.format_size(orig_size), orig_time, "Original"),
                                     tags=("original",))
                if filter_by in ["All", "Duplicates"]:
                    for file in duplicate_group:
                        if file != original:
                            file_size = os.path.getsize(file)
                            file_time = datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
                            self.tree.insert("", "end",
                                             values=(file, self.format_size(file_size), file_time, "Duplicate"),
                                             tags=("duplicate",))

    def view_selected_files(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Warning", "No files selected to view.")
            return

        for item in selected_items:
            file_path = self.tree.item(item, "values")[0]
            try:
                subprocess.call(['xdg-open', file_path])
            except Exception as e:
                messagebox.showerror("Error", f"Could not open file: {file_path}\nError: {e}")

    def delete_selected_files(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Warning", "No files selected for deletion.")
            return

        files_to_delete = [(self.tree.item(item, "values")[0], item) for item in selected_items if
                           self.tree.item(item, "values")[3] == "Duplicate"]
        if not files_to_delete:
            messagebox.showwarning("Warning", "No duplicate files selected for deletion.")
            return

        if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete {len(files_to_delete)} file(s)?"):
            for file_path, item in files_to_delete:
                try:
                    os.remove(file_path)
                    self.tree.delete(item)
                except Exception as e:
                    messagebox.showerror("Error", f"Could not delete file: {file_path}\nError: {e}")

    def find_original_file(self, files):
        try:
            return min(files, key=os.path.getctime)
        except:
            return files[0]

    def format_size(self, size):
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0


def main():
    root = tk.Tk()
    # Platform-specific window maximization
    if os.name == 'nt':  # Windows
        root.state('zoomed')
    else:  # Linux/Unix
        root.attributes('-zoomed', True)  # This works on X11-based systems
        # Alternative approach: set geometry to screen size
        # screen_width = root.winfo_screenwidth()
        # screen_height = root.winfo_screenheight()
        # root.geometry(f"{screen_width}x{screen_height}+0+0")

    app = FileToolsApp(root)
    root.protocol("WM_DELETE_WINDOW", lambda: root.quit() if not app.is_scanning else None)
    root.mainloop()


if __name__ == "__main__":
    main()