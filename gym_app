import os
import json
import shutil
import sqlite3
from datetime import date, datetime, timedelta
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# Optional library for Excel export
try:
    import pandas as pd

    PANDAS = True
except Exception:
    PANDAS = False

# Date picker
try:
    from tkcalendar import DateEntry
except Exception:
    DateEntry = None  # fallback to tk.Entry for dates

# ---------------- Config ----------------
APP_TITLE = "Gym & Sauna Manager"
DB_FILE = "gym.db"
BACKUP_DIR = "backups"
SETTINGS_FILE = "settings.json"

PRICES = {
    "Daily": 200.0,
    "Weekly": 1000.0,
    "Monthly": 3000.0,
    "Half-Yearly": 15000.0,
    "Yearly": 30000.0
}

DURATIONS = {
    "Daily": timedelta(days=1),
    "Weekly": timedelta(days=7),
    "Monthly": timedelta(days=30),
    "Half-Yearly": timedelta(days=180),
    "Yearly": timedelta(days=365)
}

PAYMENT_METHODS = ["Cash", "Mobile Transfer"]
SAUNA_ON_RENEW = 4
EXPIRY_SOON_DAYS = 3

# ---------------- Dark Theme Colors (improved contrast) ----------------
BG_COLOR = "#2b2b2b"  # page background
PANEL_BG = "#2f2f2f"  # frames / panels
FG_COLOR = "#ffffff"  # foreground text
ENTRY_BG = "#404040"  # entry background (darker)
ENTRY_FG = "#ffffff"  # entry text color
ENTRY_PLACEHOLDER = "#bdbdbd"
BUTTON_BG = "#3a7bd5"  # blue accent for buttons
BUTTON_FG = "#ffffff"
TREE_HEADER_BG = "#1f1f1f"
TREE_ODD_ROW = "#313131"
TREE_EVEN_ROW = "#3a3a3a"
WARNING_COLOR = "#ff6b6b"  # expired (light red)
SOON_COLOR = "#ffd57a"  # soon (yellow)
OK_COLOR = "#2e7d32"  # ok (green-ish)


# ---------------- Database helpers ----------------
def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone TEXT,
        subscription_type TEXT,
        start_date TEXT,
        end_date TEXT,
        sauna_sessions INTEGER DEFAULT 0,
        balance REAL DEFAULT 0.0,
        daily_days_paid INTEGER DEFAULT 0
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER,
        amount REAL,
        note TEXT,
        payment_date TEXT,
        payment_method TEXT,
        days_paid INTEGER DEFAULT 0,
        FOREIGN KEY(member_id) REFERENCES members(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS attendance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER,
        attendance_date TEXT,
        FOREIGN KEY(member_id) REFERENCES members(id)
    )""")
    conn.commit()
    conn.close()


def daily_backup():
    if not os.path.exists(DB_FILE):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    tag = datetime.now().strftime("%Y%m%d")
    dst = os.path.join(BACKUP_DIR, f"gym_{tag}.db")
    if not os.path.exists(dst):
        shutil.copy2(DB_FILE, dst)


# ---------------- Business logic ----------------
def add_member(name, phone, sub_type, start_date, days_paid=1):
    start = start_date
    if sub_type == "Daily":
        end = start + timedelta(days=days_paid)
    else:
        end = start + DURATIONS.get(sub_type, DURATIONS["Monthly"])
    sauna = SAUNA_ON_RENEW if sub_type in ["Monthly", "Half-Yearly", "Yearly"] else 0
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO members (name, phone, subscription_type, start_date, end_date, sauna_sessions, balance,
                                        daily_days_paid)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (name.strip(), phone.strip(), sub_type, start.isoformat(), end.isoformat(), sauna, 0.0,
                 days_paid if sub_type == "Daily" else 0))
    conn.commit()
    mid = cur.lastrowid
    conn.close()
    return mid


def update_member(mid, name, phone, sub_type, start_date, end_date):
    sauna = SAUNA_ON_RENEW if sub_type in ["Monthly", "Half-Yearly", "Yearly"] else 0
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""UPDATE members
                   SET name=?,
                       phone=?,
                       subscription_type=?,
                       start_date=?,
                       end_date=?,
                       sauna_sessions=?
                   WHERE id = ?""",
                (name.strip(), phone.strip(), sub_type, start_date.isoformat(), end_date.isoformat(), sauna, mid))
    conn.commit()
    conn.close()


def delete_member(mid):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM attendance WHERE member_id=?", (mid,))
    cur.execute("DELETE FROM payments WHERE member_id=?", (mid,))
    cur.execute("DELETE FROM members WHERE id=?", (mid,))
    conn.commit()
    conn.close()


def list_members(name_filter="", phone_filter="", type_filter="All"):
    conn = get_conn()
    cur = conn.cursor()
    sql = "SELECT * FROM members WHERE 1=1"
    params = []
    if name_filter:
        sql += " AND name LIKE ?"
        params.append(f"%{name_filter}%")
    if phone_filter:
        sql += " AND phone LIKE ?"
        params.append(f"%{phone_filter}%")
    if type_filter and type_filter != "All":
        sql += " AND subscription_type = ?"
        params.append(type_filter)
    sql += " ORDER BY id DESC"
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def record_payment(member_id, amount, note="", method="Cash", days_paid=0):
    if amount <= 0:
        raise ValueError("Amount must be positive")
    if method not in PAYMENT_METHODS:
        raise ValueError("Invalid payment method")

    conn = get_conn()
    cur = conn.cursor()
    today = date.today().isoformat()

    # Insert payment record
    cur.execute("""INSERT INTO payments (member_id, amount, note, payment_date, payment_method, days_paid)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (member_id, amount, note, today, method, days_paid))

    # Get member details
    cur.execute("""SELECT subscription_type, balance, end_date, daily_days_paid
                   FROM members
                   WHERE id = ?""", (member_id,))
    row = cur.fetchone()
    if not row:
        conn.commit()
        conn.close()
        raise ValueError("Member not found")

    sub = row["subscription_type"]
    price = PRICES.get(sub, PRICES["Monthly"])
    balance = float(row["balance"] or 0.0) + amount
    daily_days_paid = int(row["daily_days_paid"] or 0)
    renewed = False

    if sub == "Daily":
        # For daily clients, only extend if paying for additional days
        if days_paid > 0:
            try:
                current_end = date.fromisoformat(row["end_date"])
            except Exception:
                current_end = date.today()

            # Only extend from current end date if not expired
            if current_end >= date.today():
                new_end = current_end + timedelta(days=days_paid)
            else:
                new_end = date.today() + timedelta(days=days_paid)

            cur.execute("""UPDATE members
                           SET end_date=?,
                               daily_days_paid=?,
                               balance=?
                           WHERE id = ?""",
                        (new_end.isoformat(), days_paid, balance, member_id))
            renewed = True
        else:
            # If no days specified, just update balance
            cur.execute("UPDATE members SET balance=? WHERE id=?", (balance, member_id))
    else:
        # For other subscription types
        if balance >= price:
            periods = int(balance // price)
            remainder = balance - periods * price
            try:
                current_end = date.fromisoformat(row["end_date"])
            except Exception:
                current_end = date.today()

            # Only extend from current end date if not expired
            if current_end >= date.today():
                start_from = current_end
            else:
                start_from = date.today()

            new_end = start_from + DURATIONS[sub] * periods
            new_sauna = SAUNA_ON_RENEW if sub in ["Monthly", "Half-Yearly", "Yearly"] else 0
            cur.execute("""UPDATE members
                           SET end_date=?,
                               sauna_sessions=?,
                               balance=?
                           WHERE id = ?""",
                        (new_end.isoformat(), new_sauna, remainder, member_id))
            renewed = True
        else:
            cur.execute("UPDATE members SET balance=? WHERE id=?", (balance, member_id))

    conn.commit()
    conn.close()
    return renewed


def list_payments(member_id=None):
    conn = get_conn()
    cur = conn.cursor()
    if member_id:
        cur.execute("""SELECT p.*, m.name AS member_name
                       FROM payments p
                                JOIN members m ON p.member_id = m.id
                       WHERE member_id = ?
                       ORDER BY p.id DESC""", (member_id,))
    else:
        cur.execute("""SELECT p.*, m.name AS member_name
                       FROM payments p
                                JOIN members m ON p.member_id = m.id
                       ORDER BY p.id DESC""")
    rows = cur.fetchall()
    conn.close()
    return rows


def mark_attendance(member_id):
    today = date.today().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) c
                   FROM attendance
                   WHERE member_id = ?
                     AND attendance_date = ?""", (member_id, today))
    if cur.fetchone()["c"] > 0:
        conn.close()
        return False
    cur.execute("""INSERT INTO attendance (member_id, attendance_date)
                   VALUES (?, ?)""", (member_id, today))
    conn.commit()
    conn.close()
    return True


def list_attendance(date_from=None, date_to=None):
    conn = get_conn()
    cur = conn.cursor()
    sql = """SELECT a.*, m.name AS member_name, m.subscription_type
             FROM attendance a \
                      JOIN members m ON a.member_id = m.id \
             WHERE 1 = 1"""
    params = []
    if date_from:
        sql += " AND attendance_date >= ?"
        params.append(date_from)
    if date_to:
        sql += " AND attendance_date <= ?"
        params.append(date_to)
    sql += " ORDER BY a.id DESC"
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def use_sauna(member_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT sauna_sessions FROM members WHERE id=?", (member_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        raise ValueError("Member not found")
    s = int(r["sauna_sessions"] or 0)
    if s <= 0:
        conn.close()
        return False, 0
    s -= 1
    cur.execute("UPDATE members SET sauna_sessions=? WHERE id=?", (s, member_id))
    conn.commit()
    conn.close()
    return True, s


def sum_payments(member_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT SUM(amount) s FROM payments WHERE member_id=?", (member_id,))
    r = cur.fetchone()
    conn.close()
    return float(r["s"] or 0.0)


def get_daily_attendance_count(att_date=None):
    if not att_date:
        att_date = date.today().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) as cnt
                   FROM attendance
                   WHERE attendance_date = ?""", (att_date,))
    count = cur.fetchone()["cnt"]
    conn.close()
    return count


def get_weekly_attendance_count(start_date=None):
    if not start_date:
        start_date = date.today()
    end_date = start_date + timedelta(days=6)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) as cnt
                   FROM attendance
                   WHERE attendance_date BETWEEN ? AND ?""",
                (start_date.isoformat(), end_date.isoformat()))
    count = cur.fetchone()["cnt"]
    conn.close()
    return count


# ---------------- Utilities ----------------
def backup_db_once():
    if not os.path.exists(DB_FILE):
        return None
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f"gym_{ts}.db")
    shutil.copy2(DB_FILE, dst)
    return dst


# ---------------- UI App ----------------
class GymApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1150x720")
        self.minsize(1000, 600)
        self.configure(bg=BG_COLOR)

        # ttk style (for Treeview headers and Combobox)
        self.ttk_style = ttk.Style(self)
        try:
            self.ttk_style.theme_use('clam')
        except Exception:
            pass
        # Treeview style header
        self.ttk_style.configure("Treeview.Heading", background=TREE_HEADER_BG, foreground=FG_COLOR,
                                 font=("Segoe UI", 10, "bold"))
        self.ttk_style.configure("Treeview", rowheight=26, fieldbackground=BG_COLOR, background=BG_COLOR,
                                 foreground=FG_COLOR)

        # Header
        header = tk.Frame(self, bg=BG_COLOR, padx=8, pady=6)
        header.pack(side=tk.TOP, fill=tk.X)
        lbl = tk.Label(header, text="Gym & Sauna Manager", bg=BG_COLOR, fg=FG_COLOR, font=("Segoe UI", 14, "bold"))
        lbl.pack(side=tk.LEFT)
        btn_backup = tk.Button(header, text="Backup DB", bg=BUTTON_BG, fg=BUTTON_FG, activebackground=BUTTON_BG,
                               command=self.handle_backup)
        btn_backup.pack(side=tk.RIGHT, padx=6)
        btn_refresh = tk.Button(header, text="Refresh All", bg=BUTTON_BG, fg=BUTTON_FG, command=self.refresh_all)
        btn_refresh.pack(side=tk.RIGHT, padx=6)

        # Notebook
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Tabs
        self.tab_members = ttk.Frame(self.nb);
        self.nb.add(self.tab_members, text="Members")
        self.tab_payments = ttk.Frame(self.nb);
        self.nb.add(self.tab_payments, text="Payments")
        self.tab_attendance = ttk.Frame(self.nb);
        self.nb.add(self.tab_attendance, text="Attendance")
        self.tab_reports = ttk.Frame(self.nb);
        self.nb.add(self.tab_reports, text="Reports")

        # Build tabs
        self.build_members_tab()
        self.build_payments_tab()
        self.build_attendance_tab()
        self.build_reports_tab()

        # housekeeping
        daily_backup()
        self.load_geometry()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- Members Tab ----------
    def build_members_tab(self):
        top = ttk.Frame(self.tab_members, padding=8);
        top.pack(fill=tk.X)
        form = ttk.LabelFrame(top, text="Add / Edit Member", padding=8);
        form.pack(side=tk.LEFT, padx=6)

        # Use tk.Entry for clear bg control
        ttk.Label(form, text="Name").grid(row=0, column=0, sticky=tk.W)
        self.e_name = tk.Entry(form, width=30, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
        self.e_name.grid(row=0, column=1, padx=6, pady=2)

        ttk.Label(form, text="Phone").grid(row=1, column=0, sticky=tk.W)
        self.e_phone = tk.Entry(form, width=30, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
        self.e_phone.grid(row=1, column=1, padx=6, pady=2)

        ttk.Label(form, text="Subscription").grid(row=2, column=0, sticky=tk.W)
        self.cb_sub = ttk.Combobox(form, values=list(PRICES.keys()), state="readonly", width=28)
        self.cb_sub.set("Monthly");
        self.cb_sub.grid(row=2, column=1, padx=6, pady=2)

        ttk.Label(form, text="Start Date").grid(row=3, column=0, sticky=tk.W)
        if DateEntry:
            frame_date1 = tk.Frame(form, bg=ENTRY_BG)
            frame_date1.grid(row=3, column=1, padx=6, pady=2, sticky="w")
            self.de_start = DateEntry(frame_date1, width=24, date_pattern="yyyy-mm-dd")
            self.de_start.pack(fill="x")
        else:
            self.de_start = tk.Entry(form, width=30, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
            self.de_start.insert(0, date.today().isoformat());
            self.de_start.grid(row=3, column=1, padx=6, pady=2)

        ttk.Label(form, text="End Date").grid(row=4, column=0, sticky=tk.W)
        if DateEntry:
            frame_date2 = tk.Frame(form, bg=ENTRY_BG)
            frame_date2.grid(row=4, column=1, padx=6, pady=2, sticky="w")
            self.de_end = DateEntry(frame_date2, width=24, date_pattern="yyyy-mm-dd")
            self.de_end.pack(fill="x")
        else:
            self.de_end = tk.Entry(form, width=30, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
            self.de_end.insert(0, (date.today() + DURATIONS["Monthly"]).isoformat());
            self.de_end.grid(row=4, column=1, padx=6, pady=2)

        # New field for days paid (only relevant for Daily subscriptions)
        ttk.Label(form, text="Days Paid").grid(row=5, column=0, sticky=tk.W)
        self.e_days_paid = tk.Entry(form, width=30, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
        self.e_days_paid.insert(0, "1");
        self.e_days_paid.grid(row=5, column=1, padx=6, pady=2)

        btn_auto = tk.Button(form, text="Auto-calc End", bg=BUTTON_BG, fg=BUTTON_FG, command=self.auto_calc_end)
        btn_auto.grid(row=6, column=0, pady=6)
        btn_save = tk.Button(form, text="Save Member", bg=BUTTON_BG, fg=BUTTON_FG, command=self.handle_save_member)
        btn_save.grid(row=6, column=1, pady=6)
        btn_clear = tk.Button(form, text="Clear", bg=BUTTON_BG, fg=BUTTON_FG, command=self.clear_member_form)
        btn_clear.grid(row=7, column=0, pady=6)
        btn_delete = tk.Button(form, text="Delete Selected", bg=BUTTON_BG, fg=BUTTON_FG,
                               command=self.delete_selected_member)
        btn_delete.grid(row=7, column=1, pady=6)

        # search/filter area
        filter_frame = ttk.LabelFrame(top, text="Search / Filter", padding=8);
        filter_frame.pack(side=tk.LEFT, padx=6, fill=tk.X, expand=True)
        ttk.Label(filter_frame, text="Name").grid(row=0, column=0)
        self.f_name = tk.Entry(filter_frame, width=18, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG);
        self.f_name.grid(row=0, column=1, padx=6)
        ttk.Label(filter_frame, text="Phone").grid(row=0, column=2)
        self.f_phone = tk.Entry(filter_frame, width=14, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG);
        self.f_phone.grid(row=0, column=3, padx=6)
        ttk.Label(filter_frame, text="Subscription").grid(row=0, column=4)
        vals = ["All"] + list(PRICES.keys());
        self.f_sub = ttk.Combobox(filter_frame, values=vals, state="readonly", width=12);
        self.f_sub.set("All");
        self.f_sub.grid(row=0, column=5, padx=6)
        btn_search = tk.Button(filter_frame, text="Search", bg=BUTTON_BG, fg=BUTTON_FG,
                               command=self.refresh_members_table);
        btn_search.grid(row=0, column=6, padx=6)
        btn_clearf = tk.Button(filter_frame, text="Clear", bg=BUTTON_BG, fg=BUTTON_FG, command=self.clear_filters);
        btn_clearf.grid(row=0, column=7, padx=6)

        # Attendance summary
        today_count = get_daily_attendance_count()
        weekly_count = get_weekly_attendance_count()
        summary_frame = tk.Frame(filter_frame, bg=PANEL_BG)
        summary_frame.grid(row=1, column=0, columnspan=8, pady=6, sticky="ew")
        tk.Label(summary_frame, text=f"Today: {today_count} visitors", bg=PANEL_BG, fg=FG_COLOR).pack(side=tk.LEFT,
                                                                                                      padx=10)
        tk.Label(summary_frame, text=f"This week: {weekly_count} visitors", bg=PANEL_BG, fg=FG_COLOR).pack(side=tk.LEFT,
                                                                                                           padx=10)

        # members table (full-width)
        table_frame = tk.Frame(self.tab_members, bg=BG_COLOR, padx=8, pady=8);
        table_frame.pack(fill=tk.BOTH, expand=True)
        cols = ("id", "name", "phone", "subscription", "start", "end", "days_left", "sauna", "paid", "remaining")
        self.members_tv = ttk.Treeview(table_frame, columns=cols, show="headings", selectmode="browse")
        headings = ["ID", "Name", "Phone", "Subscription", "Start", "End", "Days Left", "Sauna", "Paid(KES)",
                    "Remaining(KES)"]
        widths = [60, 260, 120, 120, 110, 110, 90, 80, 100, 110]
        for c, h, w in zip(cols, headings, widths):
            self.members_tv.heading(c, text=h);
            self.members_tv.column(c, width=w, anchor="center")
        self.members_tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.members_tv.yview);
        sb.pack(side=tk.LEFT, fill=tk.Y);
        self.members_tv.config(yscroll=sb.set)

        # tags for contrast
        self.members_tv.tag_configure("expired", background=WARNING_COLOR, foreground="#000000")
        self.members_tv.tag_configure("soon", background=SOON_COLOR, foreground="#000000")
        self.members_tv.tag_configure("ok", background=OK_COLOR, foreground="#ffffff")
        self.members_tv.bind("<Double-1>", lambda e: self.show_member_details())

        # actions
        act = tk.Frame(self.tab_members, bg=BG_COLOR, padx=8, pady=8);
        act.pack(fill=tk.X)
        tk.Button(act, text="Refresh", bg=BUTTON_BG, fg=BUTTON_FG, command=self.refresh_members_table).pack(
            side=tk.LEFT, padx=6)
        tk.Button(act, text="Show Selected", bg=BUTTON_BG, fg=BUTTON_FG, command=self.show_member_details).pack(
            side=tk.LEFT, padx=6)

        self.refresh_members_table()

    def parse_date_widget(self, widget):
        if DateEntry and isinstance(widget, DateEntry):
            return widget.get_date()
        else:
            txt = widget.get().strip()
            try:
                return date.fromisoformat(txt)
            except Exception:
                return date.today()

    def auto_calc_end(self):
        sub = self.cb_sub.get()
        s = self.parse_date_widget(self.de_start)
        if sub == "Daily":
            try:
                days_paid = int(self.e_days_paid.get())
            except ValueError:
                days_paid = 1
            e = s + timedelta(days=days_paid)
        else:
            e = s + DURATIONS.get(sub, DURATIONS["Monthly"])
        if DateEntry and isinstance(self.de_end, DateEntry):
            self.de_end.set_date(e)
        else:
            self.de_end.delete(0, tk.END);
            self.de_end.insert(0, e.isoformat())

    def clear_member_form(self):
        self.e_name.delete(0, tk.END);
        self.e_phone.delete(0, tk.END);
        self.cb_sub.set("Monthly")
        if DateEntry and isinstance(self.de_start, DateEntry):
            self.de_start.set_date(date.today());
            self.de_end.set_date(date.today() + DURATIONS["Monthly"])
        else:
            self.de_start.delete(0, tk.END);
            self.de_start.insert(0, date.today().isoformat())
            self.de_end.delete(0, tk.END);
            self.de_end.insert(0, (date.today() + DURATIONS["Monthly"]).isoformat())
        self.e_days_paid.delete(0, tk.END);
        self.e_days_paid.insert(0, "1")

    def handle_save_member(self):
        name = self.e_name.get().strip();
        phone = self.e_phone.get().strip();
        sub = self.cb_sub.get()
        s = self.parse_date_widget(self.de_start);
        e = self.parse_date_widget(self.de_end)
        if not name:
            messagebox.showwarning("Validation", "Please enter a name.");
            return

        try:
            days_paid = int(self.e_days_paid.get())
        except ValueError:
            days_paid = 1 if sub == "Daily" else 0

        sel = self.members_tv.selection()
        if sel:
            mid = self.members_tv.item(sel[0])["values"][0]
            update_member(mid, name, phone, sub, s, e)
            messagebox.showinfo("Saved", "Member updated.")
        else:
            add_member(name, phone, sub, s, days_paid)
            messagebox.showinfo("Saved", "Member added.")
        self.refresh_members_table()

    def delete_selected_member(self):
        sel = self.members_tv.selection()
        if not sel:
            messagebox.showwarning("Select", "Select a member to delete.");
            return
        mid = self.members_tv.item(sel[0])["values"][0]
        if not messagebox.askyesno("Confirm", "Delete member and related records?"): return
        delete_member(mid);
        self.refresh_members_table()

    def clear_filters(self):
        self.f_name.delete(0, tk.END);
        self.f_phone.delete(0, tk.END);
        self.f_sub.set("All");
        self.refresh_members_table()

    def refresh_members_table(self):
        for r in self.members_tv.get_children(): self.members_tv.delete(r)
        rows = list_members(self.f_name.get().strip(), self.f_phone.get().strip(), self.f_sub.get())
        today = date.today()
        for m in rows:
            try:
                endd = date.fromisoformat(m["end_date"])
            except Exception:
                endd = today
            days_left = (endd - today).days
            tag = "ok"
            if endd < today:
                tag = "expired"
            elif days_left <= EXPIRY_SOON_DAYS:
                tag = "soon"
            total_paid = sum_payments(m["id"])
            balance = float(m["balance"] or 0.0)
            price = PRICES.get(m["subscription_type"], PRICES["Monthly"])
            remaining = max(0.0, price - balance)
            vals = (m["id"], m["name"], m["phone"], m["subscription_type"], m["start_date"], m["end_date"], days_left,
                    m["sauna_sessions"], f"{total_paid:.2f}", f"{remaining:.2f}")
            self.members_tv.insert("", tk.END, values=vals, tags=(tag,))

    def show_member_details(self):
        sel = self.members_tv.selection()
        if not sel:
            messagebox.showinfo("Select", "Select a member.");
            return
        item = self.members_tv.item(sel[0]);
        mid = item["values"][0]
        conn = get_conn();
        cur = conn.cursor();
        cur.execute("SELECT * FROM members WHERE id=?", (mid,));
        m = cur.fetchone();
        conn.close()
        if not m:
            messagebox.showerror("Error", "Member not found.");
            return
        try:
            endd = date.fromisoformat(m["end_date"])
        except Exception:
            endd = date.today()
        days_left = (endd - date.today()).days;
        total_paid = sum_payments(m["id"])
        info = (f"ID: {m['id']}\nName: {m['name']}\nPhone: {m['phone']}\nSubscription: {m['subscription_type']}\n"
                f"Start: {m['start_date']}\nEnd: {m['end_date']}\nDays left: {days_left}\nSauna sessions: {m['sauna_sessions']}\nTotal paid: KES {total_paid:.2f}\nBalance remainder stored: KES {float(m['balance'] or 0.0):.2f}")
        detail = tk.Toplevel(self);
        detail.title(f"Member {m['id']} - {m['name']}");
        detail.geometry("460x520");
        detail.configure(bg=BG_COLOR)
        tk.Label(detail, text=m['name'], bg=BG_COLOR, fg=FG_COLOR, font=("Segoe UI", 12, "bold")).pack(anchor="w",
                                                                                                       padx=8, pady=6)
        tk.Label(detail, text=info, bg=BG_COLOR, fg=FG_COLOR, justify="left").pack(anchor="w", padx=8)
        if DateEntry:
            try:
                from tkcalendar import Calendar
                cal = Calendar(detail, selectmode='none', date_pattern='yyyy-mm-dd')
                cal.pack(padx=8, pady=8, fill=tk.BOTH, expand=True)
                try:
                    cal.calevent_create(endd, 'Expiry', 'expiry');
                    cal.tag_config('expiry', background='red', foreground='white');
                    cal.see(endd)
                except Exception:
                    pass
            except Exception:
                tk.Label(detail, text="Calendar not available", bg=BG_COLOR, fg=FG_COLOR).pack(padx=8, pady=8)
        tk.Button(detail, text="Close", bg=BUTTON_BG, fg=BUTTON_FG, command=detail.destroy).pack(pady=8)

    # -------- Payments Tab --------
    def build_payments_tab(self):
        top = ttk.Frame(self.tab_payments, padding=8);
        top.pack(fill=tk.X)
        ttk.Label(top, text="Member").grid(row=0, column=0, sticky=tk.W)
        self.pay_member_cb = ttk.Combobox(top, values=self._member_list(), width=60, state="readonly");
        self.pay_member_cb.grid(row=0, column=1, padx=6)
        tk.Button(top, text="Reload Members", bg=BUTTON_BG, fg=BUTTON_FG, command=self.reload_pay_members).grid(row=0,
                                                                                                                column=2,
                                                                                                                padx=6)

        ttk.Label(top, text="Amount (KES)").grid(row=1, column=0, sticky=tk.W);
        self.pay_amount = tk.Entry(top, width=20, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG);
        self.pay_amount.grid(row=1, column=1, sticky=tk.W)

        # New field for days paid (for Daily subscriptions)
        ttk.Label(top, text="Days Paid").grid(row=2, column=0, sticky=tk.W);
        self.pay_days = tk.Entry(top, width=20, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG);
        self.pay_days.grid(row=2, column=1, sticky=tk.W)

        ttk.Label(top, text="Note").grid(row=3, column=0, sticky=tk.W);
        self.pay_note = tk.Entry(top, width=50, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG);
        self.pay_note.grid(row=3, column=1, sticky=tk.W, padx=6)
        ttk.Label(top, text="Method").grid(row=4, column=0, sticky=tk.W);
        self.pay_method = ttk.Combobox(top, values=PAYMENT_METHODS, state="readonly", width=20);
        self.pay_method.set("Cash");
        self.pay_method.grid(row=4, column=1, sticky=tk.W)

        tk.Button(top, text="Record Payment", bg=BUTTON_BG, fg=BUTTON_FG, command=self.on_record_payment).grid(row=5,
                                                                                                               column=0,
                                                                                                               columnspan=3,
                                                                                                               pady=6)

        # payments table
        table_frame = ttk.Frame(self.tab_payments, padding=8);
        table_frame.pack(fill=tk.BOTH, expand=True)
        cols = ("id", "member", "amount", "method", "note", "date", "days_paid")
        self.pay_tv = ttk.Treeview(table_frame, columns=cols, show="headings")
        headings = ["ID", "Member", "Amount", "Method", "Note", "Date", "Days Paid"]
        widths = [60, 200, 100, 100, 200, 100, 80]
        for c, h, w in zip(cols, headings, widths):
            self.pay_tv.heading(c, text=h);
            self.pay_tv.column(c, width=w, anchor="center")
        self.pay_tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.pay_tv.yview);
        sb.pack(side=tk.LEFT, fill=tk.Y)
        self.pay_tv.config(yscroll=sb.set)
        self.reload_pay_members();
        self.refresh_payments()

    def _member_list(self):
        rows = list_members();
        return [f"{r['id']} - {r['name']}" for r in rows]

    def reload_pay_members(self):
        vals = [f"{r['id']} - {r['name']}" for r in list_members()];
        self.pay_member_cb['values'] = vals
        if vals and not self.pay_member_cb.get(): self.pay_member_cb.current(0)

    def on_record_payment(self):
        sel = self.pay_member_cb.get()
        if not sel:
            messagebox.showwarning("Select", "Select a member");
            return
        try:
            mid = int(sel.split(" - ")[0])
        except Exception:
            messagebox.showwarning("Select", "Invalid member selection");
            return
        try:
            amt = float(self.pay_amount.get())
        except Exception:
            messagebox.showwarning("Amount", "Enter a valid amount");
            return

        # Get days paid (if any)
        days_paid = 0
        days_text = self.pay_days.get().strip()
        if days_text:
            try:
                days_paid = int(days_text)
            except ValueError:
                days_paid = 0

        note = self.pay_note.get().strip();
        method = self.pay_method.get()
        try:
            renewed = record_payment(mid, amt, note, method, days_paid)
            if renewed:
                messagebox.showinfo("Payment", "Payment recorded and subscription renewed (if threshold reached).")
            else:
                messagebox.showinfo("Payment", "Payment recorded (partial/accumulated).")
            self.pay_amount.delete(0, tk.END);
            self.pay_note.delete(0, tk.END);
            self.pay_days.delete(0, tk.END)
            self.refresh_payments();
            self.refresh_members_table()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def refresh_payments(self):
        for r in self.pay_tv.get_children(): self.pay_tv.delete(r)
        rows = list_payments()
        for row in rows:
            self.pay_tv.insert("", tk.END, values=(
                row["id"], row["member_name"], f"{row['amount']:.2f}",
                row["payment_method"], row["note"], row["payment_date"],
                row["days_paid"] if row["days_paid"] else ""
            ))

    # -------- Attendance Tab --------
    def build_attendance_tab(self):
        top = ttk.Frame(self.tab_attendance, padding=8);
        top.pack(fill=tk.X)
        ttk.Label(top, text="Member").grid(row=0, column=0, sticky=tk.W)
        self.att_member_cb = ttk.Combobox(top, values=self._member_list(), width=60, state="readonly");
        self.att_member_cb.grid(row=0, column=1, padx=6)
        tk.Button(top, text="Reload Members", bg=BUTTON_BG, fg=BUTTON_FG, command=self.reload_att_members).grid(row=0,
                                                                                                                column=2,
                                                                                                                padx=6)
        tk.Button(top, text="Mark Attendance", bg=BUTTON_BG, fg=BUTTON_FG, command=self.on_mark_attendance).grid(row=0,
                                                                                                                 column=3,
                                                                                                                 padx=6)
        tk.Button(top, text="Use Sauna Session", bg=BUTTON_BG, fg=BUTTON_FG, command=self.on_use_sauna).grid(row=0,
                                                                                                             column=4,
                                                                                                             padx=6)

        # Date range filter for attendance
        filter_frame = ttk.Frame(top, padding=8);
        filter_frame.grid(row=1, column=0, columnspan=5, sticky="ew")
        ttk.Label(filter_frame, text="From:").pack(side=tk.LEFT)
        if DateEntry:
            frame_date1 = tk.Frame(filter_frame, bg=ENTRY_BG)
            frame_date1.pack(side=tk.LEFT, padx=6)
            self.att_date_from = DateEntry(frame_date1, width=12, date_pattern="yyyy-mm-dd")
            self.att_date_from.pack(fill="x")
        else:
            self.att_date_from = tk.Entry(filter_frame, width=12, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
            self.att_date_from.pack(side=tk.LEFT, padx=6)

        ttk.Label(filter_frame, text="To:").pack(side=tk.LEFT)
        if DateEntry:
            frame_date2 = tk.Frame(filter_frame, bg=ENTRY_BG)
            frame_date2.pack(side=tk.LEFT, padx=6)
            self.att_date_to = DateEntry(frame_date2, width=12, date_pattern="yyyy-mm-dd")
            self.att_date_to.pack(fill="x")
        else:
            self.att_date_to = tk.Entry(filter_frame, width=12, bg=ENTRY_BG, fg=ENTRY_FG, insertbackground=ENTRY_FG)
            self.att_date_to.pack(side=tk.LEFT, padx=6)

        tk.Button(filter_frame, text="Filter", bg=BUTTON_BG, fg=BUTTON_FG, command=self.refresh_attendance).pack(
            side=tk.LEFT, padx=6)
        tk.Button(filter_frame, text="Clear", bg=BUTTON_BG, fg=BUTTON_FG, command=self.clear_att_filters).pack(
            side=tk.LEFT, padx=6)

        # Attendance summary
        today_count = get_daily_attendance_count()
        weekly_count = get_weekly_attendance_count()
        summary_frame = tk.Frame(top, bg=PANEL_BG)
        summary_frame.grid(row=2, column=0, columnspan=5, pady=6, sticky="ew")
        tk.Label(summary_frame, text=f"Today: {today_count} visitors", bg=PANEL_BG, fg=FG_COLOR).pack(side=tk.LEFT,
                                                                                                      padx=10)
        tk.Label(summary_frame, text=f"This week: {weekly_count} visitors", bg=PANEL_BG, fg=FG_COLOR).pack(side=tk.LEFT,
                                                                                                           padx=10)

        # attendance table
        table_frame = ttk.Frame(self.tab_attendance, padding=8);
        table_frame.pack(fill=tk.BOTH, expand=True)
        cols = ("id", "member", "date", "subscription")
        self.att_tv = ttk.Treeview(table_frame, columns=cols, show="headings")
        headings = ["ID", "Member", "Date", "Subscription"]
        widths = [60, 260, 120, 120]
        for c, h, w in zip(cols, headings, widths):
            self.att_tv.heading(c, text=h);
            self.att_tv.column(c, width=w, anchor="center")
        self.att_tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.att_tv.yview);
        sb.pack(side=tk.LEFT, fill=tk.Y)
        self.att_tv.config(yscroll=sb.set)
        self.reload_att_members();
        self.refresh_attendance()

    def reload_att_members(self):
        vals = [f"{r['id']} - {r['name']}" for r in list_members()];
        self.att_member_cb['values'] = vals
        if vals and not self.att_member_cb.get(): self.att_member_cb.current(0)

    def on_mark_attendance(self):
        sel = self.att_member_cb.get()
        if not sel:
            messagebox.showwarning("Select", "Select a member");
            return
        try:
            mid = int(sel.split(" - ")[0])
        except Exception:
            messagebox.showwarning("Select", "Invalid member");
            return
        ok = mark_attendance(mid)
        if ok:
            messagebox.showinfo("Attendance", "Marked for today")
            # Update attendance summary
            today_count = get_daily_attendance_count()
            weekly_count = get_weekly_attendance_count()
            for child in self.tab_attendance.winfo_children():
                if isinstance(child, tk.Frame) and hasattr(child, "winfo_children"):
                    for subchild in child.winfo_children():
                        if isinstance(subchild, tk.Frame) and "Today:" in subchild.winfo_children()[0].cget("text"):
                            subchild.winfo_children()[0].config(text=f"Today: {today_count} visitors")
                            subchild.winfo_children()[1].config(text=f"This week: {weekly_count} visitors")
        else:
            messagebox.showinfo("Attendance", "Already marked today")
        self.refresh_attendance()

    def on_use_sauna(self):
        sel = self.att_member_cb.get()
        if not sel:
            messagebox.showwarning("Select", "Select a member");
            return
        try:
            mid = int(sel.split(" - ")[0])
        except Exception:
            messagebox.showwarning("Select", "Invalid member");
            return
        ok, rem = use_sauna(mid)
        if ok:
            messagebox.showinfo("Sauna", f"Session used. Remaining: {rem}")
        else:
            messagebox.showwarning("Sauna", "No sauna sessions left")
        self.refresh_members_table()

    def clear_att_filters(self):
        if DateEntry:
            self.att_date_from.set_date(date.today())
            self.att_date_to.set_date(date.today())
        else:
            self.att_date_from.delete(0, tk.END)
            self.att_date_to.delete(0, tk.END)
        self.refresh_attendance()

    def refresh_attendance(self):
        for r in self.att_tv.get_children(): self.att_tv.delete(r)

        date_from = None
        date_to = None

        if DateEntry:
            date_from = self.att_date_from.get_date().isoformat() if self.att_date_from.get() else None
            date_to = self.att_date_to.get_date().isoformat() if self.att_date_to.get() else None
        else:
            date_from = self.att_date_from.get().strip() or None
            date_to = self.att_date_to.get().strip() or None

        rows = list_attendance(date_from, date_to)
        for a in rows:
            self.att_tv.insert("", tk.END,
                               values=(a["id"], a["member_name"], a["attendance_date"], a["subscription_type"]))

    # -------- Reports Tab --------
    def build_reports_tab(self):
        f = ttk.Frame(self.tab_reports, padding=8);
        f.pack(fill=tk.BOTH, expand=True)
        ttk.Button(f, text="Export All Members", command=lambda: self.export_members(filtered=False)).pack(side=tk.LEFT,
                                                                                                           padx=6)
        ttk.Button(f, text="Export Filtered Members", command=lambda: self.export_members(filtered=True)).pack(
            side=tk.LEFT, padx=6)
        ttk.Button(f, text="Export Payments", command=self.export_payments).pack(side=tk.LEFT, padx=6)
        ttk.Button(f, text="Export Attendance", command=self.export_attendance).pack(side=tk.LEFT, padx=6)
        ttk.Label(f, text="(CSV default; Excel if pandas+openpyxl installed)").pack(anchor=tk.W, pady=8)

    def export_members(self, filtered=False):
        if filtered:
            rows = list_members(self.f_name.get().strip(), self.f_phone.get().strip(), self.f_sub.get())
        else:
            rows = list_members()
        if not rows:
            messagebox.showinfo("No data", "No members to export");
            return
        data = [dict(r) for r in rows];
        self._export_any(data, "members")

    def export_payments(self):
        rows = list_payments()
        if not rows:
            messagebox.showinfo("No data", "No payments to export");
            return
        data = [dict(r) for r in rows];
        self._export_any(data, "payments")

    def export_attendance(self):
        date_from = None
        date_to = None
        if DateEntry:
            date_from = self.att_date_from.get_date().isoformat() if self.att_date_from.get() else None
            date_to = self.att_date_to.get_date().isoformat() if self.att_date_to.get() else None
        else:
            date_from = self.att_date_from.get().strip() or None
            date_to = self.att_date_to.get().strip() or None

        rows = list_attendance(date_from, date_to)
        if not rows:
            messagebox.showinfo("No data", "No attendance to export");
            return
        data = [dict(r) for r in rows];
        self._export_any(data, "attendance")

    def _export_any(self, data, base):
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv"), ("Excel", "*.xlsx")],
                                            initialfile=f"{base}.csv")
        if not path: return
        try:
            if PANDAS and path.lower().endswith(".xlsx"):
                pd.DataFrame(data).to_excel(path, index=False)
            elif PANDAS:
                pd.DataFrame(data).to_csv(path, index=False)
            else:
                headers = list(data[0].keys())
                with open(path, "w", encoding="utf-8") as f:
                    f.write(",".join(headers) + "\n")
                    for d in data:
                        vals = []
                        for h in headers:
                            v = "" if d.get(h) is None else str(d.get(h)).replace('"', '""')
                            vals.append(f'"{v}"')
                        f.write(",".join(vals) + "\n")
            messagebox.showinfo("Exported", f"Exported to:\n{path}")
        except Exception as e:
            messagebox.showerror("Export error", str(e))

    # -------- Helpers & housekeeping --------
    def _member_list(self):
        return [f"{r['id']} - {r['name']}" for r in list_members()]

    def refresh_all(self):
        self.refresh_members_table()
        self.refresh_payments()
        self.refresh_attendance()
        self.reload_member_combo_boxes()

    def refresh_payments(self):
        for r in self.pay_tv.get_children(): self.pay_tv.delete(r)
        rows = list_payments()
        for p in rows:
            self.pay_tv.insert("", tk.END, values=(
                p["id"], p["member_name"], f"{p['amount']:.2f}",
                p["payment_method"], p["note"], p["payment_date"],
                p["days_paid"] if p["days_paid"] else ""
            ))

    def reload_member_combo_boxes(self):
        vals = self._member_list()
        try:
            self.pay_member_cb['values'] = vals
        except Exception:
            pass
        try:
            self.att_member_cb['values'] = vals
        except Exception:
            pass

    def handle_backup(self):
        dst = backup_db_once()
        if dst:
            messagebox.showinfo("Backup", f"Backup saved to:\n{dst}")
        else:
            messagebox.showwarning("Backup", "No DB to backup yet.")

    def on_close(self):
        self.save_geometry();
        self.destroy()

    def save_geometry(self):
        try:
            geo = self.geometry()
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump({"geometry": geo}, f)
        except Exception:
            pass

    def load_geometry(self):
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                    st = json.load(f)
                if st.get("geometry"): self.geometry(st["geometry"])
            except Exception:
                pass


# ---------------- Boot ----------------
def main():
    init_db()
    daily_backup()
    app = GymApp()
    app.mainloop()


if __name__ == "__main__":
    main()
