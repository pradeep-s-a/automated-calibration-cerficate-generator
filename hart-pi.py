#!/usr/bin/env python3
"""
HART Communicator — Python desktop app
Install:  pip install pyserial
Run:      python hart_communicator.py
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, font, filedialog, messagebox
import threading
import struct
import time
import csv
import os
import math
import json
import webbrowser
import tempfile
import urllib.request
import urllib.error
from datetime import datetime
import subprocess

try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False

# ─── HART CONSTANTS ──────────────────────────────────────────────────────────

UNIT_NAMES = {
    0x01:'in H2O', 0x02:'in Hg',  0x03:'ft H2O', 0x04:'mm H2O', 0x05:'mm Hg',
    0x06:'psi',    0x07:'bar',    0x08:'mbar',   0x09:'g/cm2',  0x0A:'kg/cm2',
    0x0B:'Pa',     0x0C:'kPa',    0x0D:'torr',   0x0E:'atm',
    0x11:'l/m',      0x12:'V',      0x13:'mA',     0x14:'Ohm',
    0x20:'degC',   0x21:'degF',   0x22:'degR',   0x23:'K',
    0x26:'l/s',    0x27:'l/min',  0x28:'l/h',
    0x29:'m3/s',   0x2A:'m3/min', 0x2B:'m3/h',
    0x3A:'m',      0x3B:'cm',     0x3C:'mm',     0x3D:'ft',  0x3E:'in',
    0x57:'%',      0x58:'pH',     0x59:'mV',
}

RC_NAMES = {
    # 0x00 = clean
    0x00: 'OK',
    # 0x01-0x7F = command-specific errors
    0x01: 'Undefined command',
    0x02: 'Invalid selection',
    0x03: 'Parameter too large',
    0x04: 'Parameter too small',
    0x05: 'Too few data bytes',
    0x06: 'Device-specific error',
    0x07: 'Write-protect mode',
    0x08: 'Update failure',
    0x0E: 'Span too small',
    0x20: 'Busy',
    0x40: 'Command not implemented',
    # 0x80-0xBF = communications errors (bit7=1, bit6=0) — device UART saw bad framing
    0x80: 'Parity error',
    0x81: 'Framing error',
    0x82: 'Buffer overflow',
    0x84: 'Checksum error',
    0x88: 'Parity error',
    0x90: 'Framing + overflow',
    0x98: 'Framing + parity',
    # 0xC0-0xFF = device status warnings (bit7=1, bit6=1) — data still valid
    0xC0: 'Config changed',
    0xC8: 'More status available (run CMD 48)',
    0xD0: 'Cold start',
    0xE0: 'Device malfunction',
}

COMMANDS = [
    (0,  "Read Unique Identifier"),
    (1,  "Read Primary Variable"),
    (2,  "Read Current + % Range"),
    (3,  "Read Dynamic Variables"),
    (6,  "Write Polling Address"),
    (11, "Read Unique ID by Tag"),
    (12, "Read Message"),
    (13, "Read Tag / Desc / Date"),
    (14, "Read PV Info"),
    (15, "Read Range Values"),
    (16, "Read Final Assembly #"),
    (17, "Write Message"),
    (18, "Write Tag / Desc / Date"),
    (35, "Write Range Values"),
    (36, "Set Upper Range Value"),
    (37, "Set Lower Range Value"),
    (40, "Enter Fixed Current"),
    (41, "Perform Self Test"),
    (42, "Perform D/A Trim"),
    (43, "Set PV Zero"),
    (48, "Read Additional Status"),
    (50, "Read Dynamic Var Assign"),
]

# ─── HART FRAMING ────────────────────────────────────────────────────────────

def xor_cs(data):
    cs = 0
    for b in data:
        cs ^= b
    return cs

def build_frame(cmd, data, preambles, long_addr=None, short_addr=0):
    """Build a HART request frame.

    Short frame: STX=0x02, 1-byte address (polling addr 0-63)
    Long frame:  STX=0x82, 5-byte address (from CMD 0 unique ID)
    """
    if long_addr and len(long_addr) == 5:
        # Long frame — STX 0x82, 5-byte address
        # addr[0] bit7=0 (not burst), bit6=1 (primary master) → OR with 0x80
        addr_bytes = bytes([long_addr[0] | 0x80]) + bytes(long_addr[1:])
        payload = bytes([0x82]) + addr_bytes + bytes([cmd & 0xFF, len(data)]) + data
    else:
        # Short frame — STX 0x02, 1-byte address
        payload = bytes([0x02, short_addr & 0x3F, cmd & 0xFF, len(data)]) + data

    return bytes([0xFF] * preambles) + payload + bytes([xor_cs(payload)])

def parse_frame(raw, debug=False):
    for i in range(len(raw)):
        b = raw[i]
        if b not in (0x06, 0x86):
            continue

        is_long = (b == 0x86)
        frame = raw[i:]

        if is_long:
            if len(frame) < 9: continue   # ← was < 20, need only 8 bytes to read BC
            bc  = frame[7]
            hdr = 8
        else:
            if len(frame) < 6: continue   # ← was < 20, need only 4 bytes to read BC
            bc  = frame[3]
            hdr = 4

        # rest unchanged...

        if bc > 64: continue
        total = hdr + bc + 1
        if len(frame) < total:
            if debug:
                print(f"  @{i} STX=0x{b:02X} {'LONG' if is_long else 'SHORT'} "
                      f"BC={bc} need {total} have {len(frame)} — incomplete")
            continue

        cs_calc = xor_cs(frame[:hdr + bc])
        cs_recv = frame[hdr + bc]
        if cs_calc != cs_recv:
            if debug:
                print(f"  @{i} STX=0x{b:02X} BC={bc} "
                      f"CS calc=0x{cs_calc:02X} recv=0x{cs_recv:02X} — mismatch, skipping")
            continue

        rc       = frame[hdr]
        dev_stat = frame[hdr + 1]
        data     = frame[hdr + 2 : hdr + bc]
        addr     = frame[1:6] if is_long else frame[1:2]

        return {
            'cmd':      frame[6] if is_long else frame[2],
            'rc':       rc,
            'dev_stat': dev_stat,
            'data':     data,
            'addr':     addr,
            'long':     is_long,
            'cs_ok':    True,
            'cs_calc':  cs_calc,
            'cs_recv':  cs_recv,
        }
    return None

def diagnose_frame(raw):
    """Return a human-readable explanation of why parse_frame failed."""
    lines = []
    found_any = False
    for i in range(len(raw)):
        b = raw[i]
        if b not in (0x06, 0x86): continue
        found_any = True
        is_long = (b == 0x86)
        frame = raw[i:]
        hdr = 8 if is_long else 4
        if len(frame) < (10 if is_long else 6):
            lines.append(f"@{i} 0x{b:02X}: too short ({len(frame)} bytes)")
            continue
        bc = frame[7] if is_long else frame[3]
        total = hdr + bc + 1
        if bc > 64:
            lines.append(f"@{i} 0x{b:02X}: BC={bc} too large (>64)")
            continue
        if len(frame) < total:
            lines.append(f"@{i} 0x{b:02X}: BC={bc} need {total} have {len(frame)} bytes (incomplete)")
            continue
        cs_c = xor_cs(frame[:hdr + bc])
        cs_r = frame[hdr + bc]
        lines.append(f"@{i} 0x{b:02X} {'LONG' if is_long else 'SHORT'} BC={bc} "
                     f"CS calc=0x{cs_c:02X} recv=0x{cs_r:02X} "
                     f"{'OK' if cs_c==cs_r else 'MISMATCH'}")
    if not found_any:
        lines.append("No 0x06 or 0x86 start byte found in buffer")
    return '  |  '.join(lines)

def f32(data, offset):
    if len(data) < offset + 4:
        return float('nan')
    return struct.unpack('>f', bytes(data[offset:offset+4]))[0]

def unit_name(code):
    return UNIT_NAMES.get(code, f'u:{code:02X}')

def rc_desc(rc):
    return RC_NAMES.get(rc, f'RC=0x{rc:02X}')

# ─── APPLICATION ─────────────────────────────────────────────────────────────

class HartApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("HART Communicator")
        self.geometry("1100x700")
        self.minsize(900, 580)
        self.configure(bg='#f5f7f4')

        self._ser = None
        self._lock = threading.Lock()
        self._busy = False
        self._current_cmd = None
        self._burst_active = False
        self._long_addr = None   # 5-byte long address populated after CMD 0

        self._sign_server_url = 'http://localhost:5000'  # sign_server.py base URL

        # ── calibration state ─────────────────────────────────────────────────
        self._calib_running  = False
        self._calib_abort    = False
        self._calib_log_data = []   # list of result dicts
        self._calib_hyst_data = []
        self._fluke_ser      = None
        self._lora_tx_busy   = False

        self._setup_fonts()
        self._setup_styles()
        self._build_ui()
        self._refresh_ports()

    def _setup_fonts(self):
        self.f_mono    = font.Font(family='Consolas', size=10)
        self.f_mono_sm = font.Font(family='Consolas', size=9)
        self.f_mono_lg = font.Font(family='Consolas', size=14, weight='bold')
        self.f_label   = font.Font(family='Consolas', size=9)

    def _setup_styles(self):
        s = ttk.Style(self)
        s.theme_use('clam')
        B = '#f5f7f4'; B2 = '#edf2ec'; B3 = '#ffffff'; B4 = '#dfe7dd'
        F = '#1f2a22'; F2 = '#567062'; F3 = '#7a8f82'
        G = '#2f7a4e'; GD = '#dbeee2'
        R = '#c75146'; RD = '#f7dfdb'
        BD = '#c8d4cb'

        s.configure('.',           background=B,  foreground=F,  font=self.f_mono)
        s.configure('TFrame',      background=B)
        s.configure('Side.TFrame', background=B2)
        s.configure('TLabel',      background=B,  foreground=F,  font=self.f_mono)
        s.configure('TEntry',      fieldbackground=B3, foreground=F,
                    insertcolor=F, bordercolor=BD, lightcolor=BD, darkcolor=BD)
        s.configure('TCombobox',   fieldbackground=B3, foreground=F,
                    selectbackground=B4, arrowcolor=F2, bordercolor=BD)
        s.map('TCombobox', fieldbackground=[('readonly', B3)], foreground=[('readonly', F)])
        s.configure('TSpinbox',    fieldbackground=B3, foreground=F,
                    insertcolor=F, bordercolor=BD, arrowcolor=F2)
        s.configure('Green.TButton', background=GD, foreground=G,
                    bordercolor=G, relief='flat', padding=(8,4))
        s.map('Green.TButton', background=[('active','#c9e4d3')])
        s.configure('Red.TButton',   background=RD, foreground=R,
                    bordercolor=R, relief='flat', padding=(8,4))
        s.map('Red.TButton', background=[('active','#f2cbc5')])
        s.configure('Act.TButton',   background=B3, foreground=F2,
                    bordercolor=BD, relief='flat', padding=(6,3))
        s.map('Act.TButton', background=[('active',B4)], foreground=[('active',F)])
        s.configure('Send.TButton',  background=GD, foreground=G,
                    bordercolor=G, relief='flat', padding=(10,5))
        s.map('Send.TButton', background=[('active','#1e4d32')])
        s.configure('TScrollbar',    background=B3, troughcolor=B2,
                    bordercolor=B2, arrowcolor=F2, relief='flat')
        s.configure('TSeparator',    background=BD)

    def _build_ui(self):
        self.columnconfigure(1, weight=1)
        self.rowconfigure(0, weight=1)

        # ── SIDEBAR ──────────────────────────────────────────────────────────
        sb = ttk.Frame(self, style='Side.TFrame', width=272)
        sb.grid(row=0, column=0, sticky='nsew')
        sb.grid_propagate(False)
        sb.columnconfigure(0, weight=1)
        sb.rowconfigure(2, weight=1)

        # Header
        hd = tk.Frame(sb, bg='#edf2ec', padx=14, pady=12)
        hd.grid(row=0, column=0, sticky='ew')
        tk.Label(hd, text="HART COMMUNICATOR",
                 bg='#edf2ec', fg='#2f7a4e',
                 font=font.Font(family='Consolas', size=10, weight='bold')).pack(anchor='w')
        tk.Label(hd, text="Rev 7  Universal Commands",
                 bg='#edf2ec', fg='#7a8f82', font=self.f_mono_sm).pack(anchor='w')

        # Connection panel
        cp = tk.Frame(sb, bg='#edf2ec', padx=12, pady=10)
        cp.grid(row=1, column=0, sticky='ew')
        cp.columnconfigure(1, weight=1)

        def row_label(text, r):
            tk.Label(cp, text=text, bg='#edf2ec', fg='#7a8f82',
                     font=self.f_mono_sm).grid(row=r, column=0, columnspan=2,
                     sticky='w', pady=(5,1))

        row_label("SERIAL PORT", 0)
        pr = tk.Frame(cp, bg='#edf2ec')
        pr.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(0,4))
        pr.columnconfigure(0, weight=1)
        self._port_var = tk.StringVar()
        self._port_cb = ttk.Combobox(pr, textvariable=self._port_var,
                                      state='readonly', font=self.f_mono_sm)
        self._port_cb.grid(row=0, column=0, sticky='ew')
        ttk.Button(pr, text=" ↺ ", style='Act.TButton',
                   command=self._refresh_ports).grid(row=0, column=1, padx=(4,0))

        row_label("BAUD / PARITY", 2)
        bp = tk.Frame(cp, bg='#edf2ec')
        bp.grid(row=3, column=0, columnspan=2, sticky='w', pady=(0,6))
        self._baud_var = tk.StringVar(value='1200')
        ttk.Combobox(bp, textvariable=self._baud_var, state='readonly',
                     values=['1200','2400','9600'],
                     font=self.f_mono_sm, width=7).pack(side='left', padx=(0,6))
        self._parity_var = tk.StringVar(value='O')
        ttk.Combobox(bp, textvariable=self._parity_var, state='readonly',
                     values=['O','M','E','N'],
                     font=self.f_mono_sm, width=3).pack(side='left')
        tk.Label(bp, text=" O=odd  M=mark  E=even  N=none",
                 bg='#edf2ec', fg='#7a8f82', font=self.f_mono_sm).pack(side='left')

        # Params
        pg = tk.Frame(cp, bg='#edf2ec')
        pg.grid(row=4, column=0, columnspan=2, sticky='ew', pady=(0,4))
        self._addr_var    = tk.StringVar(value='0')
        self._pre_var     = tk.StringVar(value='5')
        self._retries_var = tk.StringVar(value='2')
        self._gap_var     = tk.StringVar(value='50')
        self._timeout_var = tk.StringVar(value='3000')

        def spin_col(parent, label, var, lo, hi, col):
            tk.Label(parent, text=label, bg='#edf2ec', fg='#7a8f82',
                     font=self.f_mono_sm).grid(row=0, column=col, sticky='w')
            ttk.Spinbox(parent, textvariable=var, from_=lo, to=hi,
                        width=5, font=self.f_mono_sm).grid(
                row=1, column=col, sticky='w', padx=(0,10))

        for i in range(6): pg.columnconfigure(i, weight=1)
        spin_col(pg, "ADDR",    self._addr_var,    0, 63,   0)
        spin_col(pg, "PREAM",   self._pre_var,     2, 20,   2)
        spin_col(pg, "RETRY",   self._retries_var, 0, 5,    4)

        pg2 = tk.Frame(cp, bg='#edf2ec')
        pg2.grid(row=5, column=0, columnspan=2, sticky='ew', pady=(0,6))
        for i in range(4): pg2.columnconfigure(i, weight=1)
        spin_col(pg2, "GAP ms",    self._gap_var,     0, 2000, 0)
        spin_col(pg2, "TIMEOUT ms",self._timeout_var, 500,10000,2)

        # Checkboxes
        ck = tk.Frame(cp, bg='#edf2ec')
        ck.grid(row=6, column=0, columnspan=2, sticky='ew', pady=(0,6))
        self._rts_var   = tk.BooleanVar(value=True)
        self._raw_var   = tk.BooleanVar(value=False)
        self._long_var  = tk.BooleanVar(value=False)
        for i, (var, lbl) in enumerate([
            (self._rts_var,  'RTS key'),
            (self._long_var, 'Long frame'),
            (self._raw_var,  'Raw log'),
        ]):
            tk.Checkbutton(ck, text=lbl, variable=var,
                           bg='#edf2ec', fg='#567062',
                           selectcolor='#ffffff',
                           activebackground='#edf2ec',
                           font=self.f_mono_sm, bd=0,
                           cursor='hand2').grid(row=0, column=i, sticky='w', padx=(0,8))

        # Long address display (shown after CMD 0 identifies device)
        self._laddr_var = tk.StringVar(value='')
        self._laddr_lbl = tk.Label(ck, textvariable=self._laddr_var,
                                    bg='#edf2ec', fg='#2a9d72',
                                    font=self.f_mono_sm)
        self._laddr_lbl.grid(row=1, column=0, columnspan=4, sticky='w', pady=(3,0))

        # Buttons
        br = tk.Frame(cp, bg='#edf2ec')
        br.grid(row=7, column=0, columnspan=2, sticky='ew')
        br.columnconfigure(0, weight=1); br.columnconfigure(1, weight=1)
        ttk.Button(br, text="Connect",    style='Green.TButton',
                   command=self._connect).grid(row=0, column=0, sticky='ew', padx=(0,3))
        ttk.Button(br, text="Disconnect", style='Red.TButton',
                   command=self._disconnect).grid(row=0, column=1, sticky='ew', padx=(3,0))

        self._status_var = tk.StringVar(value="● not connected")
        self._status_lbl = tk.Label(cp, textvariable=self._status_var,
                                     bg='#edf2ec', fg='#7a8f82', font=self.f_mono_sm)
        self._status_lbl.grid(row=8, column=0, columnspan=2, sticky='w', pady=(6,0))

        # Command list
        cl = tk.Frame(sb, bg='#edf2ec')
        cl.grid(row=2, column=0, sticky='nsew')
        cl.columnconfigure(0, weight=1)
        cl.rowconfigure(1, weight=1)

        tk.Label(cl, text="COMMANDS", bg='#edf2ec', fg='#7a8f82',
                 font=self.f_mono_sm).grid(row=0, column=0, sticky='w',
                 padx=12, pady=(8,2))

        cv = tk.Canvas(cl, bg='#edf2ec', bd=0, highlightthickness=0)
        cs_bar = ttk.Scrollbar(cl, orient='vertical', command=cv.yview)
        cv.configure(yscrollcommand=cs_bar.set)
        cv.grid(row=1, column=0, sticky='nsew')
        cs_bar.grid(row=1, column=1, sticky='ns')

        cf = tk.Frame(cv, bg='#edf2ec')
        cf_id = cv.create_window((0,0), window=cf, anchor='nw')
        cf.bind('<Configure>', lambda e: cv.configure(
            scrollregion=cv.bbox('all')))
        cv.bind('<Configure>', lambda e: cv.itemconfig(cf_id, width=e.width))

        self._cmd_btns = {}
        for cmd_num, cmd_name in COMMANDS:
            b = tk.Button(cf,
                text=f"  {cmd_num:03d}  {cmd_name}",
                bg='#edf2ec', fg='#567062',
                activebackground='#ffffff', activeforeground='#1f2a22',
                font=self.f_mono_sm, bd=0, anchor='w', pady=4,
                cursor='hand2',
                command=lambda n=cmd_num: self._select_cmd(n))
            b.pack(fill='x')
            self._cmd_btns[cmd_num] = b

        tk.Label(cf, text="  ─── Custom ───", bg='#edf2ec',
                 fg='#7a8f82', font=self.f_mono_sm,
                 anchor='w').pack(fill='x', pady=(6,2))
        raw_btn = tk.Button(cf,
            text="  RAW  Custom Command",
            bg='#edf2ec', fg='#b9770e',
            activebackground='#ffffff', activeforeground='#1f2a22',
            font=self.f_mono_sm, bd=0, anchor='w', pady=4,
            cursor='hand2', command=lambda: self._select_cmd(-1))
        raw_btn.pack(fill='x')
        self._cmd_btns[-1] = raw_btn

        # ── MAIN ─────────────────────────────────────────────────────────────
        main = tk.Frame(self, bg='#f5f7f4')
        main.grid(row=0, column=1, sticky='nsew')
        main.columnconfigure(0, weight=1)
        main.rowconfigure(2, weight=1)

        # PV bar
        pv_bar = tk.Frame(main, bg='#edf2ec')
        pv_bar.grid(row=0, column=0, sticky='ew')
        for i in range(6): pv_bar.columnconfigure(i, weight=1)

        self._pv_val  = {}
        self._pv_unit = {}
        for i, (key, lbl) in enumerate([
            ('pv','PV'), ('sv','SV'), ('tv','TV'),
            ('qv','QV'), ('ma','4-20 mA'), ('pct','% Range'),
        ]):
            cell = tk.Frame(pv_bar, bg='#edf2ec',
                            highlightbackground='#dfe7dd', highlightthickness=1)
            cell.grid(row=0, column=i, sticky='nsew', padx=1, pady=1)
            tk.Label(cell, text=lbl, bg='#edf2ec', fg='#7a8f82',
                     font=self.f_mono_sm).pack(anchor='w', padx=8, pady=(5,0))
            vf = tk.Frame(cell, bg='#edf2ec')
            vf.pack(anchor='w', padx=8, pady=(0,5))
            vv = tk.StringVar(value='—')
            vu = tk.StringVar(value='')
            self._pv_val[key]  = vv
            self._pv_unit[key] = vu
            tk.Label(vf, textvariable=vv, bg='#edf2ec', fg='#2f7a4e',
                     font=self.f_mono_lg).pack(side='left')
            tk.Label(vf, textvariable=vu, bg='#edf2ec', fg='#7a8f82',
                     font=self.f_mono_sm).pack(side='left', pady=(4,0))

        # Command header
        chdr = tk.Frame(main, bg='#edf2ec', padx=12, pady=8)
        chdr.grid(row=1, column=0, sticky='ew')
        chdr.columnconfigure(1, weight=1)

        self._cmd_badge = tk.Label(chdr, text="CMD —",
                                    bg='#ffffff', fg='#7a8f82',
                                    font=self.f_mono_sm, padx=8, pady=2)
        self._cmd_badge.grid(row=0, column=0, padx=(0,8))
        self._cmd_title_lbl = tk.Label(chdr,
                                        text="Select a command from the sidebar",
                                        bg='#edf2ec', fg='#1f2a22',
                                        font=self.f_mono)
        self._cmd_title_lbl.grid(row=0, column=1, sticky='w')

        brr = tk.Frame(chdr, bg='#edf2ec')
        brr.grid(row=0, column=2)
        ttk.Button(brr, text="Send →", style='Send.TButton',
                   command=self._send_cmd).grid(row=0, column=0, padx=(0,5))
        self._burst_btn_w = ttk.Button(brr, text="Burst", style='Act.TButton',
                                        command=self._toggle_burst)
        self._burst_btn_w.grid(row=0, column=1, padx=(0,5))
        ttk.Button(brr, text="Clear", style='Act.TButton',
                   command=self._clear_log).grid(row=0, column=2)

        # Custom input row
        self._cust_frame = tk.Frame(main, bg='#ffffff', padx=12, pady=6)
        self._cust_frame.grid(row=1, column=0, sticky='ew')
        self._cust_frame.grid_remove()
        # (overlaps header row — only shown for custom cmd)
        tk.Label(self._cust_frame, text="CMD#", bg='#ffffff',
                 fg='#7a8f82', font=self.f_mono_sm).pack(side='left', padx=(0,4))
        self._cust_cmd_var = tk.StringVar(value='0')
        ttk.Entry(self._cust_frame, textvariable=self._cust_cmd_var,
                  width=5).pack(side='left', padx=(0,12))
        tk.Label(self._cust_frame, text="Data (hex)", bg='#ffffff',
                 fg='#7a8f82', font=self.f_mono_sm).pack(side='left', padx=(0,4))
        self._cust_data_var = tk.StringVar()
        ttk.Entry(self._cust_frame, textvariable=self._cust_data_var,
                  width=38).pack(side='left')

        # ── Notebook: Terminal | Calibration ─────────────────────────────────
        style = ttk.Style()
        style.configure('Dark.TNotebook',         background='#f5f7f4', borderwidth=0)
        style.configure('Dark.TNotebook.Tab',     background='#edf2ec', foreground='#7a8f82',
                        font=self.f_mono_sm, padding=(12, 4))
        style.map('Dark.TNotebook.Tab',
                  background=[('selected','#ffffff')],
                  foreground=[('selected','#2f7a4e')])

        self._nb = ttk.Notebook(main, style='Dark.TNotebook')
        self._nb.grid(row=2, column=0, sticky='nsew')

        # ── Tab 1: Terminal ───────────────────────────────────────────────────
        term_frame = tk.Frame(self._nb, bg='#ffffff')
        term_frame.columnconfigure(0, weight=1)
        term_frame.rowconfigure(0, weight=1)
        self._nb.add(term_frame, text='  TERMINAL  ')

        self._terminal = scrolledtext.ScrolledText(
            term_frame, bg='#ffffff', fg='#1f2a22',
            font=self.f_mono_sm,
            insertbackground='#2f7a4e',
            selectbackground='#dbeee2',
            relief='flat', bd=0, wrap='word', state='disabled')
        self._terminal.grid(row=0, column=0, sticky='nsew')

        self._terminal.tag_config('ts',  foreground='#9aa9a0')
        self._terminal.tag_config('TX',  foreground='#2f7a4e')
        self._terminal.tag_config('RX',  foreground='#5b9bd5')
        self._terminal.tag_config('OK',  foreground='#2a9d72')
        self._terminal.tag_config('ERR', foreground='#c75146')
        self._terminal.tag_config('INF', foreground='#7a8f82')

        # ── Tab 2: Calibration ────────────────────────────────────────────────
        calib_frame = tk.Frame(self._nb, bg='#f5f7f4')
        calib_frame.columnconfigure(0, weight=1)
        calib_frame.rowconfigure(2, weight=1)
        self._nb.add(calib_frame, text='  CALIBRATION  ')
        self._build_calib_tab(calib_frame)

        self._log("INF", "HART Communicator — pyserial edition")
        self._log("INF", "Install:  pip install pyserial")
        self._log("INF", "Select a port, click Connect.")

    # ── CALIBRATION TAB ───────────────────────────────────────────────────────

    def _build_calib_tab(self, parent):
        B  = '#f5f7f4'; B2 = '#edf2ec'; B3 = '#ffffff'; B4 = '#dfe7dd'
        F  = '#1f2a22'; F2 = '#567062'; F3 = '#7a8f82'
        G  = '#2f7a4e'; GD = '#dbeee2'
        R  = '#c75146'; RD = '#f7dfdb'

        # ── Row 0: config strip ───────────────────────────────────────────────
        cfg = tk.Frame(parent, bg=B2, padx=12, pady=8)
        cfg.grid(row=0, column=0, sticky='ew')

        def clabel(text, col, row=0):
            tk.Label(cfg, text=text, bg=B2, fg=F3,
                     font=self.f_mono_sm).grid(row=row, column=col, sticky='w', padx=(0,3))

        def centry(var, width, col, row=0):
            ttk.Entry(cfg, textvariable=var, width=width,
                      font=self.f_mono_sm).grid(row=row, column=col, sticky='w', padx=(0,12))

        # Fluke port
        clabel("FLUKE PORT", 0); self._calib_port_var = tk.StringVar(value='COM1')
        fp_frame = tk.Frame(cfg, bg=B2); fp_frame.grid(row=0, column=1, sticky='w', padx=(0,8))
        self._calib_port_cb = ttk.Combobox(fp_frame, textvariable=self._calib_port_var,
                                            state='readonly', font=self.f_mono_sm, width=8)
        self._calib_port_cb.pack(side='left')
        ttk.Button(fp_frame, text="↺", style='Act.TButton',
                   command=self._calib_refresh_ports).pack(side='left', padx=(3,0))

        clabel("SETTLE s", 2); self._calib_settle_var = tk.StringVar(value='30')
        centry(self._calib_settle_var, 5, 3)

        clabel("SAMPLES", 4); self._calib_samples_var = tk.StringVar(value='3')
        centry(self._calib_samples_var, 4, 5)

        clabel("LRV °C", 6); self._calib_lrv_var = tk.StringVar(value='0')
        centry(self._calib_lrv_var, 6, 7)

        clabel("URV °C", 8); self._calib_urv_var = tk.StringVar(value='175')
        centry(self._calib_urv_var, 9, 9)

        clabel("TOL mA", 10); self._calib_tol_var = tk.StringVar(value='0.080')
        centry(self._calib_tol_var, 6, 11)

        # ── Row 1 of config: certificate template path ────────────────────────
        tk.Label(cfg, text="CERT TEMPLATE", bg=B2, fg=F3,
                 font=self.f_mono_sm).grid(row=1, column=0, sticky='w', pady=(6,0), padx=(0,3))
        self._calib_cert_html_var = tk.StringVar(
            value=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'calibration_certificate_generator_hart.html'))
        cert_row = tk.Frame(cfg, bg=B2)
        cert_row.grid(row=1, column=1, columnspan=10, sticky='ew', pady=(6,0))
        cert_row.columnconfigure(0, weight=1)
        ttk.Entry(cert_row, textvariable=self._calib_cert_html_var,
                  font=self.f_mono_sm).grid(row=0, column=0, sticky='ew', padx=(0,4))
        ttk.Button(cert_row, text="Browse…", style='Act.TButton',
                   command=self._calib_browse_html).grid(row=0, column=1)

        self._calib_refresh_ports()

        # ── Row 1: control bar + progress ────────────────────────────────────
        ctrl = tk.Frame(parent, bg=B3, padx=12, pady=6)
        ctrl.grid(row=1, column=0, sticky='ew')
        ctrl.columnconfigure(8, weight=1)

        self._calib_start_btn = ttk.Button(ctrl, text="▶  Run Calibration",
                                            style='Green.TButton',
                                            command=self._calib_start)
        self._calib_start_btn.grid(row=0, column=0, padx=(0,5))

        self._calib_stop_btn = ttk.Button(ctrl, text="■  Stop",
                                           style='Red.TButton',
                                           command=self._calib_stop,
                                           state='disabled')
        self._calib_stop_btn.grid(row=0, column=1, padx=(0,12))

        ttk.Button(ctrl, text="↓  Export CSV", style='Act.TButton',
                   command=self._calib_export).grid(row=0, column=2, padx=(0,5))

        ttk.Button(ctrl, text="⬛  Plot", style='Act.TButton',
                   command=self._calib_plot).grid(row=0, column=3, padx=(0,12))

        ttk.Button(ctrl, text="📄  Generate Certificate", style='Act.TButton',
                   command=self._calib_generate_cert).grid(row=0, column=4, padx=(0,12))

        ttk.Button(ctrl, text="Send via LoRa", style='Act.TButton',
                   command=self._start_lora_transmit).grid(row=0, column=5, padx=(0,12))

        ttk.Button(ctrl, text="Clear", style='Act.TButton',
                   command=self._calib_clear).grid(row=0, column=6, padx=(0,12))

        ttk.Button(ctrl, text="✏  Custom Certificate", style='Act.TButton',
                   command=self._calib_custom_cert).grid(row=0, column=7, padx=(0,12))

        # progress
        prog_f = tk.Frame(ctrl, bg=B3)
        prog_f.grid(row=0, column=8, sticky='ew')
        prog_f.columnconfigure(0, weight=1)
        self._calib_prog_var = tk.DoubleVar(value=0)
        self._calib_prog_bar = ttk.Progressbar(prog_f, variable=self._calib_prog_var,
                                                maximum=100, length=200)
        self._calib_prog_bar.grid(row=0, column=0, sticky='ew', padx=(0,8))
        self._calib_status_var = tk.StringVar(value='Idle')
        tk.Label(prog_f, textvariable=self._calib_status_var,
                 bg=B3, fg=F2, font=self.f_mono_sm, width=34,
                 anchor='w').grid(row=0, column=1, sticky='w')

        # ── Row 2: paned — results table top, log bottom ──────────────────────
        paned = tk.PanedWindow(parent, orient='vertical',
                               bg=B, sashwidth=4, sashrelief='flat',
                               sashpad=2)
        paned.grid(row=2, column=0, sticky='nsew')

        # Results table
        tbl_frame = tk.Frame(paned, bg=B)
        tbl_frame.columnconfigure(0, weight=1)
        tbl_frame.rowconfigure(1, weight=1)

        tk.Label(tbl_frame, text="CALIBRATION RESULTS",
                 bg=B, fg=F3, font=self.f_mono_sm,
                 anchor='w').grid(row=0, column=0, sticky='w', padx=8, pady=(6,2))

        cols = ("Cycle","Set °C","Theor. mA","Meas. mA","mA Error","% Span Err","Meas. PV °C","Temp Err °C")
        tv_style = ttk.Style()
        tv_style.configure('Calib.Treeview',
                           background=B3, fieldbackground=B3, foreground=F,
                           rowheight=22, font=self.f_mono_sm, borderwidth=0)
        tv_style.configure('Calib.Treeview.Heading',
                           background=B4, foreground=F2, font=self.f_mono_sm,
                           relief='flat')
        tv_style.map('Calib.Treeview', background=[('selected', GD)], foreground=[('selected', G)])

        tv_scroll = ttk.Scrollbar(tbl_frame, orient='vertical')
        self._calib_tv = ttk.Treeview(tbl_frame, columns=cols, show='headings',
                                       style='Calib.Treeview',
                                       yscrollcommand=tv_scroll.set)
        tv_scroll.config(command=self._calib_tv.yview)
        tv_scroll.grid(row=1, column=1, sticky='ns')
        self._calib_tv.grid(row=1, column=0, sticky='nsew')

        col_widths = [90, 70, 90, 90, 80, 90, 100, 90]
        for c, w in zip(cols, col_widths):
            self._calib_tv.heading(c, text=c)
            self._calib_tv.column(c, width=w, anchor='center', stretch=True)

        # Tag colours for asc / desc rows
        self._calib_tv.tag_configure('asc',  background=B3, foreground='#2c6fb7')
        self._calib_tv.tag_configure('desc', background=B3, foreground='#b9770e')

        # Hysteresis strip
        hyst_f = tk.Frame(tbl_frame, bg=B2, pady=4)
        hyst_f.grid(row=2, column=0, columnspan=2, sticky='ew', padx=4, pady=(4,0))
        hyst_f.columnconfigure(0, weight=1)
        tk.Label(hyst_f, text="HYSTERESIS", bg=B2, fg=F3,
                 font=self.f_mono_sm, anchor='w').pack(side='left', padx=8)
        self._calib_hyst_var = tk.StringVar(value='—')
        tk.Label(hyst_f, textvariable=self._calib_hyst_var,
                 bg=B2, fg='#2a9d72', font=self.f_mono_sm,
                 anchor='w').pack(side='left', padx=4)

        paned.add(tbl_frame, minsize=180)

        # Calibration log
        log_frame = tk.Frame(paned, bg=B)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self._calib_terminal = scrolledtext.ScrolledText(
            log_frame, bg='#ffffff', fg='#1f2a22',
            font=self.f_mono_sm, height=8,
            insertbackground=G, selectbackground=GD,
            relief='flat', bd=0, wrap='word', state='disabled')
        self._calib_terminal.grid(row=0, column=0, sticky='nsew')
        self._calib_terminal.tag_config('ts',   foreground='#9aa9a0')
        self._calib_terminal.tag_config('OK',   foreground='#2a9d72')
        self._calib_terminal.tag_config('ERR',  foreground='#c75146')
        self._calib_terminal.tag_config('INF',  foreground='#7a8f82')
        self._calib_terminal.tag_config('STEP', foreground='#b9770e')

        paned.add(log_frame, minsize=80)

    def _calib_refresh_ports(self):
        try:
            import serial.tools.list_ports
            ports = [p.device for p in serial.tools.list_ports.comports()]
        except ImportError:
            ports = []
        self._calib_port_cb['values'] = ports or ['COM1']
        if ports and not self._calib_port_var.get():
            self._calib_port_var.set(ports[0])

    # ── calibration log ───────────────────────────────────────────────────────

    def _clog(self, tag, msg):
        def _w():
            ts = datetime.now().strftime('%H:%M:%S')
            self._calib_terminal.configure(state='normal')
            self._calib_terminal.insert('end', f"[{ts}] ", 'ts')
            self._calib_terminal.insert('end', f"{tag:<4} ", tag)
            self._calib_terminal.insert('end', msg + '\n')
            self._calib_terminal.configure(state='disabled')
            self._calib_terminal.see('end')
        self.after(0, _w)

    def _calib_set_status(self, text):
        self.after(0, lambda: self._calib_status_var.set(text))

    def _calib_set_progress(self, pct):
        self.after(0, lambda: self._calib_prog_var.set(pct))

    # ── treeview helpers ──────────────────────────────────────────────────────

    def _calib_tv_append(self, row_dict, tag='asc'):
        """Append one row to the results treeview (thread-safe)."""
        cols = ("Cycle","Set °C","Theor. mA","Meas. mA","mA Error","% Span Err","Meas. PV °C","Temp Err °C")
        values = (
            row_dict.get('cycle', ''),
            f"{row_dict['set_temp']:.2f}",
            f"{row_dict['theor_ma']:.4f}",
            f"{row_dict['meas_ma']:.4f}",
            f"{row_dict['ma_err']:+.4f}",
            f"{row_dict['pct_span_err']:+.4f}",
            f"{row_dict['meas_pv']:.3f}",
            f"{row_dict['temp_err']:+.4f}",
        )
        def _w():
            self._calib_tv.insert('', 'end', values=values, tags=(tag,))
            self._calib_tv.yview_moveto(1)
        self.after(0, _w)

    def _calib_tv_separator(self):
        def _w():
            self._calib_tv.insert('', 'end',
                values=('─'*8,'─'*6,'─'*9,'─'*9,'─'*8,'─'*9,'─'*10,'─'*9))
        self.after(0, _w)

    def _calib_update_hyst(self):
        if not self._calib_hyst_data:
            return
        parts = []
        for h in self._calib_hyst_data:
            parts.append(f"{h['set_temp']:.2f}°C → {h['hyst_ma']:+.4f}mA ({h['hyst_pct']:+.3f}%span)")
        self.after(0, lambda: self._calib_hyst_var.set('   |   '.join(parts)))

    # ── HART send (calibration-private, bypasses busy flag) ──────────────────

    def _lora_percent_of_span(self, value, lrv, urv):
        span = float(urv) - float(lrv)
        if abs(span) < 1e-9:
            return 0.0
        return ((float(value) - float(lrv)) / span) * 100.0

    def _lora_setpoint_code(self, setpoint_pct):
        supported = {0: 0, 25: 1, 50: 2, 75: 3, 100: 4}
        pct = int(round(float(setpoint_pct)))
        if pct not in supported:
            raise ValueError("HEX LoRa format supports only 0/25/50/75/100 percent setpoints.")
        return supported[pct]

    def _pack_lora_hex_packet(self, cycle, setpoint_pct, measured_ma, measured_temp):
        cycle_code = 0 if str(cycle).lower().startswith("asc") else 1
        header = ((cycle_code & 0x0F) << 4) | (self._lora_setpoint_code(setpoint_pct) & 0x0F)
        payload = bytes([header]) + struct.pack(">f", float(measured_temp)) + struct.pack(">f", float(measured_ma))
        return payload.hex()

    def _build_lora_hex_packets(self):
        if not self._calib_log_data:
            return None, None

        try:
            lrv = float(self._calib_lrv_var.get())
            urv = float(self._calib_urv_var.get())
        except ValueError as exc:
            raise ValueError(f"Invalid calibration values for LoRa transmit: {exc}") from exc

        session_id = datetime.now().strftime('%Y%m%d%H%M%S')
        packets = []
        for row in self._calib_log_data:
            packets.append(
                self._pack_lora_hex_packet(
                    row.get('cycle', 'Ascending'),
                    self._lora_percent_of_span(row.get('set_temp'), lrv, urv),
                    row.get('meas_ma'),
                    row.get('meas_pv'),
                )
            )

        packets.extend(["ff0000000000000000"] * 3)
        return session_id, packets

    def _start_lora_transmit(self):
        if self._lora_tx_busy:
            self._clog("INF", "LoRa transmit already in progress.")
            return
        if not self._calib_log_data:
            messagebox.showinfo("No Data", "Run a calibration first, then click Send via LoRa.")
            return
        threading.Thread(target=self._lora_transmit_latest_calibration, daemon=True).start()

    def _lora_transmit_latest_calibration(self):
        if self._lora_tx_busy:
            return

        self._lora_tx_busy = True
        try:
            freq_mhz = 433.0
            gap_s = 1.0
            spi_hz = 500000
            tx_power_dbm = 2
            rst_pin = 22
            dio0_pin = 4
            timeout_s = 5.0
            max_attempts = 3

            session_id, packets = self._build_lora_hex_packets()
            if not packets:
                self._clog("ERR", "No calibration data available for LoRa transmission.")
                return

            self._clog(
                "STEP",
                f"LoRa TX start  session={session_id}  packets={len(packets)}  "
                f"freq={freq_mhz:.3f}MHz  spi={spi_hz}Hz  pwr={tx_power_dbm}dBm"
            )

            for index, packet in enumerate(packets, start=1):
                attempt = 1
                while attempt <= max_attempts:
                    result = subprocess.run(
                        [
                            "python",
                            "lorapi.py",
                            "tx",
                            "--hex",
                            packet,
                            "--freq",
                            f"{freq_mhz:.3f}",
                            "--rst-pin",
                            str(rst_pin),
                            "--dio0-pin",
                            str(dio0_pin),
                            "--spi-hz",
                            str(spi_hz),
                            "--tx-power",
                            str(tx_power_dbm),
                            "--timeout",
                            str(timeout_s),
                        ],
                        capture_output=True,
                        text=True,
                        cwd=os.path.dirname(__file__) or ".",
                    )
                    if result.stdout:
                        self._clog("INF", result.stdout.strip())
                    if result.returncode == 0:
                        self._clog("INF", f"LoRa packet {index}/{len(packets)} sent ({packet})")
                        break

                    details = (result.stderr or result.stdout or "").strip()
                    self._clog(
                        "ERR",
                        f"LoRa packet {index}/{len(packets)} retry {attempt} failed: "
                        f"{details or f'lorapi.py tx failed with code {result.returncode}'}"
                    )
                    if attempt >= max_attempts:
                        raise RuntimeError(
                            f"LoRa packet {index}/{len(packets)} failed after {max_attempts} attempts. "
                            f"{details or f'lorapi.py tx failed with code {result.returncode}'}"
                        )
                    attempt += 1
                    time.sleep(gap_s)
                if index < len(packets):
                    time.sleep(gap_s)

            self._clog("OK", f"LoRa calibration session {session_id} sent successfully.")
        except Exception as exc:
            self._clog("ERR", f"LoRa transmit failed: {exc}")
        finally:
            self._lora_tx_busy = False

    def _calib_hart_send(self, cmd, data=b'', retries=2, gap=0.05):
        """
        Send a HART command using the existing serial connection.
        Returns parsed reply dict or None.
        Does NOT touch _busy so it can run alongside the calibration thread.
        """
        if not self._ser or not self._ser.is_open:
            return None
        pream      = int(self._pre_var.get() or 5)
        timeout    = max(0.5, float(self._timeout_var.get() or 3000) / 1000)
        use_long   = self._long_addr is not None
        long_addr  = self._long_addr if use_long else None
        short_addr = int(self._addr_var.get() or 0)

        frame = build_frame(cmd, data, pream,
                            long_addr=long_addr, short_addr=short_addr)
        time.sleep(gap)

        for attempt in range(retries + 1):
            raw = self._transact(frame, timeout)
            if raw is None:
                time.sleep(gap); continue
            parsed = parse_frame(raw)
            if parsed is None:
                time.sleep(gap); continue
            return parsed
        return None

    def _calib_read_hart(self, samples=3):
        """Average CMD-2 (mA/%) and CMD-1 (PV) over `samples` reads."""
        ma_vals, pct_vals, pv_vals = [], [], []
        for _ in range(samples):
            p2 = self._calib_hart_send(2)
            if p2 and len(p2['data']) >= 8:
                d = p2['data']
                ma_vals.append(f32(d, 0))
                pct_vals.append(f32(d, 4))
            p1 = self._calib_hart_send(1)
            if p1 and len(p1['data']) >= 5:
                d = p1['data']
                pv_vals.append(f32(d, 1))
            time.sleep(0.4)

        def avg(v): return sum(v)/len(v) if v else float('nan')
        return avg(ma_vals), avg(pct_vals), avg(pv_vals)

    # ── Fluke helpers ─────────────────────────────────────────────────────────

    def _fluke_send(self, msg, delay=2.0):
        if self._fluke_ser and self._fluke_ser.is_open:
            self._fluke_ser.write(msg.encode())
            self._clog("INF", f"Fluke → {msg.strip()}")
            time.sleep(delay)

    def _fluke_open(self, port):
        import serial as _serial
        self._fluke_ser = _serial.Serial(
            port=port, baudrate=9600,
            bytesize=_serial.EIGHTBITS,
            parity=_serial.PARITY_NONE,
            stopbits=_serial.STOPBITS_ONE,
            timeout=1,
        )
        time.sleep(2)
        self._clog("INF", f"Fluke opened on {port}")

    def _fluke_close(self):
        if self._fluke_ser:
            try:
                self._fluke_send("local\r\n", 1)
                self._fluke_ser.close()
            except Exception:
                pass
            self._fluke_ser = None
            self._clog("INF", "Fluke closed.")

    # ── calibration start / stop ──────────────────────────────────────────────

    def _calib_start(self):
        if self._calib_running:
            return
        if not self._ser or not self._ser.is_open:
            messagebox.showerror("Not Connected",
                "Connect to the HART device first (sidebar → Connect).")
            return
        self._calib_running = True
        self._calib_abort   = False
        self._calib_start_btn.configure(state='disabled')
        self._calib_stop_btn.configure(state='normal')
        threading.Thread(target=self._calib_run, daemon=True).start()

    def _calib_stop(self):
        self._calib_abort = True
        self._calib_set_status('Stopping …')
        self._clog("INF", "Stop requested — will abort after current step.")

    def _calib_clear(self):
        self._calib_log_data  = []
        self._calib_hyst_data = []
        self._calib_tv.delete(*self._calib_tv.get_children())
        self._calib_hyst_var.set('—')
        self._calib_set_progress(0)
        self._calib_set_status('Idle')
        self._calib_terminal.configure(state='normal')
        self._calib_terminal.delete('1.0', 'end')
        self._calib_terminal.configure(state='disabled')

    # ── main calibration thread ───────────────────────────────────────────────

    def _calib_run(self):
        try:
            lrv     = float(self._calib_lrv_var.get())
            urv     = float(self._calib_urv_var.get())
            settle  = max(1, int(self._calib_settle_var.get()))
            samples = max(1, int(self._calib_samples_var.get()))
            span_t  = urv - lrv
            span_ma = 16.0

            points = [
                lrv,
                lrv + 0.25 * span_t,
                lrv + 0.50 * span_t,
                lrv + 0.75 * span_t,
                urv,
            ]
            cycles = [('Ascending', points), ('Descending', list(reversed(points)))]
            total_steps = len(points) * 2
            step = 0

            self._clog("STEP", f"=== CALIBRATION START  LRV={lrv}°C  URV={urv}°C ===")
            self._clog("INF",  f"Points: {[f'{p:.2f}' for p in points]}")
            self._clog("INF",  f"Settle: {settle}s   Samples/point: {samples}")

            # ── Open Fluke ────────────────────────────────────────────────────
            fluke_port = self._calib_port_var.get()
            try:
                self._fluke_open(fluke_port)
            except Exception as e:
                self._clog("ERR", f"Cannot open Fluke port {fluke_port}: {e}")
                return

            # ── Identify HART device if not yet done ──────────────────────────
            if self._long_addr is None:
                self._clog("INF", "Running CMD 0 to identify HART device …")
                self.after(0, lambda: self._bg(self._run_cmd, 0))
                time.sleep(3)

            # ── Init Fluke ────────────────────────────────────────────────────
            self._calib_set_status('Initialising Fluke …')
            self._fluke_send("remote\r\n",       2)
            self._fluke_send("*rst\r\n",          3)
            self._fluke_send("tsens_type rtd\r\n",2)
            self._fluke_send("rtd_type pt385\r\n",2)
            self._fluke_send("out 0 cel\r\n",    10)

            # ── Cycle loop ────────────────────────────────────────────────────
            for cycle_name, cycle_pts in cycles:
                if self._calib_abort: break
                self._clog("STEP", f"── {cycle_name} ──")
                self._calib_tv_separator()

                for temp in cycle_pts:
                    if self._calib_abort: break
                    step += 1
                    pct_done = (step / total_steps) * 100
                    self._calib_set_progress(pct_done)
                    self._calib_set_status(f"{cycle_name}  {temp:.2f} °C  (settling {settle}s)")

                    theor_ma  = 4.0 + ((temp - lrv) / span_t) * span_ma
                    theor_pct = ((temp - lrv) / span_t) * 100.0

                    self._clog("STEP", f"Setpoint {temp:.2f} °C  →  {theor_ma:.4f} mA  ({theor_pct:.2f} %)")

                    # Apply setpoint
                    self._fluke_send(f"out {temp} cel\r\n", 2)
                    self._fluke_send("oper\r\n", 2)

                    # Settling countdown
                    for s in range(settle, 0, -5):
                        if self._calib_abort: break
                        self._calib_set_status(f"{cycle_name}  {temp:.2f} °C  — settle {s}s …")
                        time.sleep(min(5, s))

                    if self._calib_abort: break

                    # Read HART
                    self._calib_set_status(f"{cycle_name}  {temp:.2f} °C  — reading HART …")
                    meas_ma, meas_pct, meas_pv = self._calib_read_hart(samples)

                    # Errors
                    ma_err       = meas_ma  - theor_ma
                    pct_span_err = (ma_err / span_ma) * 100.0
                    temp_from_ma = lrv + ((meas_ma - 4.0) / span_ma) * span_t
                    temp_err     = temp_from_ma - temp

                    row = dict(
                        cycle        = cycle_name,
                        set_temp     = temp,
                        theor_ma     = theor_ma,
                        theor_pct    = theor_pct,
                        meas_ma      = meas_ma,
                        meas_pct     = meas_pct,
                        meas_pv      = meas_pv,
                        ma_err       = ma_err,
                        pct_span_err = pct_span_err,
                        temp_err     = temp_err,
                        timestamp    = datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    )
                    self._calib_log_data.append(row)

                    tag = 'asc' if cycle_name == 'Ascending' else 'desc'
                    self._calib_tv_append(row, tag=tag)

                    self._clog("OK",
                        f"mA: {meas_ma:.4f}  err: {ma_err:+.4f}  "
                        f"({pct_span_err:+.3f}%span)  PV: {meas_pv:.3f}°C  "
                        f"Δ: {temp_err:+.4f}°C")

            # ── Hysteresis ────────────────────────────────────────────────────
            if not self._calib_abort:
                self._calib_hyst_data = []
                asc_rows  = [r for r in self._calib_log_data if r['cycle'] == 'Ascending']
                desc_rows = [r for r in self._calib_log_data if r['cycle'] == 'Descending']
                asc_map   = {r['set_temp']: r for r in asc_rows}
                desc_map  = {r['set_temp']: r for r in desc_rows}
                for pt in points:
                    if pt in asc_map and pt in desc_map:
                        h_ma  = asc_map[pt]['ma_err']  - desc_map[pt]['ma_err']
                        h_pct = asc_map[pt]['pct_span_err'] - desc_map[pt]['pct_span_err']
                        h_t   = asc_map[pt]['temp_err']  - desc_map[pt]['temp_err']
                        self._calib_hyst_data.append(
                            dict(set_temp=pt, hyst_ma=h_ma, hyst_pct=h_pct, hyst_temp=h_t))
                        self._clog("INF",
                            f"Hyst @ {pt:.2f}°C: {h_ma:+.4f} mA  ({h_pct:+.3f}%span)")
                self._calib_update_hyst()

                self._calib_set_progress(100)
                self._calib_set_status('✔  Complete')
                self._clog("STEP", "=== CALIBRATION COMPLETE ===")

        except Exception as e:
            self._clog("ERR", f"Calibration error: {e}")
            self._calib_set_status(f'Error: {e}')
        finally:
            self._fluke_close()
            self._calib_running = False
            self.after(0, lambda: self._calib_start_btn.configure(state='normal'))
            self.after(0, lambda: self._calib_stop_btn.configure(state='disabled'))

    # ── export / plot ─────────────────────────────────────────────────────────

    def _calib_export(self):
        if not self._calib_log_data:
            messagebox.showinfo("No Data", "Run a calibration first."); return
        path = filedialog.asksaveasfilename(
            defaultextension='.csv',
            filetypes=[('CSV files','*.csv'),('All files','*.*')],
            initialfile='hart_calibration.csv')
        if not path: return
        keys = list(self._calib_log_data[0].keys())
        with open(path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(self._calib_log_data)
            if self._calib_hyst_data:
                f.write('\n')
                w2 = csv.DictWriter(f, fieldnames=list(self._calib_hyst_data[0].keys()))
                w2.writeheader()
                w2.writerows(self._calib_hyst_data)
        self._clog("OK", f"Exported → {path}")
        messagebox.showinfo("Exported", f"Saved to:\n{path}")

    def _calib_plot(self):
        if not self._calib_log_data:
            messagebox.showinfo("No Data", "Run a calibration first."); return
        if not _HAS_MPL:
            messagebox.showerror("Missing Library",
                "matplotlib is not installed.\nRun:  pip install matplotlib")
            return

        asc  = [r for r in self._calib_log_data if r['cycle'] == 'Ascending']
        desc = [r for r in self._calib_log_data if r['cycle'] == 'Descending']

        fig, axes = plt.subplots(1, 3, figsize=(14, 5))
        fig.patch.set_facecolor('#f5f7f4')
        for ax in axes:
            ax.set_facecolor('#ffffff')
            ax.tick_params(colors='#567062')
            ax.xaxis.label.set_color('#567062')
            ax.yaxis.label.set_color('#567062')
            ax.title.set_color('#1f2a22')
            for spine in ax.spines.values():
                spine.set_edgecolor('#dfe7dd')

        def xs(rows): return [r['set_temp']     for r in rows]
        def ma_err(rows): return [r['ma_err']   for r in rows]
        def pct_err(rows): return [r['pct_span_err'] for r in rows]

        # Plot 1 — mA error
        ax = axes[0]
        ax.plot(xs(asc),  ma_err(asc),  'o-', color='#5b9bd5', label='Ascending')
        ax.plot(xs(desc), ma_err(desc), 's--', color='#b9770e', label='Descending')
        ax.axhline(0, color='#3a4a3a', linewidth=0.8)
        ax.set_title('mA Error'); ax.set_xlabel('Set Temp (°C)'); ax.set_ylabel('mA Error')
        ax.legend(facecolor='#ffffff', edgecolor='#dfe7dd', labelcolor='#567062')
        ax.grid(True, alpha=0.2, color='#dfe7dd')

        # Plot 2 — % span error
        ax = axes[1]
        ax.plot(xs(asc),  pct_err(asc),  'o-', color='#5b9bd5', label='Ascending')
        ax.plot(xs(desc), pct_err(desc), 's--', color='#b9770e', label='Descending')
        ax.axhline(0, color='#3a4a3a', linewidth=0.8)
        ax.set_title('% Span Error'); ax.set_xlabel('Set Temp (°C)'); ax.set_ylabel('% Span')
        ax.legend(facecolor='#ffffff', edgecolor='#dfe7dd', labelcolor='#567062')
        ax.grid(True, alpha=0.2, color='#dfe7dd')

        # Plot 3 — hysteresis bar
        ax = axes[2]
        if self._calib_hyst_data:
            labels = [f"{h['set_temp']:.1f}" for h in self._calib_hyst_data]
            vals   = [h['hyst_ma']            for h in self._calib_hyst_data]
            colors = ['#2f7a4e' if v >= 0 else '#c75146' for v in vals]
            ax.bar(labels, vals, color=colors, edgecolor='#f5f7f4', width=0.5)
        ax.axhline(0, color='#3a4a3a', linewidth=0.8)
        ax.set_title('Hysteresis (Asc − Desc)')
        ax.set_xlabel('Set Temp (°C)'); ax.set_ylabel('mA')
        ax.grid(True, alpha=0.2, color='#dfe7dd', axis='y')

        fig.suptitle('HART Transmitter Calibration', color='#2f7a4e', fontsize=13, fontweight='bold')
        fig.tight_layout()
        plt.show()

    # ── CERTIFICATE GENERATION ────────────────────────────────────────────────

    def _calib_browse_html(self):
        path = filedialog.askopenfilename(
            title="Select Certificate HTML Template",
            filetypes=[('HTML files', '*.html'), ('All files', '*.*')],
            initialfile='calibration_certificate_generator_hart.html')
        if path:
            self._calib_cert_html_var.set(path)

    def _calib_generate_cert(self):
        """
        Build a window.__hartCalib data blob from the live calibration results
        and inject it into the HTML certificate template, then open in browser.
        """
        if not self._calib_log_data:
            messagebox.showinfo("No Data",
                "Run a calibration first, then click Generate Certificate.")
            return

        html_path = self._calib_cert_html_var.get().strip()
        if not os.path.exists(html_path):
            messagebox.showerror("Template Not Found",
                f"Certificate HTML template not found:\n{html_path}\n\n"
                "Use the Browse button to locate the file, or place\n"
                "'calibration_certificate_generator_hart.html'\n"
                "in the same folder as this script.")
            return

        try:
            lrv = float(self._calib_lrv_var.get())
            urv = float(self._calib_urv_var.get())
            tol = float(self._calib_tol_var.get())
        except ValueError as e:
            messagebox.showerror("Invalid Value", f"Check LRV / URV / TOL fields: {e}")
            return

        # ── Build the results rows ────────────────────────────────────────────
        # Map internal calibration dict → 9-column certificate row:
        # [test_pt_pct, nominal_temp, applied_temp, iut_ma, expected_ma,
        #  error_ma, pct_span_err, tolerance_ma, status]
        result_rows = []
        for r in self._calib_log_data:
            ma_err_abs = abs(r.get('ma_err', float('nan')))
            status = 'PASS' if ma_err_abs <= tol else 'FAIL'
            sign   = '+' if r['ma_err'] >= 0 else ''
            esign  = '+' if r['pct_span_err'] >= 0 else ''
            result_rows.append({
                'cycle':         r['cycle'],
                'test_pt':       f"{r.get('theor_pct', 0.0):.1f} %",
                'nominal_temp':  f"{r['set_temp']:.2f} \u00b0C",
                'applied_temp':  f"{r.get('meas_pv', float('nan')):.3f} \u00b0C",
                'iut_ma':        f"{r['meas_ma']:.4f}",
                'expected_ma':   f"{r['theor_ma']:.4f}",
                'error_ma':      f"{sign}{r['ma_err']:.4f}",
                'pct_span_err':  f"{esign}{r['pct_span_err']:.4f} %",
                'tolerance_ma':  f"\u00b1{tol:.3f}",
                'status':        status,
            })

        # ── Hysteresis summary for remarks ────────────────────────────────────
        hyst_parts = []
        for h in self._calib_hyst_data:
            hyst_parts.append(
                f"{h['set_temp']:.1f}\u00b0C: {h['hyst_ma']:+.4f} mA"
                f" ({h['hyst_pct']:+.3f} % span)")
        hyst_str = '  |  '.join(hyst_parts) if hyst_parts else 'N/A'

        # ── Overall pass/fail for remarks ────────────────────────────────────
        pass_count = sum(1 for r in result_rows if r['status'] == 'PASS')
        all_pass   = pass_count == len(result_rows)
        result_summary = f"{'PASS' if all_pass else 'FAIL'} — {pass_count}/{len(result_rows)} points within tolerance."

        now = datetime.now()
        hart_data = {
            'type':           'temperature',
            'iut_desc':       'Temperature Transmitter (HART)',
            'iut_output':     '4 \u2013 20 mA DC  /  HART Rev.7',
            'iut_range':      f'{lrv:.1f} \u2013 {urv:.1f} \u00b0C',
            'iut_accuracy':   f'\u00b10.5 % of Span (mfr. spec.)',
            'iut_supply':     '24 V DC (external)',
            'cal_date':       now.strftime('%Y-%m-%d'),
            'cert_date':      now.strftime('%Y-%m-%d'),
            'valid_until':    now.replace(year=now.year + 1).strftime('%Y-%m-%d'),
            'amb_temp':       '23 \u00b0C \u00b1 1 \u00b0C',
            'tolerance':      tol,
            'result_rows':    result_rows,
            'hyst_summary':   hyst_str,
            'result_summary': result_summary,
            'remarks':        (
                f"Temperature transmitter calibrated over range {lrv:.1f}\u2013{urv:.1f} \u00b0C "
                f"using ascending and descending cycles. {result_summary}  "
                f"Hysteresis: {hyst_str}. "
                "Calibration performed using Fluke 9142 RTD field metrology well and "
                "HART CMD 2 (loop current) / CMD 1 (PV) averaging over multiple samples."
            ),
        }

        # ── POST calibration data to sign server, then open browser ─────────
        self._send_calib_to_server(hart_data, result_summary, pass_count, len(result_rows))

    # ── SEND CALIBRATION DATA TO SIGN SERVER ─────────────────────────────────

    def _send_calib_to_server(self, hart_data, result_summary, pass_count, total):
        """
        POST hart_data as JSON to http://localhost:5000/calib-data.
        On success, open the browser at http://localhost:5000 so the HTML
        page (served by sign_server.py) can fetch and auto-populate the form.
        Falls back to the old temp-file method if the server is unreachable.
        """
        url = f"{self._sign_server_url}/calib-data"
        payload = json.dumps(hart_data, ensure_ascii=False).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=payload,
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                status_code = resp.getcode()

            if status_code == 200:
                self._clog("OK",
                    f"Calibration data sent to sign server ({url}).  "
                    f"{pass_count}/{total} PASS.")
                webbrowser.open(self._sign_server_url)
                messagebox.showinfo("Certificate Generator",
                    f"Calibration data sent to the sign server.\n\n"
                    f"Result: {result_summary}\n\n"
                    f"Your browser will open http://localhost:5000 — the form\n"
                    f"is pre-filled.  Click 'Generate & Sign PDF' to download\n"
                    f"the digitally signed certificate.")
            else:
                raise ValueError(f"Server returned HTTP {status_code}")

        except (urllib.error.URLError, OSError, ValueError) as exc:
            self._clog("ERR",
                f"Could not reach sign server at {url}: {exc}.  "
                f"Falling back to local temp-file method.")
            self._calib_generate_cert_local(hart_data, result_summary, pass_count, total)

    def _calib_generate_cert_local(self, hart_data, result_summary, pass_count, total):
        """
        Fallback: inject hart_data into the HTML template and open as a
        local file:// URL (original behaviour before sign-server integration).
        """
        html_path = self._calib_cert_html_var.get().strip()
        if not os.path.exists(html_path):
            messagebox.showerror("Template Not Found",
                f"Certificate HTML template not found:\n{html_path}")
            return
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
        except OSError as e:
            messagebox.showerror("Read Error", str(e))
            return

        injection = (
            "<script>\n"
            "/* ── Auto-injected by HART Communicator (offline fallback) ── */\n"
            f"window.__hartCalib = {json.dumps(hart_data, indent=2, ensure_ascii=False)};\n"
            "</script>\n"
        )
        html_content = html_content.replace('</head>', injection + '</head>', 1)

        try:
            tmp = tempfile.NamedTemporaryFile(
                suffix='_hart_cert.html', delete=False,
                mode='w', encoding='utf-8')
            tmp.write(html_content)
            tmp_path = tmp.name
            tmp.close()
        except OSError as e:
            messagebox.showerror("Write Error", str(e))
            return

        subprocess.Popen(['firefox', tmp_path])
        self._clog("OK",
            f"Certificate opened in browser (offline).  "
            f"{pass_count}/{total} PASS.  Temp: {tmp_path}")
        messagebox.showinfo("Certificate Generated",
            f"Sign server unavailable — opened offline copy.\n\n"
            f"Result: {result_summary}\n\n"
            f"Review the form, then click 'Download PDF Certificate'.")


    # ── CUSTOM CERTIFICATE DIALOG ─────────────────────────────────────────────

    def _calib_custom_cert(self):
        """
        Open a modal dialog letting the user type in any calibration data,
        then POST it to the sign server (or fall back to local file) exactly
        like a live calibration would.
        """
        B  = '#f5f7f4'; B2 = '#edf2ec'; B3 = '#ffffff'; B4 = '#dfe7dd'
        F  = '#1f2a22'; F2 = '#567062'; F3 = '#7a8f82'
        G  = '#2f7a4e'; GD = '#dbeee2'
        R  = '#c75146'; RD = '#f7dfdb'

        win = tk.Toplevel(self)
        win.title("Custom Calibration Certificate")
        win.geometry("920x780")
        win.minsize(820, 640)
        win.configure(bg=B2)
        win.grab_set()

        outer = tk.Frame(win, bg=B2)
        outer.pack(fill='both', expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(outer, bg=B2, bd=0, highlightthickness=0)
        vbar   = ttk.Scrollbar(outer, orient='vertical', command=canvas.yview)
        canvas.configure(yscrollcommand=vbar.set)
        canvas.grid(row=0, column=0, sticky='nsew')
        vbar.grid(row=0, column=1, sticky='ns')

        inner = tk.Frame(canvas, bg=B2, padx=18, pady=14)
        win_id = canvas.create_window((0,0), window=inner, anchor='nw')
        inner.bind('<Configure>', lambda e: canvas.configure(
            scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>', lambda e: canvas.itemconfig(win_id, width=e.width))
        canvas.bind_all('<MouseWheel>',
            lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), 'units'))

        inner.columnconfigure(0, weight=1)
        inner.columnconfigure(1, weight=1)
        inner.columnconfigure(2, weight=1)
        inner.columnconfigure(3, weight=1)

        # helpers
        def sec_head(text, row):
            hf = tk.Frame(inner, bg=GD)
            hf.grid(row=row, column=0, columnspan=4, sticky='ew', pady=(14,6))
            tk.Label(hf, text=text, bg=GD, fg=G,
                     font=self.f_mono_sm, anchor='w', padx=10, pady=3).pack(fill='x')

        def le(label, row, col, default='', width=24):
            """labeled_entry: col in 0..3 — label+entry pair uses 2 grid cols each."""
            tk.Label(inner, text=label, bg=B2, fg=F3,
                     font=self.f_mono_sm).grid(
                row=row, column=col*2, sticky='w', padx=(0,4), pady=2)
            var = tk.StringVar(value=default)
            ttk.Entry(inner, textvariable=var, width=width,
                      font=self.f_mono_sm).grid(
                row=row, column=col*2+1, sticky='ew', pady=2, padx=(0,10))
            return var

        # ── Instrument ─────────────────────────────────────────────────────
        sec_head("INSTRUMENT UNDER TEST", 0)
        now    = datetime.now()
        v_desc     = le("Description",  1, 0, "Temperature Transmitter (HART)")
        v_mfr      = le("Manufacturer", 1, 1, "")
        v_model    = le("Model / Part", 2, 0, "")
        v_serial   = le("Serial No.",   2, 1, "")
        v_tag      = le("Tag / Asset",  3, 0, "")
        v_range    = le("Range",        3, 1, "0 \u2013 175 \u00b0C")
        v_output   = le("Output",       4, 0, "4 \u2013 20 mA DC / HART Rev.7")
        v_accuracy = le("Accuracy",     4, 1, "\u00b10.5 % of Span (mfr. spec.)")
        v_supply   = le("Supply",       5, 0, "24 V DC (external)")
        v_cond     = le("Condition",    5, 1, "Clean \u2013 No visible damage")

        # ── Cal Info ───────────────────────────────────────────────────────
        sec_head("CALIBRATION INFORMATION", 6)
        v_lrv      = le("LRV (\u00b0C)",      7, 0, "0",    width=10)
        v_urv      = le("URV (\u00b0C)",      7, 1, "175",  width=10)
        v_tol      = le("Tolerance mA",  8, 0, "0.080",width=10)
        v_amb      = le("Amb. Temp",     8, 1, "23 \u00b0C \u00b1 1 \u00b0C")
        v_caldate  = le("Cal. Date",     9, 0, now.strftime('%Y-%m-%d'))
        v_certdate = le("Cert. Date",    9, 1, now.strftime('%Y-%m-%d'))
        v_valid    = le("Valid Until",  10, 0, now.replace(year=now.year+1).strftime('%Y-%m-%d'))
        v_certno   = le("Cert. No.",   10, 1, "MIT/CAL/TT/2025/0001")

        # ── Results table ──────────────────────────────────────────────────
        sec_head("TEST RESULTS  (one row per test point)", 11)

        tbl_wrap = tk.Frame(inner, bg=B2)
        tbl_wrap.grid(row=12, column=0, columnspan=4, sticky='ew', pady=(0,4))
        tbl_wrap.columnconfigure(0, weight=1)

        col_labels = ["Cycle", "Set Temp \u00b0C", "Meas. mA",
                      "Exp. mA", "Meas. PV \u00b0C", "Status"]
        col_widths = [11, 10, 11, 11, 13, 7]

        hdr = tk.Frame(tbl_wrap, bg=B4)
        hdr.grid(row=0, column=0, sticky='ew')
        for ci, (lbl, w) in enumerate(zip(col_labels, col_widths)):
            tk.Label(hdr, text=lbl, bg=B4, fg=F2,
                     font=self.f_mono_sm, width=w, anchor='center',
                     relief='flat', padx=4, pady=3).grid(row=0, column=ci, padx=1)
        tk.Label(hdr, text="", bg=B4, width=3).grid(row=0, column=len(col_labels))

        rows_frame = tk.Frame(tbl_wrap, bg=B2)
        rows_frame.grid(row=1, column=0, sticky='ew')

        result_rows = []

        def add_row(cycle='Ascending', set_t='', meas_ma='',
                    exp_ma='', meas_pv='', status='PASS'):
            ri = len(result_rows)
            rf = tk.Frame(rows_frame, bg=B3 if ri%2==0 else B2)
            rf.pack(fill='x', pady=1)
            c_var  = tk.StringVar(value=cycle)
            s_var  = tk.StringVar(value=str(set_t))
            mm_var = tk.StringVar(value=str(meas_ma))
            em_var = tk.StringVar(value=str(exp_ma))
            pv_var = tk.StringVar(value=str(meas_pv))
            st_var = tk.StringVar(value=status)
            for vi, (var, w) in enumerate(zip(
                    [c_var, s_var, mm_var, em_var, pv_var],
                    col_widths[:5])):
                ttk.Entry(rf, textvariable=var, width=w,
                          font=self.f_mono_sm).grid(row=0, column=vi, padx=1, pady=1)
            ttk.Combobox(rf, textvariable=st_var,
                         values=['PASS','FAIL','N/A'],
                         state='readonly', width=col_widths[5],
                         font=self.f_mono_sm).grid(row=0, column=5, padx=1)
            row_tuple = (c_var, s_var, mm_var, em_var, pv_var, st_var, rf)
            result_rows.append(row_tuple)
            del_btn = tk.Button(rf, text="\u2715", bg=B3, fg=R,
                                activebackground=RD, font=self.f_mono_sm,
                                bd=0, cursor='hand2', padx=4)
            del_btn.grid(row=0, column=6)
            del_btn.configure(command=lambda f=rf, t=row_tuple: (
                f.destroy(), result_rows.remove(t)))

        # seed 5-point ascending + descending
        for cname in ('Ascending', 'Descending'):
            pts = [0, 25, 50, 75, 100]
            if cname == 'Descending': pts = list(reversed(pts))
            for pct in pts:
                add_row(cycle=cname, set_t=f"{pct:.1f}")

        # Buttons below table
        bf = tk.Frame(inner, bg=B2)
        bf.grid(row=13, column=0, columnspan=4, sticky='w', pady=(4,10))
        tk.Button(bf, text="+ Add Ascending Row",
                  bg=B3, fg='#2c6fb7', activebackground=B4,
                  font=self.f_mono_sm, bd=0, cursor='hand2', padx=8, pady=3,
                  command=lambda: add_row('Ascending')).pack(side='left', padx=(0,6))
        tk.Button(bf, text="+ Add Descending Row",
                  bg=B3, fg='#b9770e', activebackground=B4,
                  font=self.f_mono_sm, bd=0, cursor='hand2', padx=8, pady=3,
                  command=lambda: add_row('Descending')).pack(side='left', padx=(0,6))
        tk.Button(bf, text="\u27f3 Auto-fill Expected mA",
                  bg=B3, fg=G, activebackground=GD,
                  font=self.f_mono_sm, bd=0, cursor='hand2', padx=8, pady=3,
                  command=lambda: self._custom_cert_autofill(
                      result_rows, v_lrv, v_urv)).pack(side='left')

        # ── Remarks ────────────────────────────────────────────────────────
        sec_head("REMARKS", 14)
        rem_txt = tk.Text(inner, bg=B3, fg=F, font=self.f_mono_sm,
                          height=3, relief='flat', insertbackground=G)
        rem_txt.grid(row=15, column=0, columnspan=4, sticky='ew', pady=(0,8))
        rem_txt.insert('1.0',
            "Instrument calibrated manually. Results entered by operator.")

        # ── Bottom bar ─────────────────────────────────────────────────────
        act = tk.Frame(win, bg=B4, padx=14, pady=8)
        act.pack(fill='x', side='bottom')
        status_lbl = tk.Label(act, text="", bg=B4, fg=F2, font=self.f_mono_sm)
        status_lbl.pack(side='left')
        tk.Button(act, text="Cancel", bg=B3, fg=F3,
                  activebackground=B4, font=self.f_mono_sm,
                  bd=0, cursor='hand2', padx=12, pady=5,
                  command=win.destroy).pack(side='right', padx=(6,0))
        tk.Button(act, text="\U0001f4c4  Generate & Sign Certificate",
                  bg=GD, fg=G, activebackground='#1e4d32',
                  font=font.Font(family='Consolas', size=10, weight='bold'),
                  bd=0, cursor='hand2', padx=14, pady=5,
                  command=lambda: self._custom_cert_submit(
                      win, status_lbl,
                      v_desc, v_mfr, v_model, v_serial, v_tag,
                      v_range, v_output, v_accuracy, v_supply, v_cond,
                      v_lrv, v_urv, v_tol, v_amb,
                      v_caldate, v_certdate, v_valid, v_certno,
                      result_rows, rem_txt,
                  )).pack(side='right')

    # ─────────────────────────────────────────────────────────────────────────

    def _custom_cert_autofill(self, result_rows, v_lrv, v_urv):
        """Compute Expected mA from LRV/URV + Set Temp; mirror PV if blank."""
        try:
            lrv     = float(v_lrv.get())
            urv     = float(v_urv.get())
            span_t  = urv - lrv
            span_ma = 16.0
        except ValueError:
            messagebox.showerror("Invalid Range", "Check LRV and URV values.")
            return
        if span_t == 0:
            messagebox.showerror("Invalid Range", "URV and LRV cannot be equal.")
            return
        for c_var, s_var, mm_var, em_var, pv_var, st_var, _ in result_rows:
            try:
                temp   = float(s_var.get())
                exp_ma = 4.0 + ((temp - lrv) / span_t) * span_ma
                em_var.set(f"{exp_ma:.4f}")
                if not pv_var.get().strip():
                    pv_var.set(f"{temp:.3f}")
            except ValueError:
                pass

    def _custom_cert_submit(self, win, status_lbl,
                             v_desc, v_mfr, v_model, v_serial, v_tag,
                             v_range, v_output, v_accuracy, v_supply, v_cond,
                             v_lrv, v_urv, v_tol, v_amb,
                             v_caldate, v_certdate, v_valid, v_certno,
                             result_rows, rem_txt):
        """Validate, build hart_data, dispatch to sign server."""
        try:
            lrv = float(v_lrv.get())
            urv = float(v_urv.get())
            tol = float(v_tol.get())
        except ValueError as e:
            messagebox.showerror("Invalid Value",
                f"LRV / URV / Tolerance must be numbers: {e}")
            return
        span_t  = urv - lrv
        span_ma = 16.0
        if not result_rows:
            messagebox.showerror("No Test Points",
                "Add at least one test-point row before generating.")
            return

        import math
        result_row_dicts = []
        errors = []
        for i, (c_var, s_var, mm_var, em_var, pv_var, st_var, _) in enumerate(result_rows):
            cycle = c_var.get().strip() or 'Ascending'
            try:
                set_temp = float(s_var.get())
            except ValueError:
                errors.append(f"Row {i+1}: Set Temp is not a number.")
                continue
            try:
                meas_ma = float(mm_var.get()) if mm_var.get().strip() else float('nan')
            except ValueError:
                meas_ma = float('nan')
            try:
                if em_var.get().strip():
                    exp_ma = float(em_var.get())
                elif span_t:
                    exp_ma = 4.0 + ((set_temp - lrv) / span_t) * span_ma
                else:
                    exp_ma = float('nan')
            except ValueError:
                exp_ma = float('nan')
            try:
                meas_pv = float(pv_var.get()) if pv_var.get().strip() else set_temp
            except ValueError:
                meas_pv = set_temp

            ma_err       = meas_ma - exp_ma if not (math.isnan(meas_ma) or math.isnan(exp_ma)) else float('nan')
            pct_span_err = (ma_err / span_ma * 100.0) if not math.isnan(ma_err) else float('nan')
            status       = st_var.get() or 'PASS'
            if not math.isnan(ma_err) and status != 'N/A':
                status = 'PASS' if abs(ma_err) <= tol else 'FAIL'

            fmt_ma  = lambda x, s='': f"{s}{x:.4f}" if not math.isnan(x) else '\u2014'
            fmt_pct = lambda x, s='': f"{s}{x:.4f} %" if not math.isnan(x) else '\u2014'
            s_sign  = '+' if (not math.isnan(ma_err) and ma_err >= 0) else ''
            p_sign  = '+' if (not math.isnan(pct_span_err) and pct_span_err >= 0) else ''
            pct_of_range = ((set_temp - lrv) / span_t * 100) if span_t else 0
            result_row_dicts.append({
                'cycle':        cycle,
                'test_pt':      f"{pct_of_range:.1f} %",
                'nominal_temp': f"{set_temp:.2f} \u00b0C",
                'applied_temp': f"{meas_pv:.3f} \u00b0C",
                'iut_ma':       fmt_ma(meas_ma),
                'expected_ma':  fmt_ma(exp_ma),
                'error_ma':     fmt_ma(ma_err, s_sign),
                'pct_span_err': fmt_pct(pct_span_err, p_sign),
                'tolerance_ma': f"\u00b1{tol:.3f}",
                'status':       status,
            })

        if errors:
            messagebox.showerror("Row Errors", "\n".join(errors))
            return

        pass_count     = sum(1 for r in result_row_dicts if r['status'] == 'PASS')
        all_pass       = pass_count == len(result_row_dicts)
        result_summary = (f"{'PASS' if all_pass else 'FAIL'} \u2014 "
                          f"{pass_count}/{len(result_row_dicts)} points within tolerance.")

        remarks = rem_txt.get('1.0', 'end').strip() or (
            f"Instrument calibrated over range {lrv:.1f}\u2013{urv:.1f} \u00b0C. "
            f"{result_summary}")

        hart_data = {
            'type':           'temperature',
            'iut_desc':       v_desc.get(),
            'iut_mfr':        v_mfr.get(),
            'iut_model':      v_model.get(),
            'iut_serial':     v_serial.get(),
            'iut_tag':        v_tag.get(),
            'iut_output':     v_output.get(),
            'iut_range':      v_range.get(),
            'iut_accuracy':   v_accuracy.get(),
            'iut_supply':     v_supply.get(),
            'iut_condition':  v_cond.get(),
            'cal_date':       v_caldate.get(),
            'cert_date':      v_certdate.get(),
            'valid_until':    v_valid.get(),
            'cert_no':        v_certno.get(),
            'amb_temp':       v_amb.get(),
            'tolerance':      tol,
            'result_rows':    result_row_dicts,
            'hyst_summary':   'N/A (manual entry)',
            'result_summary': result_summary,
            'remarks':        remarks,
        }

        status_lbl.configure(text="Sending to sign server\u2026", fg=F2)
        win.update_idletasks()
        self._clog("INF",
            f"Custom cert: {len(result_row_dicts)} rows \u2014 {result_summary}")
        win.destroy()
        self._send_calib_to_server(
            hart_data, result_summary, pass_count, len(result_row_dicts))

    # ── PORTS ─────────────────────────────────────────────────────────────────


    def _refresh_ports(self):
        try:
            import serial.tools.list_ports
            ports = [p.device for p in serial.tools.list_ports.comports()]
        except ImportError:
            ports = []
        self._port_cb['values'] = ports
        if ports and not self._port_var.get():
            self._port_var.set(ports[0])
        if not ports:
            self._log("ERR",
                "No serial ports found. Is pyserial installed and modem connected?")

    # ── CONNECT ───────────────────────────────────────────────────────────────

    def _connect(self):
        try:
            import serial
        except ImportError:
            self._log("ERR", "pyserial not installed.  Run:  pip install pyserial")
            return

        port = self._port_var.get()
        if not port:
            self._log("ERR", "No port selected. Click ↺ to refresh port list.")
            return
        baud = int(self._baud_var.get())
        try:
            self._ser = serial.Serial(
                port=port, baudrate=baud,
                bytesize=8, parity=self._parity_var.get(),
                stopbits=1,
                timeout=float(self._timeout_var.get()) / 1000,
                rtscts=False, dsrdtr=False,
            )
            self._ser.rts = False
            # ── CP210x / RPi low-latency fix ──────────────────────────────
            try:
                import fcntl, array
                buf = array.array('B', [0] * 72)
                fcntl.ioctl(self._ser.fd, 0x541E, buf)   # TIOCGSERIAL
                buf[27] |= 0x20                           # ASYNC_LOW_LATENCY
                fcntl.ioctl(self._ser.fd, 0x541F, buf)   # TIOCSSERIAL
                self._log("INF", "Low-latency mode set on serial port.")
            except Exception as e:
                self._log("INF", f"Low-latency ioctl skipped ({e})")
            # ──────────────────────────────────────────────────────────────
            self._busy = False
            parity_name = {'O':'Odd','E':'Even','M':'Mark','S':'Space','N':'None'}.get(
                self._parity_var.get(), self._parity_var.get())
            self._set_status(f"● {port} @ {baud}", '#2f7a4e')
            self._log("INF", f"Opened {port}  {baud} baud  8-{parity_name}-1")
            self.after(200, lambda: self._bg(self._run_cmd, 0))
        except Exception as e:
            self._log("ERR", f"Could not open port: {e}")
            self._set_status("● error", '#c75146')

    def _disconnect(self):
        self._burst_active = False
        if self._ser:
            try: self._ser.close()
            except Exception: pass
            self._ser = None
        self._long_addr = None
        self._laddr_var.set('')
        self._set_status("● disconnected", '#7a8f82')
        self._log("INF", "Disconnected.")

    # ── COMMANDS ──────────────────────────────────────────────────────────────

    def _select_cmd(self, n):
        self._current_cmd = n
        for k, b in self._cmd_btns.items():
            b.configure(bg='#edf2ec',
                        fg='#567062' if k != -1 else '#b9770e')
        if n in self._cmd_btns:
            self._cmd_btns[n].configure(bg='#ffffff', fg='#1f2a22')
        if n == -1:
            self._cmd_badge.configure(text="CMD RAW")
            self._cmd_title_lbl.configure(text="Custom / Raw Command")
            self._cust_frame.grid()
        else:
            name = next((nm for c, nm in COMMANDS if c == n), f"Command {n}")
            self._cmd_badge.configure(text=f"CMD {n:03d}")
            self._cmd_title_lbl.configure(text=name)
            self._cust_frame.grid_remove()

    def _send_cmd(self):
        if not self._ser or not self._ser.is_open:
            self._log("ERR", "Not connected."); return
        if self._current_cmd is None:
            self._log("ERR", "Select a command first."); return
        n = self._current_cmd
        if n == -1:
            try: n = int(self._cust_cmd_var.get())
            except ValueError:
                self._log("ERR", "Invalid command number."); return
        self._bg(self._run_cmd, n)

    def _bg(self, fn, *args):
        threading.Thread(target=fn, args=args, daemon=True).start()

    def _run_cmd(self, cmd):
        if self._busy:
            self._log("INF", "Busy — previous command still running."); return
        if not self._ser or not self._ser.is_open:
            self._log("INF", "Not connected."); return
        self._busy = True
        try:
            gap_s   = max(0, int(self._gap_var.get() or 50)) / 1000
            retries = int(self._retries_var.get() or 2)
            pream   = int(self._pre_var.get() or 5)
            timeout = max(0.5, float(self._timeout_var.get() or 3000) / 1000)

            data = b''
            if self._current_cmd == -1:
                hs = self._cust_data_var.get().strip()
                if hs:
                    try: data = bytes(int(h, 16) for h in hs.split())
                    except ValueError:
                        self._log("ERR", "Bad hex data."); return

            time.sleep(gap_s)
            use_long   = self._long_var.get() and self._long_addr is not None
            long_addr  = self._long_addr if use_long else None
            short_addr = int(self._addr_var.get() or 0)

            try:
                frame = build_frame(cmd, data, pream, long_addr=long_addr, short_addr=short_addr)
            except Exception as e:
                self._log("ERR", f"build_frame failed: {e}"); return

            mode = (f"LONG {bytes(long_addr).hex(' ').upper()}"
                    if use_long else f"SHORT addr={short_addr}")
            self._log("TX", f"CMD {cmd} [{mode}]  {frame.hex(' ').upper()}")

            for attempt in range(retries + 1):
                try:
                    raw = self._transact(frame, timeout)
                except Exception as e:
                    self._log("ERR", f"Serial error: {e}"); return

                if not raw:
                    if attempt < retries:
                        self._log("INF", f"No response · retry {attempt+1}/{retries}...")
                        time.sleep(gap_s)
                        continue
                    else:
                        self._log("ERR", "No response after all retries."); return

                self._log("RX", raw.hex(' ').upper())

                try:
                    parsed = parse_frame(raw)
                except Exception as e:
                    self._log("ERR", f"parse_frame exception: {e}"); return

                if not parsed:
                    diag = diagnose_frame(raw)
                    self._log("ERR", f"Parse fail: {diag}")
                    return

                rc = parsed['rc']
                if rc != 0:
                    has_data = len(parsed['data']) > 0
                    # HART RC byte classification:
                    #   0x00        = no error
                    #   0x01-0x7F   = command-specific error (no data)
                    #   0x80-0xBF   = communications error (bit7=1, bit6=0) — device UART problem, no data
                    #   0xC0-0xFF   = device status warning (bit7=1, bit6=1) — data IS valid
                    is_comm_err   = (rc & 0xC0) == 0x80   # 0x80-0xBF
                    is_dev_status = (rc & 0xC0) == 0xC0   # 0xC0-0xFF
                    is_cmd_err    = (rc & 0x80) == 0x00 and rc != 0  # 0x01-0x7F
                    is_hard       = (is_comm_err or is_cmd_err) and not has_data

                    if is_dev_status:
                        self._log("INF", f"DevStatus=0x{rc:02X} — {rc_desc(rc)}")
                        # data is valid, fall through to decode
                    elif is_comm_err:
                        self._log("ERR", f"COMM ERR RC=0x{rc:02X} — {rc_desc(rc)}  DevStat=0x{parsed['dev_stat']:02X}")
                        if is_hard: return
                    else:
                        self._log("ERR" if is_hard else "INF",
                                  f"CMD RC=0x{rc:02X} — {rc_desc(rc)}  DevStat=0x{parsed['dev_stat']:02X}")
                        if is_hard: return

                try:
                    self._decode(cmd, parsed)
                except Exception as e:
                    self._log("ERR", f"Decode error: {e}")
                return

        finally:
            self._busy = False

    def _transact(self, frame, timeout):
        """Write frame, read bytes until a checksum-valid HART response is found."""
        ser = self._ser
        rts = self._rts_var.get()
        raw = self._raw_var.get()

        with self._lock:
            ser.reset_input_buffer()
            ser.reset_output_buffer()

            # RTS HIGH → transmit
            if rts:
                ser.rts = True
                time.sleep(0.010)  # give modem time to switch to TX direction

            ser.write(frame)
            ser.flush()

            # Wait for OS TX buffer to actually drain (works correctly with low_latency ioctl)
            t0 = time.time()
            while ser.out_waiting and (time.time() - t0) < 1.0:
                time.sleep(0.001)

            # One final byte time for the last byte to finish clocking out
            ms_per_byte = 11 / ser.baudrate * 1000
            time.sleep(ms_per_byte / 1000 + 0.003)   # ? was: len(frame)*ms_per_byte/1000 + 0.025

            # RTS LOW ? receive
            if rts:
                ser.rts = False
                time.sleep(0.005)

            # Accumulate bytes until parse_frame finds a CS-valid response or timeout.
            # TX echo bytes are in the buffer too — parse_frame skips them naturally
            # because master STX=0x02/0x82 != 0x06/0x86, and any false 0x06/0x86
            # matches inside echo data fail the CS check.
            buf = b''
            deadline = time.time() + timeout
            while time.time() < deadline:
                waiting = ser.in_waiting
                chunk = ser.read(waiting if waiting else 1)
                if chunk:
                    buf += chunk
                    if raw:
                        self._log("INF", f"RAW← {chunk.hex(' ').upper()}")
                    if parse_frame(buf) is not None:
                        return buf
                else:
                    time.sleep(0.005)

            return buf if buf else None

    # ── DECODE ────────────────────────────────────────────────────────────────

    def _decode(self, cmd, p):
        d = p['data']
        vals = {}

        if cmd == 0 and len(d) >= 12:
            self._log("INF", f"CMD0 raw ({len(d)}B): {bytes(d).hex(' ').upper()}")
            mfr_id        = d[1]
            dev_type      = d[2]
            # CMD 0 data layout: [0xFE, mfr_id, dev_type, req_preambles,
            #                      hart_rev, dev_rev, sw_rev, hw_rev, flags,
            #                      dev_id[0], dev_id[1], dev_id[2]]
            req_preambles = d[3]   # was d[4] — off by one
            hart_rev      = d[4]   # was d[5]
            dev_rev       = d[5]   # was d[6]
            sw_rev        = d[6]   # was d[7]
            dev_id        = bytes(d[9:12])
            long_addr     = bytes([mfr_id & 0x3F, dev_type]) + dev_id
            self._long_addr = long_addr
            wire_addr     = bytes([long_addr[0] | 0x80]) + long_addr[1:]
            self.after(0, lambda s=wire_addr.hex(' ').upper(): self._laddr_var.set(f"Long addr: {s}"))
            self._log("INF", f"Long addr (wire): {wire_addr.hex(' ').upper()}")
            self._log("INF", f"HART rev={hart_rev}  Device rev={dev_rev}  SW rev={sw_rev}")
            self._log("INF", f"Required preambles={req_preambles}  sending={self._pre_var.get()}")
            try:
                if int(self._pre_var.get()) < req_preambles:
                    self._log("ERR",
                        f"TOO FEW PREAMBLES — device needs {req_preambles}, "
                        f"sending {self._pre_var.get()}. Increase and reconnect.")
            except ValueError:
                pass
            vals = {
                'mfr':      f"0x{mfr_id:02X}",
                'type':     f"0x{dev_type:02X}",
                'uid':      dev_id.hex(' ').upper(),
                'hart_rev': hart_rev,
                'dev_rev':  dev_rev,
            }
        elif cmd == 1 and len(d) >= 5:
            pv = f32(d, 1); u = unit_name(d[0])
            vals = {'unit': f"0x{d[0]:02X}({u})", 'pv': f"{pv:.4f}"}
            self.after(0, lambda: self._upv('pv', f"{pv:.3f}", u))
        elif cmd == 2 and len(d) >= 8:
            ma = f32(d, 0); pct = f32(d, 4)
            vals = {'mA': f"{ma:.4f}", 'pct': f"{pct:.3f}"}
            self.after(0, lambda: [self._upv('ma', f"{ma:.3f}", 'mA'),
                                    self._upv('pct', f"{pct:.2f}", '%')])
        elif cmd == 3 and len(d) >= 9:
            # CMD 3 data layout:
            #   d[0:4]  = loop current (IEEE 754 float, mA)  — no unit prefix
            #   d[4]    = PV unit code
            #   d[5:9]  = PV value (float)
            #   d[9]    = SV unit code   (if present)
            #   d[10:14]= SV value
            #   d[14]   = TV unit code   (if present)
            #   d[15:19]= TV value
            #   d[19]   = QV unit code   (if present)
            #   d[20:24]= QV value
            # (was: pv=f32(d,1) / pu=unit_name(d[0]) — both wrong;
            #  d[0] is the high byte of the loop current float, not a unit code)
            ma = f32(d, 0)
            pu = unit_name(d[4]);  pv = f32(d, 5)
            su = unit_name(d[9])  if len(d) >= 14 else '?';  sv = f32(d, 10) if len(d) >= 14 else float('nan')
            tu = unit_name(d[14]) if len(d) >= 19 else '?';  tv = f32(d, 15) if len(d) >= 19 else float('nan')
            qu = unit_name(d[19]) if len(d) >= 24 else '?';  qv = f32(d, 20) if len(d) >= 24 else float('nan')
            vals = {
                'mA': f"{ma:.4f}",
                'pv': f"{pv:.3f} {pu}", 'sv': f"{sv:.3f} {su}",
                'tv': f"{tv:.3f} {tu}", 'qv': f"{qv:.3f} {qu}",
            }
            self.after(0, lambda: [
                self._upv('ma', f"{ma:.3f}", 'mA'),
                self._upv('pv', f"{pv:.3f}", pu), self._upv('sv', f"{sv:.3f}", su),
                self._upv('tv', f"{tv:.3f}", tu), self._upv('qv', f"{qv:.3f}", qu),
            ])
        elif cmd == 12:
            vals = {'msg': bytes(d).decode('ascii','replace').strip()}
        elif cmd == 13:
            tag  = bytes(d[:8]).decode('ascii','replace').strip()
            desc = bytes(d[8:24]).decode('ascii','replace').strip()
            vals = {'tag': tag, 'desc': desc}
        elif cmd == 14 and len(d) >= 10:
            vals = {
                'unit':  f"0x{d[0]:02X}({unit_name(d[0])})",
                'hi':    f"{f32(d,2):.4f}",
                'lo':    f"{f32(d,6):.4f}",
            }
        elif cmd == 15 and len(d) >= 15:
        # CMD 15 data layout (this device):
            #   d[0:2]  = prefix bytes (alarm select, transfer function flags)
            #   d[2]    = PV unit code
            #   d[3:7]  = upper range value (IEEE 754 float)
            #   d[7:11] = lower range value (IEEE 754 float)
            #   d[11:15]= damping value (IEEE 754 float, seconds)
            #
            # Verified against RX:
            #   00 00 | 20 | 43 2F 00 00 | 00 00 00 00 | 3F 80 00 00
            #   pfx     °C   175.0          0.0           1.0 s
            u = unit_name(d[2])
            hi   = f32(d,  3)
            lo   = f32(d,  7)
            damp = f32(d, 11)
            vals = {
                'unit': f"0x{d[2]:02X}({u})",
                'hi':   f"{hi:.4f} {u}",
                'lo':   f"{lo:.4f} {u}",
                'damp': f"{damp:.3f} s",
            }

        elif cmd == 48 and d:
            raw48 = bytes(d).hex(' ').upper()
            # Decode standard CMD 48 device status bits (first 2 bytes)
            bits = []
            if len(d) >= 1:
                b0 = d[0]
                if b0 & 0x80: bits.append("Primary var OOR")
                if b0 & 0x40: bits.append("Non-primary var OOR")
                if b0 & 0x20: bits.append("Loop current fixed")
                if b0 & 0x10: bits.append("Loop current saturated")
                if b0 & 0x08: bits.append("Non-primary var OOR warn")
                if b0 & 0x04: bits.append("Primary var OOR warn")
                if b0 & 0x02: bits.append("Maintenance required")
                if b0 & 0x01: bits.append("Device variable alert")
            if len(d) >= 2:
                b1 = d[1]
                if b1 & 0x80: bits.append("Critical power failure")
                if b1 & 0x40: bits.append("Failure")
                if b1 & 0x20: bits.append("Out of specification")
                if b1 & 0x10: bits.append("Function check")
                if b1 & 0x08: bits.append("Simulation active")
                if b1 & 0x04: bits.append("Non-volatile mem defect")
                if b1 & 0x02: bits.append("Volatile mem error")
                if b1 & 0x01: bits.append("WD reset")
            status_str = ', '.join(bits) if bits else 'No faults'
            vals = {'raw': raw48, 'status': status_str}
        elif d:
            vals = {'data': bytes(d).hex(' ').upper()}

        out = '  '.join(f"{k}={v}" for k, v in vals.items())
        self._log("OK", out or f"CMD {cmd} OK")

    def _upv(self, key, val, unit=''):
        self._pv_val[key].set(val)
        self._pv_unit[key].set(f" {unit}" if unit else '')

    # ── BURST ─────────────────────────────────────────────────────────────────

    def _toggle_burst(self):
        if self._burst_active:
            self._burst_active = False
            self._burst_btn_w.configure(text='Burst')
            self._log("INF", "Burst stopped.")
        else:
            if not self._ser or not self._ser.is_open:
                self._log("ERR", "Not connected."); return
            cmd = self._current_cmd if (self._current_cmd and self._current_cmd >= 0) else 3
            self._burst_active = True
            self._burst_btn_w.configure(text='■ Stop')
            self._log("INF", f"Burst — CMD {cmd}")
            threading.Thread(target=self._burst_loop, args=(cmd,), daemon=True).start()

    def _burst_loop(self, cmd):
        iv = max(0.5, int(self._gap_var.get() or 1000) / 1000)
        while self._burst_active:
            self._run_cmd(cmd)
            time.sleep(iv)

    # ── LOG / STATUS ──────────────────────────────────────────────────────────

    def _log(self, tag, msg):
        def _w():
            ts = datetime.now().strftime('%H:%M:%S')
            self._terminal.configure(state='normal')
            self._terminal.insert('end', f"[{ts}] ", 'ts')
            self._terminal.insert('end', f"{tag:<3} ", tag)
            self._terminal.insert('end', msg + '\n')
            self._terminal.configure(state='disabled')
            self._terminal.see('end')
        self.after(0, _w)

    def _clear_log(self):
        self._terminal.configure(state='normal')
        self._terminal.delete('1.0', 'end')
        self._terminal.configure(state='disabled')

    def _set_status(self, text, color='#7a8f82'):
        def _w():
            self._status_var.set(text)
            self._status_lbl.configure(fg=color)
        self.after(0, _w)


if __name__ == '__main__':
    try:
        import serial  # noqa
    except ImportError:
        root = tk.Tk(); root.withdraw()
        from tkinter import messagebox
        messagebox.showerror(
            "Missing dependency",
            "pyserial is not installed.\n\n"
            "Run this in your terminal:\n\n"
            "    pip install pyserial\n\n"
            "Then run the script again.")
        raise SystemExit(1)

    HartApp().mainloop()
