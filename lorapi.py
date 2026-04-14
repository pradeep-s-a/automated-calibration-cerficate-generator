#!/usr/bin/env python3
"""
SX1278 LoRa helper for Raspberry Pi.

This module keeps the Pi-side SX1278 settings in one place so other Python
programs can send or receive packets without duplicating register code.

Examples:
    python lorapi.py rx
    python lorapi.py tx --text "hello"
    python lorapi.py tx --json "{\"t\":\"ping\"}"
    python lorapi.py tx --hex "04422d999a40a00000"
"""

import argparse
import json
import time

try:
    import spidev
    import RPi.GPIO as GPIO
    _HW_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - depends on Raspberry Pi runtime
    spidev = None
    GPIO = None
    _HW_IMPORT_ERROR = exc


# BCM GPIO pin numbers (not physical pin numbers)
DEFAULT_RST_PIN = 22
DEFAULT_DIO0_PIN = 4

# SX1278 registers
REG_FIFO = 0x00
REG_OP_MODE = 0x01
REG_PA_CONFIG = 0x09
REG_LNA = 0x0C
REG_FIFO_ADDR_PTR = 0x0D
REG_FIFO_TX_BASE = 0x0E
REG_FIFO_RX_BASE = 0x0F
REG_FIFO_RX_CURR = 0x10
REG_IRQ_FLAGS = 0x12
REG_RX_NB_BYTES = 0x13
REG_PKT_SNR = 0x19
REG_PKT_RSSI = 0x1A
REG_MODEM_CFG1 = 0x1D
REG_MODEM_CFG2 = 0x1E
REG_HOP_CHANNEL = 0x1C
REG_PAYLOAD_LENGTH = 0x22
REG_MODEM_CFG3 = 0x26
REG_SYNC_WORD = 0x39
REG_DIO_MAPPING1 = 0x40
REG_FRF_MSB = 0x06
REG_FRF_MID = 0x07
REG_FRF_LSB = 0x08
REG_VERSION = 0x42

MODE_LORA = 0x80
MODE_LF = 0x08
MODE_SLEEP = 0x00
MODE_STDBY = 0x01
MODE_TX = 0x03
MODE_RX_CONT = 0x05

IRQ_RX_TIMEOUT = 0x80
IRQ_RX_DONE = 0x40
IRQ_PAYLOAD_CRC_ERROR = 0x20
IRQ_TX_DONE = 0x08


class SX1278LoRa:
    """Minimal SX1278 helper for 433 MHz LoRa links on Raspberry Pi."""

    def __init__(
        self,
        frequency_hz=433_000_000,
        rst_pin=DEFAULT_RST_PIN,
        dio0_pin=DEFAULT_DIO0_PIN,
        spi_bus=0,
        spi_device=0,
        spi_speed_hz=500_000,
        tx_power_dbm=2,
    ):
        self.frequency_hz = int(frequency_hz)
        self.rst_pin = int(rst_pin)
        self.dio0_pin = int(dio0_pin)
        self.spi_bus = int(spi_bus)
        self.spi_device = int(spi_device)
        self.spi_speed_hz = int(spi_speed_hz)
        self.tx_power_dbm = int(tx_power_dbm)
        self.spi = None
        self._opened = False
        self._op_mode_base = MODE_LORA | (MODE_LF if self.frequency_hz < 525_000_000 else 0x00)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _require_hardware(self):
        if _HW_IMPORT_ERROR is not None:
            raise RuntimeError(
                "SX1278 access requires Raspberry Pi packages 'spidev' and "
                "'RPi.GPIO'. Run this on the Pi connected to the LoRa module."
            ) from _HW_IMPORT_ERROR

    def write_reg(self, reg, value):
        self.spi.xfer2([reg | 0x80, value & 0xFF])

    def read_reg(self, reg):
        return self.spi.xfer2([reg & 0x7F, 0x00])[1]

    def write_bytes(self, reg, data):
        self.spi.xfer2([reg | 0x80] + list(data))

    def read_bytes(self, reg, count):
        return bytes(self.spi.xfer2([reg & 0x7F] + [0x00] * count)[1:])

    def _reset_module(self):
        GPIO.output(self.rst_pin, GPIO.LOW)
        time.sleep(0.02)
        GPIO.output(self.rst_pin, GPIO.HIGH)
        time.sleep(0.05)

    def _set_mode(self, mode, settle=0.01, attempts=3):
        target = self._op_mode_base | int(mode)
        observed = None
        for _ in range(max(1, int(attempts))):
            self.write_reg(REG_OP_MODE, target)
            time.sleep(max(0.0, float(settle)))
            observed = self.read_reg(REG_OP_MODE)
            if observed == target:
                return observed
        return observed

    def _read_dio0(self):
        if GPIO is None:
            return None
        try:
            return int(GPIO.input(self.dio0_pin))
        except Exception:
            return None

    def _set_frequency(self, freq_hz):
        frf = int((freq_hz / 32_000_000.0) * 524288)
        self.write_reg(REG_FRF_MSB, (frf >> 16) & 0xFF)
        self.write_reg(REG_FRF_MID, (frf >> 8) & 0xFF)
        self.write_reg(REG_FRF_LSB, frf & 0xFF)

    def _set_tx_power(self, power_dbm):
        # PA_BOOST path, 2-17 dBm.
        power_dbm = max(2, min(17, int(power_dbm)))
        self.write_reg(REG_PA_CONFIG, 0x80 | (power_dbm - 2))

    def open(self, receive_mode=False):
        if self._opened:
            if receive_mode:
                self.start_receive()
            return self

        self._require_hardware()

        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.rst_pin, GPIO.OUT, initial=GPIO.HIGH)
        GPIO.setup(self.dio0_pin, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)

        self.spi = spidev.SpiDev()
        self.spi.open(self.spi_bus, self.spi_device)
        self.spi.max_speed_hz = self.spi_speed_hz
        self.spi.mode = 0b00

        self._reset_module()

        version = self.read_reg(REG_VERSION)
        if version != 0x12:
            self.close()
            raise RuntimeError(
                f"SX1278 not found on SPI{self.spi_bus}.{self.spi_device}. "
                f"Got version 0x{version:02X}; check wiring."
            )

        self._set_mode(MODE_SLEEP, settle=0.01, attempts=5)
        self._set_frequency(self.frequency_hz)
        self.write_reg(REG_FIFO_TX_BASE, 0x80)
        self.write_reg(REG_FIFO_RX_BASE, 0x00)
        self.write_reg(REG_FIFO_ADDR_PTR, 0x00)
        self.write_reg(REG_MODEM_CFG1, 0x72)  # BW 125kHz, CR 4/5, explicit header
        self.write_reg(REG_MODEM_CFG2, 0xB4)  # SF11, CRC on
        self.write_reg(REG_MODEM_CFG3, 0x04)  # AGC auto on
        self.write_reg(REG_SYNC_WORD, 0x12)
        self.write_reg(REG_DIO_MAPPING1, 0x00)
        self.write_reg(REG_LNA, 0x23)         # LNA max gain, boost on
        self._set_tx_power(self.tx_power_dbm)
        self.write_reg(REG_IRQ_FLAGS, 0xFF)
        self._set_mode(MODE_STDBY, settle=0.01, attempts=5)

        self._opened = True
        if receive_mode:
            self.start_receive()
        return self

    def close(self):
        if self.spi is not None:
            try:
                self.write_reg(REG_OP_MODE, self._op_mode_base | MODE_SLEEP)
            except Exception:
                pass
            try:
                self.spi.close()
            finally:
                self.spi = None

        if GPIO is not None:
            try:
                GPIO.cleanup([self.rst_pin, self.dio0_pin])
            except Exception:
                pass

        self._opened = False

    def start_receive(self):
        if not self._opened:
            self.open(receive_mode=True)
            return
        self.write_reg(REG_IRQ_FLAGS, 0xFF)
        self.write_reg(REG_FIFO_ADDR_PTR, 0x00)
        self._set_mode(MODE_RX_CONT, settle=0.01, attempts=5)

    def standby(self):
        if not self._opened:
            self.open()
        self._set_mode(MODE_STDBY, settle=0.01, attempts=5)

    def send_bytes(self, payload, timeout=3.0):
        if not payload:
            raise ValueError("Cannot send an empty LoRa payload.")
        if len(payload) > 255:
            raise ValueError(f"LoRa payload too large ({len(payload)} bytes; max is 255).")

        if not self._opened:
            self.open()

        tx_base = self.read_reg(REG_FIFO_TX_BASE)
        self.standby()
        time.sleep(0.01)
        self.write_reg(REG_IRQ_FLAGS, 0xFF)
        self.write_reg(REG_DIO_MAPPING1, 0x00)
        self.write_reg(REG_FIFO_ADDR_PTR, tx_base)
        self.write_bytes(REG_FIFO, payload)
        self.write_reg(REG_PAYLOAD_LENGTH, len(payload))
        op_mode_after_tx = self._set_mode(MODE_TX, settle=0.005, attempts=5)

        deadline = time.time() + float(timeout)
        irq_last = self.read_reg(REG_IRQ_FLAGS)
        op_mode_last = op_mode_after_tx
        dio0_last = self._read_dio0()
        while time.time() < deadline:
            irq = self.read_reg(REG_IRQ_FLAGS)
            irq_last = irq
            op_mode_last = self.read_reg(REG_OP_MODE)
            dio0_last = self._read_dio0()
            if irq & IRQ_TX_DONE:
                self.write_reg(REG_IRQ_FLAGS, 0xFF)
                self.standby()
                return True
            time.sleep(0.005)

        self.standby()
        irq_flags = irq_last
        op_mode = op_mode_last
        version = self.read_reg(REG_VERSION)
        hop = self.read_reg(REG_HOP_CHANNEL)
        modem1 = self.read_reg(REG_MODEM_CFG1)
        modem2 = self.read_reg(REG_MODEM_CFG2)
        modem3 = self.read_reg(REG_MODEM_CFG3)
        pa_cfg = self.read_reg(REG_PA_CONFIG)
        payload_len = self.read_reg(REG_PAYLOAD_LENGTH)
        raise TimeoutError(
            "Timed out waiting for SX1278 TX_DONE "
            f"(IRQ=0x{irq_flags:02X}, OPMODE=0x{op_mode:02X}, OPMODE_TX=0x{op_mode_after_tx:02X}, VER=0x{version:02X}, "
            f"MODE_BASE=0x{self._op_mode_base:02X}, HOP=0x{hop:02X}, "
            f"MODEM=0x{modem1:02X}/0x{modem2:02X}/0x{modem3:02X}, PA=0x{pa_cfg:02X}, "
            f"TX_BASE=0x{tx_base:02X}, LEN={payload_len}, DIO0={dio0_last}, PWR={self.tx_power_dbm}dBm). "
            "Check Pi SPI0 CE0/NSS, GPIO22 reset, GPIO4 DIO0, 3.3V power, and antenna/module wiring."
        )

    def send_text(self, text, timeout=3.0):
        return self.send_bytes(text.encode("utf-8"), timeout=timeout)

    def send_hex(self, hex_text, timeout=3.0):
        payload = bytes.fromhex(str(hex_text).strip())
        return self.send_bytes(payload, timeout=timeout)

    def send_json(self, payload, timeout=3.0):
        message = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        return self.send_text(message, timeout=timeout)

    def receive(self):
        if not self._opened:
            self.open(receive_mode=True)

        irq = self.read_reg(REG_IRQ_FLAGS)
        if irq & IRQ_RX_TIMEOUT:
            self.write_reg(REG_IRQ_FLAGS, 0xFF)
            return None, None, None
        if not (irq & IRQ_RX_DONE):
            return None, None, None

        if irq & IRQ_PAYLOAD_CRC_ERROR:
            self.write_reg(REG_IRQ_FLAGS, 0xFF)
            return None, None, None

        self.write_reg(REG_IRQ_FLAGS, 0xFF)
        length = self.read_reg(REG_RX_NB_BYTES)
        self.write_reg(REG_FIFO_ADDR_PTR, self.read_reg(REG_FIFO_RX_CURR))
        packet = self.read_bytes(REG_FIFO, length)
        rssi = self.read_reg(REG_PKT_RSSI) - 164
        snr_raw = self.read_reg(REG_PKT_SNR)
        if snr_raw > 127:
            snr_raw -= 256
        snr = snr_raw / 4.0
        return packet, rssi, snr


def _build_arg_parser():
    parser = argparse.ArgumentParser(description="SX1278 LoRa helper for Raspberry Pi")
    parser.add_argument("mode", choices=("rx", "tx"), help="receive or transmit")
    parser.add_argument("--freq", type=float, default=433.0, help="LoRa frequency in MHz")
    parser.add_argument("--text", default="", help="text payload for tx mode")
    parser.add_argument("--hex", dest="hex_text", default="", help="hex payload for tx mode")
    parser.add_argument("--json", dest="json_text", default="", help="JSON payload for tx mode")
    parser.add_argument("--interval", type=float, default=0.05, help="poll interval in seconds for rx mode")
    parser.add_argument("--rst-pin", type=int, default=DEFAULT_RST_PIN, help="BCM reset pin")
    parser.add_argument("--dio0-pin", type=int, default=DEFAULT_DIO0_PIN, help="BCM DIO0 pin")
    parser.add_argument("--spi-hz", type=int, default=500_000, help="SPI clock in Hz")
    parser.add_argument("--tx-power", type=int, default=2, help="Transmit power in dBm (2-17)")
    parser.add_argument("--timeout", type=float, default=3.0, help="TX timeout in seconds")
    return parser


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()

    radio = SX1278LoRa(
        frequency_hz=int(args.freq * 1_000_000),
        rst_pin=args.rst_pin,
        dio0_pin=args.dio0_pin,
        spi_speed_hz=args.spi_hz,
        tx_power_dbm=args.tx_power,
    )

    if args.mode == "tx":
        with radio:
            if args.json_text:
                payload = json.loads(args.json_text)
                radio.send_json(payload, timeout=args.timeout)
                print("Sent JSON:", json.dumps(payload, ensure_ascii=True))
            elif args.hex_text:
                radio.send_hex(args.hex_text, timeout=args.timeout)
                print("Sent HEX:", args.hex_text.lower())
            else:
                text = args.text or "hello"
                radio.send_text(text, timeout=args.timeout)
                print("Sent text:", text)
        return

    radio.open(receive_mode=True)
    print(f"Listening on {args.freq:.3f} MHz at SPI {args.spi_hz} Hz...")
    try:
        while True:
            packet, rssi, snr = radio.receive()
            if packet:
                try:
                    message = packet.decode("utf-8")
                except UnicodeDecodeError:
                    message = packet.hex()
                print(f"Received: {message}")
                print(f"RSSI    : {rssi} dBm")
                print(f"SNR     : {snr:.2f} dB")
                print(f"Length  : {len(packet)} bytes\n")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        radio.close()


if __name__ == "__main__":
    main()
