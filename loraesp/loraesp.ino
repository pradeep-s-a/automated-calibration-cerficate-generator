#include <SPI.h>
#include <LoRa.h>

// Choose the pin profile that matches your hardware first.
//
// Profile A: ESP32 DevKit + external SX1278 / RA-02 module
#define LORA_SCK 18
#define LORA_MISO 19
#define LORA_MOSI 23
#define LORA_SS 5
#define LORA_RST 14
#define LORA_DIO0 2
//
// Profile B: many Heltec / TTGO-style ESP32 LoRa boards
// #define LORA_SCK 5
// #define LORA_MISO 19
// #define LORA_MOSI 27
// #define LORA_SS 18
// #define LORA_RST 14
// #define LORA_DIO0 26

static const long LORA_FREQ_HZ = 433000000L;
static const long LORA_SPI_HZ = 1000000L;
static const unsigned long ACK_DELAY_MS = 200;

static const long SERIAL_BAUD = 115200;
static const unsigned long HEARTBEAT_MS = 2000;
static unsigned long lastHeartbeatMs = 0;
static unsigned long packetCount = 0;

static void printWiringHelp() {
  Serial.println("Expected wiring:");
  Serial.print("  SCK  -> GPIO ");
  Serial.println(LORA_SCK);
  Serial.print("  MISO -> GPIO ");
  Serial.println(LORA_MISO);
  Serial.print("  MOSI -> GPIO ");
  Serial.println(LORA_MOSI);
  Serial.print("  NSS  -> GPIO ");
  Serial.println(LORA_SS);
  Serial.print("  RST  -> GPIO ");
  Serial.println(LORA_RST);
  Serial.print("  DIO0 -> GPIO ");
  Serial.println(LORA_DIO0);
  Serial.println("  VCC  -> 3.3V only");
  Serial.println("  GND  -> GND");
  Serial.println("If you have a Heltec/TTGO LoRa board, switch to Profile B above.");
}

static void printJsonEscaped(const String &value) {
  for (size_t i = 0; i < value.length(); ++i) {
    char ch = value[i];
    switch (ch) {
      case '\\':
        Serial.print("\\\\");
        break;
      case '"':
        Serial.print("\\\"");
        break;
      case '\n':
        Serial.print("\\n");
        break;
      case '\r':
        Serial.print("\\r");
        break;
      case '\t':
        Serial.print("\\t");
        break;
      default:
        if (static_cast<uint8_t>(ch) < 0x20) {
          char escaped[7];
          snprintf(escaped, sizeof(escaped), "\\u%04X", static_cast<uint8_t>(ch));
          Serial.print(escaped);
        } else {
          Serial.print(ch);
        }
        break;
    }
  }
}

static bool isPrintablePayload(const String &value) {
  for (size_t i = 0; i < value.length(); ++i) {
    char ch = value[i];
    if (ch == '\r' || ch == '\n' || ch == '\t') {
      continue;
    }
    if (static_cast<uint8_t>(ch) < 32 || static_cast<uint8_t>(ch) > 126) {
      return false;
    }
  }
  return true;
}

static String toHexString(const uint8_t *data, size_t len) {
  String out = "";
  out.reserve(len * 3);
  for (size_t i = 0; i < len; ++i) {
    if (i) {
      out += ' ';
    }
    if (data[i] < 0x10) {
      out += '0';
    }
    out += String(data[i], HEX);
  }
  out.toUpperCase();
  return out;
}

static void printHeartbeat() {
  Serial.print("WAIT packets=");
  Serial.print(packetCount);
  Serial.println(" listening...");
}

static bool extractJsonStringField(const String &json, const char *key, String &value) {
  String needle = "\"";
  needle += key;
  needle += "\":\"";

  int start = json.indexOf(needle);
  if (start < 0) {
    return false;
  }
  start += needle.length();

  int end = json.indexOf('"', start);
  if (end < 0) {
    return false;
  }

  value = json.substring(start, end);
  return true;
}

static bool extractJsonLongField(const String &json, const char *key, long &value) {
  String needle = "\"";
  needle += key;
  needle += "\":";

  int start = json.indexOf(needle);
  if (start < 0) {
    return false;
  }
  start += needle.length();

  int end = start;
  if (end < static_cast<int>(json.length()) && json[end] == '-') {
    end++;
  }
  while (end < static_cast<int>(json.length()) && isDigit(json[end])) {
    end++;
  }
  if (end <= start) {
    return false;
  }

  value = json.substring(start, end).toInt();
  return true;
}

static bool extractAckInfo(const String &message, String &sessionId, long &packetKey) {
  if (!message.startsWith("{")) {
    return false;
  }
  if (!extractJsonStringField(message, "id", sessionId)) {
    return false;
  }
  if (!extractJsonLongField(message, "k", packetKey)) {
    return false;
  }
  return true;
}

static void sendAck(const String &sessionId, long packetKey) {
  String ack = "{\"t\":\"ack\",\"id\":\"";
  ack += sessionId;
  ack += "\",\"k\":";
  ack += String(packetKey);
  ack += "}";

  delay(ACK_DELAY_MS);
  LoRa.idle();
  LoRa.beginPacket();
  LoRa.print(ack);
  int ok = LoRa.endPacket();
  LoRa.receive();

  Serial.print("ACK id=");
  Serial.print(sessionId);
  Serial.print(" k=");
  Serial.print(packetKey);
  Serial.print(" sent=");
  Serial.println(ok);
}

void setup() {
  Serial.begin(SERIAL_BAUD);
  delay(400);

  Serial.println();
  Serial.println("ESP32 LoRa Receiver");
  Serial.print("Serial baud: ");
  Serial.println(SERIAL_BAUD);
  Serial.print("Pins SS=");
  Serial.print(LORA_SS);
  Serial.print(" RST=");
  Serial.print(LORA_RST);
  Serial.print(" DIO0=");
  Serial.println(LORA_DIO0);
  Serial.print("SPI  SCK=");
  Serial.print(LORA_SCK);
  Serial.print(" MISO=");
  Serial.print(LORA_MISO);
  Serial.print(" MOSI=");
  Serial.println(LORA_MOSI);

  SPI.begin(LORA_SCK, LORA_MISO, LORA_MOSI, LORA_SS);
  LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
  LoRa.setSPIFrequency(LORA_SPI_HZ);

  if (!LoRa.begin(LORA_FREQ_HZ)) {
    Serial.println("LoRa init failed. ESP32 could not talk to the SX127x over SPI.");
    printWiringHelp();
    while (true) {
      delay(1000);
      Serial.println("LoRa init failed. Recheck power, SPI pins, NSS, and RST.");
    }
  }

  // Match the Raspberry Pi SX1278 settings in lorapi.py.
  LoRa.setSpreadingFactor(11);
  LoRa.setSignalBandwidth(125E3);
  LoRa.setCodingRate4(5);
  LoRa.setSyncWord(0x12);
  LoRa.enableCrc();

  Serial.println("Listening on 433 MHz...");
  Serial.println("Config: SF11 BW125k CR4/5 CRC on SyncWord 0x12");
  Serial.println("USB serial output format ready");
  printHeartbeat();
}

void loop() {
  int packetSize = LoRa.parsePacket();
  if (!packetSize) {
    if (millis() - lastHeartbeatMs >= HEARTBEAT_MS) {
      lastHeartbeatMs = millis();
      printHeartbeat();
    }
    delay(10);
    return;
  }

  packetCount++;

  uint8_t buffer[256];
  size_t index = 0;
  while (LoRa.available() && index < sizeof(buffer)) {
    buffer[index++] = static_cast<uint8_t>(LoRa.read());
  }

  String message = "";
  message.reserve(index);
  for (size_t i = 0; i < index; ++i) {
    message += static_cast<char>(buffer[i]);
  }

  String hexPayload = toHexString(buffer, index);
  bool printable = isPrintablePayload(message);
  String ackSessionId = "";
  long ackPacketKey = -1;
  bool shouldAck = printable && extractAckInfo(message, ackSessionId, ackPacketKey);

  Serial.print("RX count=");
  Serial.print(packetCount);
  Serial.print(" len=");
  Serial.print(packetSize);
  Serial.print(" RSSI=");
  Serial.print(LoRa.packetRssi());
  Serial.print(" SNR=");
  Serial.println(LoRa.packetSnr(), 1);

  if (printable) {
    Serial.print("RX text: ");
    Serial.println(message);
  } else {
    Serial.println("RX text: <non-printable>");
  }

  Serial.print("RX hex : ");
  Serial.println(hexPayload);

  Serial.print("LORA_JSON {\"payload\":\"");
  if (printable) {
    printJsonEscaped(message);
  } else {
    printJsonEscaped(hexPayload);
  }
  Serial.print("\",\"rssi\":");
  Serial.print(LoRa.packetRssi());
  Serial.print(",\"snr\":");
  Serial.print(LoRa.packetSnr(), 1);
  Serial.print(",\"len\":");
  Serial.print(packetSize);
  Serial.println("}");

  if (shouldAck) {
    sendAck(ackSessionId, ackPacketKey);
  }
}
