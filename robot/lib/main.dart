// ============================================================================
//  TLACUACHE - Control de robot de sumo (ESP32 vía UDP)
//  Archivo: lib/main.dart
//
//  Estructura del archivo:
//    1. Configuración y comandos      (constantes en un solo lugar)
//    2. Modelo de telemetría          (parseo de la trama "T,...")
//    3. UdpService                    (socket + throttling)
//    4. VoiceCommandService           (speech_to_text + mapeo de palabras)
//    5. RobotController              (estado de la app, ChangeNotifier)
//    6. UI                            (widgets pequeños y reutilizables)
// ============================================================================

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:speech_to_text/speech_to_text.dart';
import 'package:udp/udp.dart';

// ============================================================================
// 1. CONFIGURACIÓN Y COMANDOS
// ============================================================================

/// Parámetros de red y control. Cambia aquí, no en el resto del código.
class RobotConfig {
  RobotConfig._();

  static const String robotIp = '192.168.4.1';
  static const int port = 4210;

  static const int minPower = -255;
  static const int maxPower = 255;

  /// Intervalo mínimo entre paquetes de los sliders (~25 paquetes/s).
  static const Duration sendInterval = Duration(milliseconds: 40);

  /// Si no llega telemetría en este tiempo, se considera "Sin Conexión".
  static const Duration linkTimeout = Duration(seconds: 2);

  /// true: al soltar un slider vuelve a 0 y el robot se detiene.
  static const bool slidersReturnToZero = true;

  /// Idioma preferido para el reconocimiento de voz.
  static const String preferredLocale = 'es_MX';
}

/// Comandos que entiende la ESP32.
class Cmd {
  Cmd._();

  static const String nitro = 'N';
  static const String stop = 'S';
  static const String batteryReset = 'BATRESET';

  /// Control por sliders: C,1,<izq>,<der>
  static String drive(int left, int right) => 'C,1,$left,$right';
}

// ============================================================================
// 2. MODELO DE TELEMETRÍA
// ============================================================================

/// Trama esperada: "T,<bat%>,<min_rest>,<fuente>,<modo>,..."
class Telemetry {
  const Telemetry({
    required this.batteryPercent,
    required this.minutesLeft,
    required this.source,
    required this.mode,
  });

  final int batteryPercent;
  final int minutesLeft;
  final String source;
  final String mode;

  /// Ajusta estas palabras clave a lo que envíe tu firmware en <fuente>.
  bool get isXboxControl {
    final s = source.toUpperCase();
    return s.contains('XBOX') || s.contains('MANDO') || s == 'X';
  }

  /// Devuelve null si la trama es inválida (nunca lanza excepciones).
  static Telemetry? tryParse(String raw) {
    final parts = raw.trim().split(',');
    if (parts.length < 5 || parts[0] != 'T') return null;

    final battery = num.tryParse(parts[1])?.round();
    final minutes = num.tryParse(parts[2])?.round();
    if (battery == null || minutes == null) return null;

    return Telemetry(
      batteryPercent: battery.clamp(0, 100),
      minutesLeft: minutes < 0 ? 0 : minutes,
      source: parts[3].trim(),
      mode: parts[4].trim(),
    );
  }
}

// ============================================================================
// 3. SERVICIO UDP (con throttling)
// ============================================================================

class UdpService {
  UDP? _socket;
  StreamSubscription<Datagram?>? _rxSub;

  final Endpoint _robot = Endpoint.unicast(
    InternetAddress(RobotConfig.robotIp),
    port: const Port(RobotConfig.port),
  );

  final StreamController<String> _incoming = StreamController<String>.broadcast();

  // --- Estado del throttling ---
  final Stopwatch _clock = Stopwatch()..start();
  Duration _lastSend = -RobotConfig.sendInterval;
  String? _pending;
  Timer? _trailingTimer;

  /// Mensajes de texto recibidos desde el robot (telemetría).
  Stream<String> get messages => _incoming.stream;

  Future<void> start() async {
    // Se enlaza al puerto 4210 para que las respuestas del robot lleguen aquí.
    // Si está ocupado, cae a un puerto aleatorio (el envío sigue funcionando).
    try {
      _socket = await UDP.bind(Endpoint.any(port: const Port(RobotConfig.port)));
    } catch (_) {
      _socket = await UDP.bind(Endpoint.any());
    }

    _rxSub = _socket!.asStream().listen((datagram) {
      if (datagram == null) return;
      _incoming.add(utf8.decode(datagram.data, allowMalformed: true).trim());
    });
  }

  /// Para sliders: limita la frecuencia, pero SIEMPRE envía el último valor
  /// (si llegan varios cambios seguidos, solo sale el más reciente).
  void sendThrottled(String command) {
    _pending = command;
    final elapsed = _clock.elapsed - _lastSend;

    if (elapsed >= RobotConfig.sendInterval) {
      _flushPending();
    } else {
      _trailingTimer ??= Timer(RobotConfig.sendInterval - elapsed, _flushPending);
    }
  }

  /// Para acciones críticas (STOP, NITRO): sin throttling y cancela lo pendiente.
  /// [repeat] reenvía el paquete para compensar pérdidas UDP (sin delays).
  void sendImmediate(String command, {int repeat = 1}) {
    _trailingTimer?.cancel();
    _trailingTimer = null;
    _pending = null;

    for (var i = 0; i < repeat; i++) {
      _transmit(command);
    }
    _lastSend = _clock.elapsed;
  }

  void _flushPending() {
    _trailingTimer?.cancel();
    _trailingTimer = null;

    final command = _pending;
    if (command == null) return;
    _pending = null;

    _transmit(command);
    _lastSend = _clock.elapsed;
  }

  void _transmit(String command) {
    // Fire-and-forget: no se espera confirmación, UDP no la necesita.
    _socket?.send(utf8.encode(command), _robot);
  }

  void dispose() {
    _trailingTimer?.cancel();
    _rxSub?.cancel();
    _socket?.close();
    _incoming.close();
  }
}

// ============================================================================
// 4. SERVICIO DE VOZ
// ============================================================================

enum VoiceAction { attack, spin, stop, reverse }

class VoiceCommandService {
  final SpeechToText _stt = SpeechToText();
  bool _ready = false;
  String? _localeId;

  Future<bool> init({
    required void Function(String status) onStatus,
    required void Function(String error) onError,
  }) async {
    _ready = await _stt.initialize(
      onStatus: onStatus,
      onError: (e) => onError(e.errorMsg),
    );
    if (_ready) _localeId = await _pickSpanishLocale();
    return _ready;
  }

  Future<void> listen(void Function(String words) onWords) async {
    if (!_ready) return;
    await _stt.listen(
      onResult: (result) => onWords(result.recognizedWords),
      listenFor: const Duration(seconds: 6),
      pauseFor: const Duration(seconds: 2),
      localeId: _localeId,
      listenOptions: SpeechListenOptions(
        partialResults: true, // reacciona sin esperar a que termines de hablar
        cancelOnError: true,
      ),
    );
  }

  Future<void> stop() => _stt.stop();

  Future<String?> _pickSpanishLocale() async {
    final locales = await _stt.locales();
    final ids = locales.map((l) => l.localeId).toList();
    if (ids.contains(RobotConfig.preferredLocale)) return RobotConfig.preferredLocale;
    for (final id in ids) {
      if (id.toLowerCase().startsWith('es')) return id;
    }
    return null; // idioma por defecto del sistema
  }

  /// Traduce lo escuchado a una acción. "Parar" se evalúa PRIMERO por seguridad.
  static VoiceAction? parse(String words) {
    final text = _normalize(words);

    if (_hasAny(text, const ['parar', 'para', 'alto', 'stop', 'frena', 'detente'])) {
      return VoiceAction.stop;
    }
    if (_hasAny(text, const ['atacar', 'ataca', 'ataque', 'nitro'])) {
      return VoiceAction.attack;
    }
    if (_hasAny(text, const ['reversa', 'atras', 'retrocede'])) {
      return VoiceAction.reverse;
    }
    if (_hasAny(text, const ['girar', 'gira', 'giro'])) {
      return VoiceAction.spin;
    }
    return null;
  }

  static bool _hasAny(String text, List<String> keywords) =>
      keywords.any(text.contains);

  /// Minúsculas y sin acentos, para comparar de forma robusta.
  static String _normalize(String s) {
    const from = 'áéíóúüñ';
    const to = 'aeiouun';
    var out = s.toLowerCase();
    for (var i = 0; i < from.length; i++) {
      out = out.replaceAll(from[i], to[i]);
    }
    return out;
  }
}

// ============================================================================
// 5. CONTROLADOR (estado de la app)
// ============================================================================

class RobotController extends ChangeNotifier {
  final UdpService _udp = UdpService();
  final VoiceCommandService _voice = VoiceCommandService();

  StreamSubscription<String>? _telemetrySub;
  Timer? _linkWatchdog;
  DateTime? _lastTelemetryAt;
  bool _voiceReady = false;
  bool _voiceHandled = false;

  // Estado observable por la UI
  int leftPower = 0;
  int rightPower = 0;
  bool isConnected = false;
  Telemetry? telemetry;
  bool isListening = false;
  String voiceHint = 'Toca el micrófono';

  Future<void> start() async {
    await _udp.start();

    _telemetrySub = _udp.messages.listen(_onMessage);

    // Vigila que la telemetría siga llegando para marcar "Sin Conexión".
    _linkWatchdog = Timer.periodic(const Duration(milliseconds: 500), (_) {
      final last = _lastTelemetryAt;
      final alive =
          last != null && DateTime.now().difference(last) < RobotConfig.linkTimeout;
      if (alive != isConnected) {
        isConnected = alive;
        notifyListeners();
      }
    });
  }

  // ---------------------------- Telemetría ----------------------------------

  void _onMessage(String raw) {
    final parsed = Telemetry.tryParse(raw);
    if (parsed == null) return;

    telemetry = parsed;
    isConnected = true;
    _lastTelemetryAt = DateTime.now();
    notifyListeners();
  }

  // ----------------------------- Movimiento ---------------------------------

  void setLeft(int value) {
    leftPower = value;
    _sendDriveThrottled();
  }

  void setRight(int value) {
    rightPower = value;
    _sendDriveThrottled();
  }

  void _sendDriveThrottled() {
    _udp.sendThrottled(Cmd.drive(leftPower, rightPower));
    notifyListeners();
  }

  /// Al soltar un slider: envía el valor final sin esperar al throttling.
  void onSliderReleased() {
    if (RobotConfig.slidersReturnToZero) {
      leftPower = 0;
      rightPower = 0;
    }
    _udp.sendImmediate(Cmd.drive(leftPower, rightPower));
    notifyListeners();
  }

  void nitro() {
    leftPower = RobotConfig.maxPower;
    rightPower = RobotConfig.maxPower;
    _udp.sendImmediate(Cmd.nitro);
    notifyListeners();
  }

  void stop() {
    leftPower = 0;
    rightPower = 0;
    _udp.sendImmediate(Cmd.stop, repeat: 2); // doble envío: el frenado es crítico
    notifyListeners();
  }

  void resetBattery() {
    _udp.sendImmediate(Cmd.batteryReset, repeat: 2);
  }

  // -------------------------------- Voz -------------------------------------

  Future<void> toggleVoice() async {
    if (isListening) {
      await _voice.stop();
      return;
    }

    if (!_voiceReady) {
      _voiceReady = await _voice.init(
        onStatus: _onVoiceStatus,
        onError: (error) {
          isListening = false;
          voiceHint = 'Error de voz: $error';
          notifyListeners();
        },
      );
    }
    if (!_voiceReady) {
      voiceHint = 'Micrófono no disponible';
      notifyListeners();
      return;
    }

    _voiceHandled = false;
    isListening = true;
    voiceHint = 'Escuchando…';
    notifyListeners();
    await _voice.listen(_onVoiceWords);
  }

  void _onVoiceStatus(String status) {
    if (status == 'notListening' || status == 'done') {
      isListening = false;
      notifyListeners();
    }
  }

  void _onVoiceWords(String words) {
    if (_voiceHandled) return; // un solo comando por sesión de escucha

    final action = VoiceCommandService.parse(words);
    if (action == null) {
      voiceHint = '"$words"';
      notifyListeners();
      return;
    }

    _voiceHandled = true;
    _voice.stop();
    _runVoiceAction(action);
  }

  void _runVoiceAction(VoiceAction action) {
    switch (action) {
      case VoiceAction.attack:
        voiceHint = 'Voz: ATACAR';
        nitro();
      case VoiceAction.stop:
        voiceHint = 'Voz: PARAR';
        stop();
      case VoiceAction.spin:
        voiceHint = 'Voz: GIRAR';
        _driveImmediate(RobotConfig.maxPower, RobotConfig.minPower);
      case VoiceAction.reverse:
        voiceHint = 'Voz: REVERSA';
        _driveImmediate(RobotConfig.minPower, RobotConfig.minPower);
    }
    notifyListeners();
  }

  void _driveImmediate(int left, int right) {
    leftPower = left;
    rightPower = right;
    _udp.sendImmediate(Cmd.drive(left, right));
  }

  @override
  void dispose() {
    _linkWatchdog?.cancel();
    _telemetrySub?.cancel();
    _udp.dispose();
    super.dispose();
  }
}

// ============================================================================
// 6. UI
// ============================================================================

class AppColors {
  AppColors._();

  static const background = Color(0xFF0B0F14);
  static const surface = Color(0xFF151B23);
  static const accent = Color(0xFF00E5FF);
  static const ok = Color(0xFF00E676);
  static const warning = Color(0xFFFFB300);
  static const danger = Color(0xFFE53935);
  static const nitro = Color(0xFFFF6D00);
  static const xbox = Color(0xFF9BE564);
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // Modo horizontal a pantalla completa: más espacio y menos toques accidentales.
  await SystemChrome.setPreferredOrientations(const [
    DeviceOrientation.landscapeLeft,
    DeviceOrientation.landscapeRight,
  ]);
  await SystemChrome.setEnabledSystemUIMode(SystemUiMode.immersiveSticky);

  runApp(const TlacuacheApp());
}

class TlacuacheApp extends StatelessWidget {
  const TlacuacheApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'TLACUACHE',
      debugShowCheckedModeBanner: false,
      theme: ThemeData.dark(useMaterial3: true).copyWith(
        scaffoldBackgroundColor: AppColors.background,
        colorScheme: const ColorScheme.dark(
          primary: AppColors.accent,
          surface: AppColors.surface,
        ),
      ),
      home: const ControlPage(),
    );
  }
}

class ControlPage extends StatefulWidget {
  const ControlPage({super.key});

  @override
  State<ControlPage> createState() => _ControlPageState();
}

class _ControlPageState extends State<ControlPage> {
  final RobotController _robot = RobotController();

  @override
  void initState() {
    super.initState();
    _robot.start();
  }

  @override
  void dispose() {
    _robot.dispose();
    super.dispose();
  }

  Future<void> _confirmBatteryReset() async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        backgroundColor: AppColors.surface,
        title: const Text('Reset de batería'),
        content: const Text(
          '¿Cambiaste las celdas 18650?\nEl contador volverá al 100%.',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx, false),
            child: const Text('Cancelar'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(ctx, true),
            child: const Text('Reiniciar'),
          ),
        ],
      ),
    );
    if (confirmed == true) _robot.resetBattery();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: ListenableBuilder(
          listenable: _robot,
          builder: (context, _) {
            // Si el mando de Xbox tiene el control, se atenúan los sliders
            // (siguen activos: la app puede tomar el mando en una emergencia).
            final xboxHasControl = _robot.telemetry?.isXboxControl ?? false;

            return Padding(
              padding: const EdgeInsets.all(8),
              child: Column(
                children: [
                  _TopBar(robot: _robot),
                  const SizedBox(height: 8),
                  Expanded(
                    child: Row(
                      children: [
                        SizedBox(
                          width: 110,
                          child: _PowerSlider(
                            label: 'IZQ',
                            value: _robot.leftPower,
                            dimmed: xboxHasControl,
                            onChanged: _robot.setLeft,
                            onReleased: _robot.onSliderReleased,
                          ),
                        ),
                        Expanded(
                          child: _CenterPanel(
                            robot: _robot,
                            onBatteryReset: _confirmBatteryReset,
                          ),
                        ),
                        SizedBox(
                          width: 110,
                          child: _PowerSlider(
                            label: 'DER',
                            value: _robot.rightPower,
                            dimmed: xboxHasControl,
                            onChanged: _robot.setRight,
                            onReleased: _robot.onSliderReleased,
                          ),
                        ),
                      ],
                    ),
                  ),
                ],
              ),
            );
          },
        ),
      ),
    );
  }
}

// ----------------------------- Barra superior -------------------------------

class _TopBar extends StatelessWidget {
  const _TopBar({required this.robot});

  final RobotController robot;

  @override
  Widget build(BuildContext context) {
    final telemetry = robot.telemetry;

    final String sourceLabel;
    final Color sourceColor;
    final IconData sourceIcon;
    if (!robot.isConnected || telemetry == null) {
      sourceLabel = 'CONTROL: --';
      sourceColor = Colors.grey;
      sourceIcon = Icons.help_outline;
    } else if (telemetry.isXboxControl) {
      sourceLabel = 'CONTROL: MANDO XBOX';
      sourceColor = AppColors.xbox;
      sourceIcon = Icons.sports_esports;
    } else {
      sourceLabel = 'CONTROL: APP';
      sourceColor = AppColors.accent;
      sourceIcon = Icons.smartphone;
    }

    return Row(
      children: [
        const Text(
          'TLACUACHE',
          style: TextStyle(
            fontSize: 20,
            fontWeight: FontWeight.w900,
            letterSpacing: 3,
            color: AppColors.accent,
          ),
        ),
        const Spacer(),
        if (robot.isConnected && telemetry != null) ...[
          _StatusChip(
            icon: Icons.settings_suggest,
            label: 'MODO: ${telemetry.mode}',
            color: Colors.white70,
          ),
          const SizedBox(width: 8),
        ],
        _StatusChip(icon: sourceIcon, label: sourceLabel, color: sourceColor),
        const SizedBox(width: 8),
        _StatusChip(
          icon: robot.isConnected ? Icons.wifi : Icons.wifi_off,
          label: robot.isConnected ? 'Wi-Fi OK' : 'Sin Conexión',
          color: robot.isConnected ? AppColors.ok : AppColors.danger,
        ),
      ],
    );
  }
}

class _StatusChip extends StatelessWidget {
  const _StatusChip({
    required this.icon,
    required this.label,
    required this.color,
  });

  final IconData icon;
  final String label;
  final Color color;

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
      decoration: BoxDecoration(
        color: color.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(20),
        border: Border.all(color: color, width: 1.2),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 16, color: color),
          const SizedBox(width: 6),
          Text(
            label,
            style: TextStyle(
              fontSize: 12,
              fontWeight: FontWeight.bold,
              color: color,
            ),
          ),
        ],
      ),
    );
  }
}

// ------------------------------ Panel central -------------------------------

class _CenterPanel extends StatelessWidget {
  const _CenterPanel({required this.robot, required this.onBatteryReset});

  final RobotController robot;
  final VoidCallback onBatteryReset;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 8),
      child: Column(
        children: [
          _BatteryPanel(telemetry: robot.telemetry, connected: robot.isConnected),
          const SizedBox(height: 8),
          Expanded(
            child: Row(
              children: [
                Expanded(
                  child: _ActionButton(
                    label: 'NITRO',
                    icon: Icons.bolt,
                    color: AppColors.nitro,
                    onPressed: robot.nitro,
                  ),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: _ActionButton(
                    label: 'STOP',
                    icon: Icons.stop_circle_outlined,
                    color: AppColors.danger,
                    onPressed: robot.stop,
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(height: 8),
          SizedBox(
            height: 52,
            child: Row(
              children: [
                _MicButton(
                  listening: robot.isListening,
                  onPressed: robot.toggleVoice,
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    robot.voiceHint,
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: const TextStyle(fontSize: 12, color: Colors.white60),
                  ),
                ),
                OutlinedButton.icon(
                  onPressed: onBatteryReset,
                  icon: const Icon(Icons.battery_charging_full, size: 18),
                  label: const Text('Reset Batería'),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

// ------------------------------- Batería ------------------------------------

class _BatteryPanel extends StatelessWidget {
  const _BatteryPanel({required this.telemetry, required this.connected});

  final Telemetry? telemetry;
  final bool connected;

  static Color _colorFor(int percent) {
    if (percent > 50) return AppColors.ok;
    if (percent > 20) return AppColors.warning;
    return AppColors.danger;
  }

  static IconData _iconFor(int percent) {
    if (percent > 85) return Icons.battery_full;
    if (percent > 60) return Icons.battery_6_bar;
    if (percent > 40) return Icons.battery_4_bar;
    if (percent > 20) return Icons.battery_2_bar;
    return Icons.battery_alert;
  }

  @override
  Widget build(BuildContext context) {
    final t = telemetry;
    final hasData = connected && t != null;
    final percent = hasData ? t.batteryPercent : 0;
    final color = hasData ? _colorFor(percent) : Colors.grey;

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
      decoration: BoxDecoration(
        color: AppColors.surface,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: color.withValues(alpha: 0.6)),
      ),
      child: Row(
        children: [
          Icon(hasData ? _iconFor(percent) : Icons.battery_unknown,
              size: 36, color: color),
          const SizedBox(width: 10),
          Text(
            hasData ? '$percent%' : '--%',
            style: TextStyle(fontSize: 30, fontWeight: FontWeight.w900, color: color),
          ),
          const SizedBox(width: 14),
          Expanded(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                ClipRRect(
                  borderRadius: BorderRadius.circular(6),
                  child: LinearProgressIndicator(
                    value: percent / 100,
                    minHeight: 10,
                    color: color,
                    backgroundColor: Colors.white12,
                  ),
                ),
                const SizedBox(height: 4),
                Text(
                  hasData ? 'Restante: ${t.minutesLeft} min' : 'Sin datos del robot',
                  style: const TextStyle(fontSize: 12, color: Colors.white70),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

// ------------------------------- Botones ------------------------------------

/// Botón grande que reacciona al CONTACTO (onTapDown), no al soltar:
/// ahorra decenas de milisegundos, importante en una pelea de sumo.
class _ActionButton extends StatefulWidget {
  const _ActionButton({
    required this.label,
    required this.icon,
    required this.color,
    required this.onPressed,
  });

  final String label;
  final IconData icon;
  final Color color;
  final VoidCallback onPressed;

  @override
  State<_ActionButton> createState() => _ActionButtonState();
}

class _ActionButtonState extends State<_ActionButton> {
  bool _pressed = false;

  void _setPressed(bool value) => setState(() => _pressed = value);

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTapDown: (_) {
        _setPressed(true);
        HapticFeedback.heavyImpact();
        widget.onPressed();
      },
      onTapUp: (_) => _setPressed(false),
      onTapCancel: () => _setPressed(false),
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 80),
        decoration: BoxDecoration(
          color: _pressed ? widget.color : widget.color.withValues(alpha: 0.18),
          borderRadius: BorderRadius.circular(18),
          border: Border.all(color: widget.color, width: 3),
          boxShadow: [
            if (_pressed)
              BoxShadow(
                color: widget.color.withValues(alpha: 0.6),
                blurRadius: 24,
              ),
          ],
        ),
        child: Center(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              Icon(widget.icon,
                  size: 40, color: _pressed ? Colors.black : widget.color),
              Text(
                widget.label,
                style: TextStyle(
                  fontSize: 22,
                  fontWeight: FontWeight.w900,
                  letterSpacing: 2,
                  color: _pressed ? Colors.black : widget.color,
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _MicButton extends StatelessWidget {
  const _MicButton({required this.listening, required this.onPressed});

  final bool listening;
  final VoidCallback onPressed;

  @override
  Widget build(BuildContext context) {
    final color = listening ? AppColors.danger : AppColors.accent;
    return GestureDetector(
      onTapDown: (_) => onPressed(),
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 150),
        width: 52,
        height: 52,
        decoration: BoxDecoration(
          shape: BoxShape.circle,
          color: color.withValues(alpha: listening ? 0.9 : 0.15),
          border: Border.all(color: color, width: 2),
        ),
        child: Icon(
          listening ? Icons.mic : Icons.mic_none,
          color: listening ? Colors.white : color,
        ),
      ),
    );
  }
}

// ----------------------------- Slider vertical ------------------------------

class _PowerSlider extends StatelessWidget {
  const _PowerSlider({
    required this.label,
    required this.value,
    required this.dimmed,
    required this.onChanged,
    required this.onReleased,
  });

  final String label;
  final int value;
  final bool dimmed;
  final ValueChanged<int> onChanged;
  final VoidCallback onReleased;

  @override
  Widget build(BuildContext context) {
    // Verde = avance, rojo = reversa, gris = detenido.
    final color = value > 0
        ? AppColors.ok
        : value < 0
            ? AppColors.danger
            : Colors.white54;

    return Opacity(
      opacity: dimmed ? 0.5 : 1,
      child: Container(
        decoration: BoxDecoration(
          color: AppColors.surface,
          borderRadius: BorderRadius.circular(18),
        ),
        padding: const EdgeInsets.symmetric(vertical: 10),
        child: Column(
          children: [
            Text(label,
                style: const TextStyle(
                    fontWeight: FontWeight.bold, letterSpacing: 2)),
            Expanded(
              // quarterTurns: 3 => el slider horizontal queda vertical,
              // con el mínimo (-255) abajo y el máximo (255) arriba.
              child: RotatedBox(
                quarterTurns: 3,
                child: SliderTheme(
                  data: SliderTheme.of(context).copyWith(
                    trackHeight: 22,
                    activeTrackColor: color,
                    inactiveTrackColor: Colors.white12,
                    thumbColor: color,
                    thumbShape:
                        const RoundSliderThumbShape(enabledThumbRadius: 18),
                    overlayShape: SliderComponentShape.noOverlay,
                    trackShape: const RoundedRectSliderTrackShape(),
                  ),
                  child: Slider(
                    min: RobotConfig.minPower.toDouble(),
                    max: RobotConfig.maxPower.toDouble(),
                    value: value.toDouble(),
                    onChanged: (v) => onChanged(v.round()),
                    onChangeEnd: (_) => onReleased(),
                  ),
                ),
              ),
            ),
            Text(
              '$value',
              style: TextStyle(
                fontSize: 22,
                fontWeight: FontWeight.w900,
                color: color,
              ),
            ),
          ],
        ),
      ),
    );
  }
}