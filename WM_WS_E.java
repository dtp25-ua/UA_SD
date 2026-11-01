import java.util.Locale;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;

public class WM_WS_E {

    // constante: tiempo entre reintentos de conexion al monitor
    private static final int ESPERA_REINTENTO_MS = 3000;

    // parametros de entrada por linea de comandos
    private String kafkaHost;
    private String kafkaPuerto;
    private String monitorHost;
    private int monitorPuerto;
    private double caudalMin;

    // estado leido y escrito desde distintos hilos, por eso se declaran como volatile
    private volatile boolean averia = false; 
    private volatile boolean bloqueada = false;
    private volatile boolean monitorConectado = false;
    private volatile String idEstacion = null;

    // variables para el estado del riego
    private boolean regando = false;
    private double volumenAcumulado = 0.0;
    private int segundosRiego = 0;
    private int duracionMax = 0;
    private String idOperario = null;
    private Thread hiloRiego = null;
    private String resumenPendiente = null;

    public WM_WS_E(String p_kafkaHost, String p_kafkaPuerto, String p_monitorHost, int p_monitorPuerto, double p_caudal)
    {
        this.kafkaHost = p_kafkaHost;
        this.kafkaPuerto = p_kafkaPuerto;
        this.monitorHost = p_monitorHost;
        this.monitorPuerto = p_monitorPuerto;
        this.caudalMin = p_caudal;
    }

    // formatea un valor decimal
    private static String formato(double p_valor)
    {
        // locale.us (regional usa) para que el decimal sea siempre . y no ,
        return String.format(Locale.US, "%.2f", p_valor);
    }

    // punto por el que salen todos los mensajes hacia la central
    // por implementar: sustituir por productor Kafka 
    private void publicarEvento(String p_trama)
    {
        System.out.println("[Kafka pendiente] " + p_trama);
    }

    // para que una estacion comience un riego
    // devuelve null si se inicia / el motivo del fallo si lo hay
    public synchronized String iniciarRiego(String p_idOperario, int p_duracionMax)
    {
        // posibles errores al iniciar un riego
        if (!monitorConectado) return "WS averiada: sin conexión con el Monitor";
        if (averia) return "WS averiada";
        if (bloqueada) return "WS fuera de servicio";
        if (regando) return "Ya hay un riego en curso";
        if (p_duracionMax <= 0) return "Duración de riego no válida (<=0)";

        regando = true;
        volumenAcumulado = 0.0;
        segundosRiego = 0;
        idOperario = p_idOperario;
        duracionMax = p_duracionMax;

        // creamos un nuevo hilo y lo iniciamos
        hiloRiego = new Thread(() -> bucleRiego());
        hiloRiego.start();

        System.out.println("[RIEGO] Iniciado (operario " + p_idOperario + ", maximo " + p_duracionMax + "s)");
        return null;
    }

    // simula el caudalímetro, cada segundo suma caudal/60 litros
    private void bucleRiego()
    {
        try
        {
            for(;;)
            {
                // detenemos el riego por 1s para enviar información a la central cada segundo
                Thread.sleep(1000);
                // para que dos hilos no puedan acceder a la vez
                synchronized (this)
                {
                    // salimos si el riego ha terminado o este hilo ya no es el actual
                    if(!regando || Thread.currentThread() != hiloRiego)
                        return;

                    segundosRiego++;
                    volumenAcumulado += caudalMin / 60.0;

                    System.out.println("[RIEGO] t=" + segundosRiego + "s caudal=" + formato(caudalMin)
						+ " L/min volumen=" + formato(volumenAcumulado) + " L");

                    publicarEvento("RIEGO_DATOS#" + idEstacion + "#" + formato(caudalMin) + "#"
						+ formato(volumenAcumulado) + "#" + idOperario);

                    if (segundosRiego >= duracionMax)
                    {
                        finalizarRiego("TIEMPO_LIMITE");
                        return;
                    }
                }

            }
        } 
        catch (InterruptedException e)
        {
            // el riego se ha finalizado desde fuera, así que simplemente salimos
        } 
    }

    // usamos 'synchronized' para que solo un hilo pueda utilizar el método a la vez
    // termina el proceso de riego y genera el resumen de este
    public synchronized void finalizarRiego(String p_motivo)
    {
        // si no hay riego en curso, no hacemos nada
        if (!regando)
            return;

        regando = false;
        hiloRiego.interrupt();

        // generamos la trama del resumen y mostramos por pantalla
        String resumen = "RIEGO_FIN#" + idEstacion + "#" + formato(volumenAcumulado) + "#"
			+ segundosRiego + "#" + p_motivo;
        System.out.println("[RIEGO] Finalizado (" + p_motivo + "): " + formato(volumenAcumulado)
			+ " L en " + segundosRiego + " s");

        // comprobamos si la estacion tiene una averia o se ha desconectado de su monitor
        if (averia || !monitorConectado)
        {
            // se guarda para no perder lo regado
            resumenPendiente = resumen;
            System.out.println("[RIEGO] Resumen guardado: se enviará cuando se recupere la WS");
        }
        else{
            publicarEvento(resumen);
        }
    }

    // para enviar un resumen que haya quedado pendiente 
    private synchronized void enviarResumenPendiente()
    {
        if (resumenPendiente != null && !averia && monitorConectado)
        {
            publicarEvento(resumenPendiente);
            resumenPendiente = null;
        }
    }

    // simula una averia, que finaliza el riego actual
    public void simularAveria()
    {
        averia = true;
        finalizarRiego("AVERIA");
        System.out.println("[WS] AVERIA simulada: el monitor recibirá KO en el siguiente chequeo");
    }

    // arregla la averia y envia el resumen del riego
    public void resolverAveria()
    {
        averia = false;
        System.out.println("[WS] Averia resuelta");
        enviarResumenPendiente();
    }

    // metodo para que la Central pueda bloquear la WS finalizando el riego
    public void bloquear()
    {
        bloqueada = true;
        finalizarRiego("BLOQUEO");
        System.out.println("[WS] Fuera de servicio por orden de Central");
    }

    // vuelve a activar la WS
    public void activar()
    {
        bloqueada = false;
        System.out.println("[WS] Activada de nuevo");
    }

    // devuelve el estado actual 
    private synchronized String estadoTexto()
    {
        if (!monitorConectado) return "AVERIADA (sin conexión con el Monitor)";
        if (averia) return "AVERIADA (KO simulado)";
        if (bloqueada) return "FUERA DE SERVICIO";
        if (regando) return "REGANDO";
        return "DISPONIBLE";
    }

    // responde a las peticiones del Monitor
    private String responderMonitor(String p_peticion)
    {
        String[] campos = p_peticion.split("#");

        if (campos[0].equals("ASIGNAR") && campos.length == 2)
        {
            idEstacion = campos[1];
            System.out.println("[WS] ID asignado por el monitor: " + idEstacion);
            return "ASIGNAR#OK";
        }
        // comprobar si hay una averia o no
        if (campos[0].equals("SALUD"))
        {
            return averia ? "SALUD#KO" : "SALUD#OK";
        }
        return "ERROR#Peticion no válida";
    }

    // pérdida de conexión con el monitor
    private void monitorPerdido()
    {
        monitorConectado = false;
        finalizarRiego("MONITOR_CAIDO");
        System.out.println("[WS] AVERIADA: conexion con el monitor perdida. No se aceptan riegos");
    }

    // mantiene la conexion con Monitor y reintenta si se pierde
    private void atenderMonitor()
    {
        boolean estuvoConectado = false;

        for(;;)
        {
            Socket sk = null;
            try
            {
                // abrimos la conexion con el monitor
                sk = new Socket(monitorHost, monitorPuerto);
			    DataInputStream entrada = new DataInputStream(sk.getInputStream());
			    DataOutputStream salida = new DataOutputStream(sk.getOutputStream());

                monitorConectado = true;
                estuvoConectado = true;
                System.out.println("[WS] Conectado con el monitor en " + monitorHost + ":" + monitorPuerto);
                enviarResumenPendiente();

                for(;;)
                {
                    // esperamos a recibir el mensaje que Monitor manda cada segundo
                    String peticion = entrada.readUTF();
                    salida.writeUTF(responderMonitor(peticion));
                    salida.flush();
                }
            }
            catch (EOFException e)
            {
                // cierre limpio de conexion
                System.out.println("[WS] El Monitor ha cerrado la conexión");
            }
            catch (IOException e)
            {
                if (estuvoConectado)
                    System.out.println("[WS] Error de comunicación con el Monitor: " + e.toString());
                else 
                    System.out.println("[WS] No se puede conectar con el Monitor, reintentando...");
            }
            finally{
                if (estuvoConectado)
                {
                    // avisamos de que se ha desconectado el Monitor
                    monitorPerdido();
                    estuvoConectado = false;
                }
                if (sk != null)
                {
                    // cerramos el socket
                    try { sk.close(); } catch (IOException e) {}
                }
            }

            // esperamos después del cierre de la conexion para evitar saturacion
            try { Thread.sleep(ESPERA_REINTENTO_MS); }
            catch (InterruptedException e) {return; } 
        }
    }

    // menu principal de la WS, para que el usuario pueda iniciar y finalizar riegos manualmente
    private void menu(String idEstacion)
    {
        InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader (isr);
        try
        {
            for(;;)
            {
                // mostramos el manú principal
                System.out.println("\n--- Menú de WM_WS_E | Estado: " + estadoTexto() + " ---");
				System.out.println("[1] Iniciar riego manual");
				System.out.println("[2] Finalizar riego");
				System.out.println("[3] Simular averia (KO)");
				System.out.println("[4] Resolver averia");
				System.out.println("[5] Salir");

                String linea = br.readLine();
                if (linea == null)
                    break;

                // trim() para eliminar espacios
                switch(linea.trim())
                {
                    case "1":
                        // pedimos la duracion del riego
                        System.out.println("Duración máxima del riego (en segundos): ");
                        String texto = br.readLine();
                        if (texto == null)
                            return;
                        try
                        {
                            String motivo = iniciarRiego("MANUAL", Integer.parseInt(texto));
                            // si no devuelve null, mostramos el motivo del error
                            if (motivo != null)
                                System.out.println("[WS] Riego rechazado: " + motivo);
                        }
                        // posible error en Integer.parseInt()
                        catch (NumberFormatException e)
                        {
                            System.out.println("Valor no válido");
                        }
                        break;
                    case "2":
                        finalizarRiego("MANUAL");
                        break;
                    case "3":
                        simularAveria();
                        break;
                    case "4":
                        resolverAveria();
                        break;
                    case "5":
                        finalizarRiego("PARADA_ENGINE");
                        System.exit(0);
                        break;
                    default:
                        System.out.println("Opción no válida");
                }
            }
        }
        catch (IOException e)
        {
            System.out.println("Error en la entrada de datos: " + e.toString());
        }
    }

    // arranca la WS, lanza el hilo de monitor y el menu principal
    public void arrancar()
    {
        System.out.println("WM_WS_E Arrancado. Kafka configurado en " + kafkaHost + ":" + kafkaPuerto
            + "(pendiente de integrar). Caudal simulado: " + formato(caudalMin) + " L/min.");
        
        // lanzamos un hilo para atender al monitor
        Thread hiloMonitor = new Thread(() -> atenderMonitor());
        hiloMonitor.setDaemon(true); // para que se cierre al cerrar el main
        hiloMonitor.start();

        // lanzamos el menu principal
        menu(idEstacion);
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Debe indicar la IP y puerto de Kafka y la IP y puerto del Monitor");
            System.out.println("$./WM_WS_E kafka_host kafka_puerto monitor_host monitor_puerto [caudal_min]");
            System.exit (1);
        }

        double caudal = 12.0; // valor por defecto
        try
        {
            // guardamos el caudal minimo si se ha indicado por linea de comandos
            if (args.length > 4)
            {
                caudal = Double.parseDouble(args[4]);
                if (caudal <= 0)
                {
                    System.out.println("El caudal debe ser un valor positivo");
                    System.exit(1);
                }
            }
            WM_WS_E engine = new WM_WS_E(args[0], args[1], args[2], Integer.parseInt(args[3]), caudal);
            engine.arrancar();
        }
        catch (NumberFormatException e)
        {
            System.out.println("Error: puerto o caudal no numéricos: " + e.toString());
        }
    }
}
