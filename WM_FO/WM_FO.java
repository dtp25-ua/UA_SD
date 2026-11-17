import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class WM_FO {

    // topics: Central, Engine y FO usan exactamente los mismos nombres
	private static final String TOPIC_SOLICITUDES = "wm-solicitudes";   // para solicitudes de riego a Central (FO -> Central)
	private static final String TOPIC_OPERARIOS   = "wm-operarios";     // para enviar respuestas e info al op. (Central -> FO) 
	private static final String TOPIC_ESTADO_WS   = "wm-estado-ws";     // para distribuir el estado de las ws (Central -> todos)

    // tiempos
	private static final int ESPERA_ENTRE_SERVICIOS_MS = 4000;   
	private static final int TIMEOUT_RESPUESTA_MS = 10000;       // espera a que Central autorice o deniegue
	private static final int TIMEOUT_SILENCIO_MS = 5000;         // sin noticias de Central durante un riego
	private static final int DURACION_POR_DEFECTO_S = 10;

    // fases del servicio actual
	private static final String LIBRE = "LIBRE";
	private static final String ESPERANDO_RESPUESTA = "ESPERANDO_RESPUESTA";
	private static final String EN_RIEGO = "EN_RIEGO";

	private String idOperario;
	private MensajeriaKafka kafka;

	// para sincronizar los hilos
	private final Object cerrojo = new Object();
	private String fase = LIBRE;
	private String wsServicio = null;   // id de la estacion
	private long ultimaActividad = 0;   // momento de la ultima comunicacion con Central

    // estado de todas las WS segun Central: id -> {ubicacion, estado}
	private ConcurrentHashMap<String, String[]> estadoWs = new ConcurrentHashMap<String, String[]>();

    public WM_FO(String p_kafkaHost, String p_kafkaPuerto, String p_idOperario)
	{
		this.idOperario = p_idOperario;
		this.kafka = new MensajeriaKafka(p_kafkaHost, p_kafkaPuerto);
	}

	private void dormir(int p_ms)
	{
		try { Thread.sleep(p_ms); }
		catch (InterruptedException e) { Thread.currentThread().interrupt(); }
	}

    // -- PARA MENSAJES QUE LLEGAN DE CENTRAL -- 

    private boolean tramaCompleta(String[] p_campos, int p_minimo, String p_trama)
	{
		if (p_campos.length < p_minimo)
		{
			System.out.println("[FO] Mensaje de Central mal formado: " + p_trama);
			return false;
		}
		return true;
	}

    // procesa las respuestas y notificaciones de Central dirigidas al operario
    // como el topic es compartido solo atendemos las que tienen de clave nuestro id
    private void alRecibirDeCentral(String p_clave, String p_valor)
	{
		if (p_clave == null || !p_clave.equals(idOperario))
			return;

		String[] c = p_valor.split("#");

        // para que dos hilos no lo ejecuten a la vez
		synchronized (cerrojo)
		{
			ultimaActividad = System.currentTimeMillis();

			switch (c[0])
			{
				case "RIEGO_PASO":
					if (tramaCompleta(c, 3, p_valor))
						System.out.println("[FO] " + c[1] + ": " + c[2]);
					break;

				case "RIEGO_AUTORIZADO":
					if (tramaCompleta(c, 2, p_valor))
					{
						System.out.println("[FO] Riego AUTORIZADO en " + c[1]);
						fase = EN_RIEGO;
						wsServicio = c[1];
						cerrojo.notifyAll();    // avisar a los hilos que esperan
					}
					break;

				case "RIEGO_DENEGADO":
					if (tramaCompleta(c, 3, p_valor))
					{
						System.out.println("[FO] Riego DENEGADO en " + c[1] + ": " + c[2]);
						fase = LIBRE;
						wsServicio = null;
						cerrojo.notifyAll();
					}
					break;

				case "RIEGO_ESTADO":
					if (tramaCompleta(c, 4, p_valor))
					{
						System.out.println("[FO] " + c[1] + ": caudal " + c[2] + " L/min, volumen " + c[3] + " L");
						if (fase.equals(LIBRE))
						{
							// nos incorporamos a un riego que empezo antes, por ejemplo, tras reiniciar el FO
							fase = EN_RIEGO;
							wsServicio = c[1];
						}
					}
					break;

				case "RIEGO_RESUMEN":
					if (tramaCompleta(c, 5, p_valor))
					{
						System.out.println("[FO] Riego FINALIZADO en " + c[1] + ": " + c[2] + " L en "
							+ c[3] + " s (motivo: " + c[4] + ")");
						fase = LIBRE;
						wsServicio = null;
						cerrojo.notifyAll();
					}
					break;

				default:
					System.out.println("[FO] Mensaje no reconocido de Central: " + p_valor);
			}
		}
	}

    // actualiza el estado de las WS
	private void alRecibirEstadoWs(String p_clave, String p_valor)
	{
		String[] c = p_valor.split("#");
		if (c.length == 4 && c[0].equals("WS_ESTADO"))
			estadoWs.put(c[1], new String[] { c[2], c[3] });
	}

	// -- PARA SERVICIO DE RIEGO --

    // devuelve el servicio a 'libre'
    private void liberar()
	{
		synchronized (cerrojo)
		{
			fase = LIBRE;
			wsServicio = null;
		}
	}

	// libera el servicio si Central lleva mucho tiempo sin dar noticias
	private void liberarSiCaducado()
	{
		if (fase.equals(EN_RIEGO) && System.currentTimeMillis() - ultimaActividad > TIMEOUT_SILENCIO_MS)
		{
			System.out.println("[FO] Imposible conectar con WM_Central: sin noticias del riego en " + wsServicio);
			fase = LIBRE;
			wsServicio = null;
		}
	}

    private String estadoServicio()
	{
		synchronized (cerrojo)
		{
			if (fase.equals(EN_RIEGO))
				return "EN RIEGO (" + wsServicio + ")";
			if (fase.equals(ESPERANDO_RESPUESTA))
				return "ESPERANDO RESPUESTA";
			return "LIBRE";
		}
	}

    // pide un riego y espera la respuesta de Central. Devuelve true si lo autoriza.
	private boolean solicitarRiego(String p_idWs, int p_duracionS)
	{
		synchronized (cerrojo)
		{
			liberarSiCaducado();
			if (!fase.equals(LIBRE))
			{
				System.out.println("[FO] Ya tienes un servicio en curso: " + estadoServicio());
				return false;
			}
			fase = ESPERANDO_RESPUESTA;
			wsServicio = p_idWs;
			ultimaActividad = System.currentTimeMillis();
		}

		System.out.println("[FO] Solicitando riego en " + p_idWs + " (" + p_duracionS + " s)...");
		boolean enviado = kafka.publicar(TOPIC_SOLICITUDES, p_idWs,
			"SOLICITUD_RIEGO#" + idOperario + "#" + p_idWs + "#" + p_duracionS);
		if (!enviado)
		{
			liberar();
			System.out.println("[FO] Imposible conectar con Kafka: la solicitud no se ha enviado");
			return false;
		}

		synchronized (cerrojo)
		{
			long limite = System.currentTimeMillis() + TIMEOUT_RESPUESTA_MS;
			try
			{
				while (fase.equals(ESPERANDO_RESPUESTA))
				{
					long resto = limite - System.currentTimeMillis();
					if (resto <= 0)
						break;
					cerrojo.wait(resto);    // esperar a que Central responda
				}
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}

            // si aun no ha recibido respuesta de Central
			if (fase.equals(ESPERANDO_RESPUESTA))
			{
				System.out.println("[FO] Imposible conectar con WM_Central: no ha respondido en "
					+ (TIMEOUT_RESPUESTA_MS / 1000) + " s");
				fase = LIBRE;
				wsServicio = null;
				return false;
			}
			return fase.equals(EN_RIEGO);
		}
	}

    // espera a que llegue el resumen del riego o a que Central deje de dar señales
	private void esperarFinRiego()
	{
		synchronized (cerrojo)
		{
			try
			{
				while (fase.equals(EN_RIEGO))
				{
					cerrojo.wait(1000);
					liberarSiCaducado();
				}
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
		}
	}

    // lee un fichero y ejecuta las solicitudes de riego que contenga
	private void ejecutarFichero(String p_ruta)
	{
        // lee por lineas (una linea por servicio)
		List<String> lineas;
		try
		{
			lineas = Files.readAllLines(Paths.get(p_ruta));
		}
		catch (IOException e)
		{
			System.out.println("[FO] No se puede leer el fichero " + p_ruta + ": " + e.toString());
			return;
		}

		// primero validamos todo el fichero
		List<String[]> servicios = new ArrayList<String[]>();
		for (int i = 0; i < lineas.size(); i++)
		{
			String linea = lineas.get(i).trim();    // trim() para eliminar espacios
			if (i == 0 && linea.startsWith("\uFEFF"))
				linea = linea.substring(1).trim();   
			if (linea.isEmpty())
				continue;

			String[] c = linea.split("#");
			try
			{
				String ws = c[0].trim();
				int duracion = (c.length == 2) ? Integer.parseInt(c[1].trim()) : DURACION_POR_DEFECTO_S;
				if (ws.isEmpty() || duracion <= 0 || c.length > 2)
					throw new NumberFormatException();
				servicios.add(new String[] { ws, String.valueOf(duracion) });
			}
			catch (NumberFormatException e)
			{
				System.out.println("[FO] Linea " + (i + 1) + " no valida, se ignora: " + linea);
			}
		}

        // si se ha validado correctamente
		System.out.println("[FO] " + servicios.size() + " servicios en el fichero");
		for (int i = 0; i < servicios.size(); i++)
		{
			String ws = servicios.get(i)[0];
			int duracion = Integer.parseInt(servicios.get(i)[1]);

			System.out.println("[FO] Servicio " + (i + 1) + "/" + servicios.size() + ": " + ws + " (" + duracion + " s)");
			if (solicitarRiego(ws, duracion))
				esperarFinRiego();

			// esperamos 4s antes del siguiente
			if (i < servicios.size() - 1)
			{
				System.out.println("[FO] Esperando " + (ESPERA_ENTRE_SERVICIOS_MS / 1000) + " s...");
				dormir(ESPERA_ENTRE_SERVICIOS_MS);
			}
		}
		System.out.println("[FO] Fichero completado");
	}

    // -- MENU --

    // para introducir una solicitud desde el menú
	private void solicitudManual(BufferedReader p_br) throws IOException
	{
		System.out.print("ID de la WS: ");
		String id = p_br.readLine();
		if (id == null)
			return;
		id = id.trim();
		if (id.isEmpty())
		{
			System.out.println("ID vacio");
			return;
		}

		System.out.print("Duracion del riego en segundos [" + DURACION_POR_DEFECTO_S + "]: ");
		String texto = p_br.readLine();
		if (texto == null)
			return;
		texto = texto.trim();

		int duracion = DURACION_POR_DEFECTO_S;
		if (!texto.isEmpty())
		{
			try
			{
				duracion = Integer.parseInt(texto);
			}
			catch (NumberFormatException e)
			{
				System.out.println("Valor no valido");
				return;
			}
		}
		if (duracion <= 0)
		{
			System.out.println("La duracion debe ser mayor que 0");
			return;
		}

		// la respuesta y el progreso se iran mostrando solos
		solicitarRiego(id, duracion);
	}

    // muestra las estaciones conocidas y sus estados
	private void verEstaciones()
	{
		if (estadoWs.isEmpty())
		{
			System.out.println("[FO] Todavia no hay informacion de las estaciones (Central no ha publicado su estado)");
			return;
		}
		System.out.println(String.format("%-8s %-18s %s", "ID", "UBICACION", "ESTADO"));
		for (Map.Entry<String, String[]> e : new TreeMap<String, String[]>(estadoWs).entrySet())
			System.out.println(String.format("%-8s %-18s %s", e.getKey(), e.getValue()[0], e.getValue()[1]));
	}

    // menu para gestionar las opciones del operario
    private void menu()
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try
		{
			for(;;)
			{
				System.out.println("\n--- Menu de WM_FO | Operario " + idOperario + " | Servicio: " + estadoServicio() + " ---");
				System.out.println("[1] Solicitar riego");
				System.out.println("[2] Ver estaciones (WS)");
				System.out.println("[3] Ejecutar servicios desde fichero");
				System.out.println("[4] Salir");

				String linea = br.readLine();
				if (linea == null)
				{
					System.out.println("Sin entrada de teclado: menu desactivado");
					return;
				}

				switch (linea.trim())
				{
					case "1":
						solicitudManual(br);
						break;
					case "2":
						verEstaciones();
						break;
					case "3":
						System.out.print("Ruta del fichero: ");
						String ruta = br.readLine();
						if (ruta != null && !ruta.trim().isEmpty())
							ejecutarFichero(ruta.trim());
						break;
					case "4":
						// salir no cancela ningun riego, sigue su curso en la WS
						System.exit(0);
						break;
					default:
						System.out.println("Opcion no valida");
				}
			}
		}
		catch (IOException e)
		{
			System.out.println("Error en la entrada de datos: " + e.toString());
		}
	}

    // inicializa Kafka y pone en marcha la aplicación
    public void arrancar(String p_ficheroInicial)
	{
		System.out.println("WM_FO arrancado. Operario " + idOperario);
		kafka.iniciarProductor();

		// un grupo por operario y offsets confirmados. si el FO se cierra durante un riego,
		// al volver recibe lo que Central publico mientras estuvo caido.
		// "earliest" para no perder una respuesta que llegue antes de que el consumidor este listo.
		kafka.suscribir(TOPIC_OPERARIOS, "wm-fo-" + idOperario, "earliest", true,
			(k, v) -> alRecibirDeCentral(k, v));

		// el estado de las WS solo interesa el actual, creamos un grupo nuevo en cada arranque
		kafka.suscribir(TOPIC_ESTADO_WS, "wm-fo-ws-" + idOperario + "-" + UUID.randomUUID(), "latest", false,
			(k, v) -> alRecibirEstadoWs(k, v));

		if (p_ficheroInicial != null)
			ejecutarFichero(p_ficheroInicial);

		menu();
	}

    public static void main(String[] args)
	{
		if (args.length < 3)
		{
			System.out.println("Debe indicar la IP y puerto de Kafka y el ID del operario");
			System.out.println("$./WM_FO kafka_host kafka_puerto id_operario [fichero_servicios]");
			System.exit(1);
		}

		try
		{
			Integer.parseInt(args[1]);
		}
		catch (NumberFormatException e)
		{
			System.out.println("Error: el puerto de Kafka no es numerico: " + args[1]);
			System.exit(1);
		}

		WM_FO fo = new WM_FO(args[0], args[1], args[2]);
		fo.arrancar(args.length >= 4 ? args[3] : null);
	}
}