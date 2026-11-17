import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class GestorRiegos {

    private static final String OPERARIO_CENTRAL = "CENTRAL";
	private static final int TIMEOUT_ARRANQUE_MS = 10000;   // tiempo en el que la WS empieza a regar

    private MensajeriaKafka kafka;
	private GestorBD gestorBD;
	private GestorEstaciones gestorEstaciones;

	// riego en curso de cada WS por ID de estacion
	private ConcurrentHashMap<String, RiegoActivo> activos = new ConcurrentHashMap<String, RiegoActivo>();

    // clase para representar un riego en curso
    private static class RiegoActivo
	{
		final String operario;
		final long creado = System.currentTimeMillis();
		volatile boolean arrancado = false;   // ya ha llegado algun dato de la WS
		volatile int segundos = 0;            // un dato por segundo
		volatile double volumen = 0.0;
		private boolean finNotificado = false;

		RiegoActivo(String p_operario) { this.operario = p_operario; }

		// true solo la primera vez: evita avisar dos veces al operario
		synchronized boolean marcarFinNotificado()
		{
			boolean primera = !finNotificado;
			finNotificado = true;
			return primera;
		}
	}

	public GestorRiegos(String p_kafkaHost, String p_kafkaPuerto, GestorBD p_gestorBD, GestorEstaciones p_gestorEstaciones)
	{
		this.kafka = new MensajeriaKafka(p_kafkaHost, p_kafkaPuerto);
		this.gestorBD = p_gestorBD;
		this.gestorEstaciones = p_gestorEstaciones;
	}

	// inicializar kafka, suscribir consumidores y arrancar hilo del supervisor
	public void iniciar()
	{
		kafka.iniciarProductor();

		// solicitudes: grupo nuevo en cada arranque y latest
		kafka.suscribir(MensajeriaKafka.TOPIC_SOLICITUDES, "wm-central-solicitudes-" + UUID.randomUUID(),
			"latest", false, (k, v) -> alRecibirSolicitud(v));

		// eventos de las WS: grupo fijo y offsets confirmados
		kafka.suscribir(MensajeriaKafka.TOPIC_EVENTOS_WS, "wm-central-eventos",
			"earliest", true, (k, v) -> alRecibirEvento(v));

		Thread supervisor = new Thread(() -> supervisar());
		supervisor.setDaemon(true);
		supervisor.start();
	}

	private static String formato(double p_valor)
	{
		return String.format(Locale.US, "%.2f", p_valor);
	}

	// busca una estacion por su id, devuelve null si no la encuentra
	private EstacionInfo buscar(String p_id)
	{
		for (EstacionInfo e : gestorEstaciones.getAllEstaciones())
		{
			if (e.getIdEstacion().equalsIgnoreCase(p_id))
				return e;
		}
		return null;
	}

	// -- SALIDA HACIA LOS OPERARIOS --

	private void notificar(String p_operario, String p_trama)
	{
		// los riegos pedidos desde el menu de Central no tienen FO al que avisar
		if (p_operario.equals(OPERARIO_CENTRAL))
			return;
		kafka.publicarAsync(MensajeriaKafka.TOPIC_OPERARIOS, p_operario, p_trama);
	}

	private void paso(String p_operario, String p_idWs, String p_texto)
	{
		System.out.println("[CENTRAL] " + p_idWs + ": " + p_texto);
		notificar(p_operario, "RIEGO_PASO#" + p_idWs + "#" + p_texto);
	}

	private void denegar(String p_operario, String p_idWs, String p_motivo)
	{
		System.out.println("[CENTRAL] Riego DENEGADO en " + p_idWs + " para " + p_operario + ": " + p_motivo);
		notificar(p_operario, "RIEGO_DENEGADO#" + p_idWs + "#" + p_motivo);
	}

	// ordenes de Central a una WS
	public void enviarOrden(String p_idWs, String p_trama)
	{
		kafka.publicarAsync(MensajeriaKafka.TOPIC_ORDENES, p_idWs, p_trama);
	}

	// -- SOLICITUDES DE RIEGO --

	// recibe y valida solicitudes de riego desde kafka
	private void alRecibirSolicitud(String p_trama)
	{
		String[] c = p_trama.split("#");
		if (c.length != 4 || !c[0].equals("SOLICITUD_RIEGO"))
		{
			System.out.println("[CENTRAL] Solicitud mal formada: " + p_trama);
			return;
		}
		try
		{
			procesarSolicitud(c[1], c[2], Integer.parseInt(c[3]));
		}
		catch (NumberFormatException e)
		{
			denegar(c[1], c[2], "duracion no valida");
		}
	}

	// comprueba si la estacion esta disponible y autoriza/rechaza el riego
	public void procesarSolicitud(String p_operario, String p_idWs, int p_duracion)
	{
		System.out.println("[CENTRAL] Solicitud de " + p_operario + ": riego en " + p_idWs + " durante " + p_duracion + " s");
		paso(p_operario, p_idWs, "Solicitud recibida por Central");

		EstacionInfo est = buscar(p_idWs);
		if (est == null)
		{
			denegar(p_operario, p_idWs, "estacion desconocida");
			return;
		}
		String id = est.getIdEstacion();

		paso(p_operario, id, "Comprobando disponibilidad de la estacion");
		if (p_duracion <= 0)
		{
			denegar(p_operario, id, "duracion no valida");
			return;
		}
		// comprobamos que se puede utilizar la estacion
		if (!est.reservar(p_operario))
		{
			denegar(p_operario, id, "estacion no disponible (" + est.getMotivoNoDisponible() + ")");
			return;
		}

		// añadimos a la lista de riegos activos
		activos.put(id, new RiegoActivo(p_operario));
		paso(p_operario, id, "Estacion disponible: solicitando autorizacion a la estacion");
		enviarOrden(id, "ORDEN_REGAR#" + id + "#" + p_operario + "#" + p_duracion);
		notificar(p_operario, "RIEGO_AUTORIZADO#" + id);
		System.out.println("[CENTRAL] Riego AUTORIZADO en " + id + " para " + p_operario);
	}

	// -- EVENTOS QUE LLEGAN DE LAS WS --

	// procesa los eventos que lleguen de las estaciones segun el tipo de evento
	private void alRecibirEvento(String p_trama)
	{
		String[] c = p_trama.split("#");
		switch (c[0])
		{
			case "RIEGO_DATOS":
				if (c.length == 5) datos(c[1], c[2], c[3], c[4]);
				else malformada(p_trama);
				break;
			case "RIEGO_FIN":
				if (c.length == 5) fin(c[1], c[2], c[3], c[4]);
				else malformada(p_trama);
				break;
			case "RIEGO_RECHAZADO":
				if (c.length == 4) rechazado(c[1], c[2], c[3]);
				else malformada(p_trama);
				break;
			default:
				malformada(p_trama);
		}
	}

	private void malformada(String p_trama)
	{
		System.out.println("[CENTRAL] Evento de WS no reconocido o mal formado: " + p_trama);
	}

	// actualiza la informacion de un riego
	private void datos(String p_idWs, String p_caudal, String p_volumen, String p_operario)
	{
		EstacionInfo est = buscar(p_idWs);
		if (est == null)
			return;

		double caudal = Double.parseDouble(p_caudal);
		double volumen = Double.parseDouble(p_volumen);

		// si la estacion no está disponible o regando no puede cambiar su estado
		if (!est.actualizarRiego(caudal, volumen, p_operario))
			return;

		String id = est.getIdEstacion();
		RiegoActivo r = activos.get(id);
		if (r == null)
		{
			// riego que empezo antes de arrancar Central
			r = new RiegoActivo(p_operario);
			activos.put(id, r);
		}
		r.arrancado = true;
		r.segundos++;
		r.volumen = volumen;

		notificar(p_operario, "RIEGO_ESTADO#" + id + "#" + p_caudal + "#" + p_volumen);
	}

	// finaliza el riego y registra los datos de este en BD
	private void fin(String p_idWs, String p_volumen, String p_duracion, String p_motivo)
	{
		EstacionInfo est = buscar(p_idWs);
		if (est == null)
			return;
		String id = est.getIdEstacion();

		RiegoActivo r = activos.remove(id);   // eliminamos el riego de la lista
		String operario = (r != null) ? r.operario : est.getIdOperarioRiego();
		if (operario == null)
			operario = "DESCONOCIDO";

		est.terminarRiego();

		try
		{
			// registramos el riego y el operario en la base de datos al terminar
			gestorBD.registrarOperario(operario);
			gestorBD.insertarRiego(id, operario, Double.parseDouble(p_volumen), Integer.parseInt(p_duracion), p_motivo);
		}
		catch (SQLException e)
		{
			System.out.println("[CENTRAL] Error de BD al guardar el riego: " + e.toString());
		}

		System.out.println("[CENTRAL] Riego FINALIZADO en " + id + ": " + p_volumen + " L en " + p_duracion
			+ " s (" + p_motivo + ")");
		notificar(operario, "RIEGO_RESUMEN#" + id + "#" + p_volumen + "#" + p_duracion + "#" + p_motivo);
	}

	// para cuando una WS no ha podido iniciar el riego
	private void rechazado(String p_idWs, String p_operario, String p_motivo)
	{
		EstacionInfo est = buscar(p_idWs);
		if (est == null)
			return;

		activos.remove(est.getIdEstacion()); 	// borramos el riego de la lista de activos
		est.liberarReserva(); 	
		// notificamos que no se puede iniciar el riego
		denegar(p_operario, est.getIdEstacion(), "la estacion no puede regar: " + p_motivo);
	}

	// -- SUPERVISION --

	private void dormir(int p_ms)
	{
		try { Thread.sleep(p_ms); }
		catch (InterruptedException e) { Thread.currentThread().interrupt(); }
	}

	// comprueba cada segundo los riegos activos, cada dos publica el estado de todas las WS
	private void supervisar()
	{
		int ciclo = 0;
		for(;;)
		{
			dormir(1000);
			ciclo++;
			try
			{
				revisarRiegos();
				if (ciclo % 2 == 0)
					publicarEstadoWs();
			}
			catch (Exception e)
			{
				System.out.println("[CENTRAL] Error en la supervision: " + e.toString());
			}
		}
	}

	// detecta estaciones que no responden o riegos interrumpidos
	private void revisarRiegos()
	{
		// recorremos todas las entradas (id_ws, riegoActivo) de activos
		for (Map.Entry<String, RiegoActivo> entrada : activos.entrySet())
		{
			EstacionInfo est = gestorEstaciones.getEstacion(entrada.getKey());
			RiegoActivo r = entrada.getValue();
			if (est == null)
				continue;
			String id = est.getIdEstacion();

			// si la WS no ha respondido a la orden de riego
			if (!r.arrancado && System.currentTimeMillis() - r.creado > TIMEOUT_ARRANQUE_MS)
			{
				activos.remove(id, r);
				est.liberarReserva();
				denegar(r.operario, id, "la estacion no ha respondido a la orden");
				continue;
			}

			// si el riego se interrumpe porque la WS se ha averiado o desconectado
			String base = est.getEstado();
			boolean interrumpido = base.equals(EstacionInfo.FUGA) || base.equals(EstacionInfo.DESCONECTADA);
			if (interrumpido && r.marcarFinNotificado())
			{
				String motivo = base.equals(EstacionInfo.FUGA) ? "AVERIA" : "DESCONEXION";
				est.terminarRiego();
				if (!r.arrancado)
					activos.remove(id, r);

				System.out.println("[CENTRAL] Riego INTERRUMPIDO en " + id + " (" + motivo + "): "
					+ formato(r.volumen) + " L en " + r.segundos + " s");
				notificar(r.operario, "RIEGO_RESUMEN#" + id + "#" + formato(r.volumen) + "#" + r.segundos + "#" + motivo);

				// si el riego habia arrancado, se queda en "activos" esperando RIEGO_FIN que envia la ws
			}
		}
	}

	// publica en kafka el resultado de todas las estaciones
	private void publicarEstadoWs()
	{
		for (EstacionInfo e : gestorEstaciones.getAllEstaciones())
		{
			kafka.publicarAsync(MensajeriaKafka.TOPIC_ESTADO_WS, e.getIdEstacion(),
				"WS_ESTADO#" + e.getIdEstacion() + "#" + e.getUbicacion() + "#" + e.getEstadoVisible());
		}
	}


}
