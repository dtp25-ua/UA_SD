import java.io.*;
import java.net.*;

// clase que implementa el cliente que se conecta al servidor WM_Central para registrar una estacion
public class WM_WS_M {

	// constantes de tiempos
	private static final int PERIODO_MS = 1000; 		  // cada cuanato se comprueba la salud y se avisa a Central
	private static final int TIMEOUT_ENGINE_MS = 2000;    // maximo de espera para que el engine responda
	private static final int TIMEOUT_CENTRAL_MS = 3000;	  // maximo de espera para que Central responda
	private static final int TIMEOUT_CONEXION_MS = 3000;  // maximo de espera para que se establezca la conexion con Central
	private static final int ESPERA_REINTENTO_MS = 3000;  // tiempo de espera entre reintentos de conexion a Central

	// parametros (linea de comandos)
	private int puertoEngine;
	private String centralHost;
	private int centralPuerto;
	private String idEstacion;
	private String ubicacion;

	// ServerSocket porque solo espera a que lleguen peticiones de conexion, no envia nada 
	private ServerSocket servidorEngine;  

	// estado del Engine visto desde el monitor
	// SIN_ENGINE - OK - KO
	private volatile String estadoEngine = "SIN_ENGINE";  

	// atributo auxiliar para saber si se ha podido establecer conexion con Central
	private boolean sesionActiva = false;

	public WM_WS_M(int p_puertoEngine, String p_centralHost, int p_centralPuerto, String p_idEstacion, String p_ubicacion)
	{
		this.puertoEngine = p_puertoEngine;
		this.centralHost = p_centralHost;
		this.centralPuerto = p_centralPuerto;
		this.idEstacion = p_idEstacion;
		this.ubicacion = p_ubicacion;
	}

	// metodo que duerme el hilo actual durante un periodo de tiempo predefinido 
	private void dormir(int p_ms)
	{
		try {Thread.sleep(p_ms);} 
		catch (InterruptedException e) {
			// a estos hilos no se les interrumpe nunca, asi que no deberia llegar aqui
		}
	}

	// envia una trama al Engine y espera la respuesta, si hay error la excepcion sube a quien lo llame
	private String enviarYRecibir(DataInputStream p_entrada, DataOutputStream p_salida, String p_trama) throws IOException
	{
		p_salida.writeUTF(p_trama);
		p_salida.flush();

		return p_entrada.readUTF();
	}

	// -- PARA CONEXION CON EL ENGINE --

	// cambia el estado de Engine y lo muestra por pantalla solo si ha cambiado
	private synchronized void cambiarEstadoEngine(String p_nuevo)
	{
		// si son el mismo estado, no hacemos nada
		if (!estadoEngine.equals(p_nuevo))
		{
			estadoEngine = p_nuevo;
			if (p_nuevo.equals("OK"))
				System.out.println("[MON] Engine OK: la WS está disponible");
			else if (p_nuevo.equals("KO"))
				System.out.println("[MON] Engine KO: la WS está averiada (fuga)");
			else
				System.out.println("[MON] Engine no conectado: la WS está averiada");
		}
	}

	private void atenderEngine() 
	{
		for(;;)
		{
			Socket sk = null;
			try
			{
				System.out.println("[MON] Esperando conexion del Engine en el puerto " + puertoEngine + "...");
				sk = servidorEngine.accept(); 	// aceptamos la conexion del Engine
				sk.setSoTimeout(TIMEOUT_ENGINE_MS);	// tiempo maximo para que el Engine responda

				// abrimos los flujos de datos
				DataInputStream entrada = new DataInputStream(sk.getInputStream());
				DataOutputStream salida = new DataOutputStream(sk.getOutputStream());
				System.out.println("[MON] Engine conectado desde " + sk.getInetAddress());
				
				// el Monitor conoce el id de la estacion por linea de comandos: se lo envía a Engine
				String resp = enviarYRecibir(entrada, salida, "ASIGNAR#" + idEstacion);

				// posible comportamiento inesperado de Engine
				if (!resp.equals("ASIGNAR#OK"))
				{
					throw new IOException("Respuesta inesperada del Engine: " + resp);
				}

				// mientras el Engine siga conectado, leemos y respondemos
				for(;;)
				{
					resp = enviarYRecibir(entrada, salida, "SALUD");
					if (resp.equals("SALUD#OK"))
						cambiarEstadoEngine("OK");
					else
						cambiarEstadoEngine("KO");
					dormir(PERIODO_MS);
				}
			}
			catch (EOFException e)
			{
				System.out.println("[MON] El Engine ha cerrado la conexion");
			}
			catch (IOException e) {
				System.out.println("[MON] Fallo de comunicacion con el Engine: " + e.toString());
			}
			finally
			{
					cambiarEstadoEngine("SIN_ENGINE");
					// cerramos el socket del Engine
				if (sk != null)
				{
					try { sk.close(); } catch (IOException e) { }
				}
			}
			dormir(500);  // esperamos un tiempo antes de volver a aceptar conexiones del Engine
		}
	}


	// -- PARA CONEXION CON CENTRAL --

	// sesion completa con Central: registro y latido o cambio de estado cada segundo
	// lanza IOException si se pierde la conexion / con return si Central rechaza el registro
	private void sesionConCentral(Socket p_sk) throws IOException
	{
		// abrimos los flujos de datos
		DataInputStream entrada = new DataInputStream(p_sk.getInputStream());
		DataOutputStream salida = new DataOutputStream(p_sk.getOutputStream());

		// registro o autenticacion
		String respuesta = enviarYRecibir(entrada, salida, "REGISTRO#" + idEstacion + "#" + ubicacion);
		if (!respuesta.startsWith("STATUS#OK"))
		{
			// si Central rechaza el registro, no lanzamos excepcion, solo se termina la sesion
			System.out.println("[MON] Central ha rechazado el registro: " + respuesta);
			return;
		}
		System.out.println("[MON] Registrado en Central: " + respuesta);
		sesionActiva = true;

		// estado que tiene la estacion segun la ultima confirmacion a Central
		String ultimoEstadoConfirmado = "OK";

		// bucle infinito para los latidos y cambios de estado
		for(;;)
		{
			// estado real del Engine visto desde monitor
			String estadoActual = estadoEngine.equals("OK") ? "OK" : "KO";
			String trama;

			// si el estado ha cambiado desde la ultima confirmacion a Central, avisamos de la averia o de su resolucion
			if (estadoActual.equals("KO") && ultimoEstadoConfirmado.equals("OK"))
				trama = "AVERIA#" + idEstacion + "#FUGA";
			else if (estadoActual.equals("OK") && ultimoEstadoConfirmado.equals("KO"))
				trama = "AVERIA_RESUELTA#" + idEstacion;
			else 
				// si no ha habido cambio de estado, enviamos un latido
				trama ="HEARTBEAT#" + idEstacion + "#OK";

			respuesta = enviarYRecibir(entrada, salida, trama);

			// si Central ha respondido con OK, actualizamos el ultimo estado confirmado
			if (respuesta.startsWith("STATUS#OK"))
			{
				if (!trama.startsWith("HEARTBEAT"))
				{
					ultimoEstadoConfirmado = estadoActual;
					System.out.println("[MON] Central informada: " + trama);
				}
				// else 
					// System.out.println("[MON] Central informada: latido recibido");
			}
			else
				// no damos el cambio de estado por confirmado si Central ha respondido con error
				System.out.println("[MON] Central ha respondido con error a " + trama + ": " + respuesta);

			dormir(PERIODO_MS);
		}
	}

	// intenta conectarse a Central y mantener la conexion, reintentando si se pierde
	private void conectarConCentral() 
	{
		for(;;)
		{
			// al empezar la sesion, aun no hay conexion establecida con Central
			sesionActiva = false;
			Socket sk = new Socket();
			try
			{
				// 'InetSocketAddress' para poder poner timeout a la conexion (tiempo max. para conectarse a Central)
				sk.connect(new InetSocketAddress(centralHost, centralPuerto), TIMEOUT_CONEXION_MS);
				sk.setSoTimeout(TIMEOUT_CENTRAL_MS);  // tiempo maximo para que Central responda

				System.out.println("[MON] Conexión con Central establecida");
				sesionConCentral(sk);
			}
			catch (IOException e)
			{
				if (sesionActiva)
					System.out.println("[MON] Conexion con Central perdida: " + e.toString());
				else
					System.out.println("[MON] No se ha podido conectar con Central (" + centralHost + ":" 
						+ centralPuerto + "): " + e.toString());
			}
			finally
			{
				if (sk != null)
				{
					try { sk.close(); } catch (IOException e) { }
				}
			}
			System.out.println("[MON] Reintentando en " + (ESPERA_REINTENTO_MS / 1000) + "s...");
			dormir(ESPERA_REINTENTO_MS);
		}
	}
	
	// metodo que arranca el monitor, abre el puerto para el Engine y se conecta a Central
	private void arrancar()
	{
		System.out.println("WM_WS_M arrancado. WS " + idEstacion + " (" + ubicacion + "). Central en "
			+ centralHost + ":" + centralPuerto + ". Puerto para el Engine: " + puertoEngine);

		try
		{
			// abrimos el socket para que el Engine se conecte
			servidorEngine = new ServerSocket(puertoEngine);
		}
		catch (IOException e)
		{
			System.out.println("[MON] No se ha podido abrir el puerto" + puertoEngine + " para el Engine: " + e.toString());
			System.exit(1);
		}

		// arrancamos un hilo para atender al Engine
		Thread hiloEngine = new Thread(() -> atenderEngine());
		hiloEngine.setDaemon(true);  // para que el hilo termine cuando termine el main
		hiloEngine.start();

		// el hilo principal se encarga de la conexion con Central
		conectarConCentral();
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 5) {
			System.out.println ("Debe indicar el puerto para el Engine, la IP y el puerto de Central, el id de la estacion y su ubicacion");
			System.out.println ("$./WM_WS_M puerto_engine central_host central_puerto id_estacion ubicacion");
			System.exit(1);
		}

		try
		{
			WM_WS_M monitor = new WM_WS_M(Integer.parseInt(args[0]), args[1], 
				Integer.parseInt(args[2]), args[3], args[4]);
			monitor.arrancar();
		}
		catch (NumberFormatException e)
		{
			System.out.println("Error en los argumentos: " + e.toString());
			System.exit(1);
		}
	}

}
